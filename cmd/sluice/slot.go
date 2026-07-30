package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// SlotCmd groups the operator-facing replication-slot management
// commands. Slots are a Postgres-specific concept today; engines that
// don't expose [ir.SlotManagerOpener] surface a clear error rather
// than silently no-op.
//
// Why this lives at the top level rather than under `sluice sync`:
// slot management is a recovery and diagnostic surface that operators
// reach for *outside* the normal sync flow — typically when something
// has gone wrong (slot invalidated, abandoned slot from a previous
// run, accumulated WAL). Keeping it under `sluice slot` makes it
// discoverable on its own.
type SlotCmd struct {
	List SlotListCmd `cmd:"" help:"List logical-replication slots on the source database. The NAME column is the literal slot name 'sluice slot drop' takes."`
	Drop SlotDropCmd `cmd:"" help:"Drop a replication slot on the source database by its literal name (as 'sluice slot list' prints it)."`
}

// SlotListCmd shows every replication slot visible on the source.
// One row per slot; columns mirror pg_replication_slots so operators
// can correlate against psql output without translation.
type SlotListCmd struct {
	SourceDriver string `help:"Source engine name (e.g. postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
}

// Run implements `sluice slot list`.
func (s *SlotListCmd) Run(g *Globals) error {
	mgr, err := openSlotManager(s.SourceDriver, s.Source)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()

	ctx := kongContext()
	slots, err := mgr.List(ctx)
	if err != nil {
		return err
	}
	if len(slots) == 0 {
		fmt.Fprintln(os.Stdout, "no replication slots on source")
		return nil
	}

	// ADR-0155: at an interactive terminal, render the on-brand bordered
	// table. Everywhere else — piped, CI, --log-format=json, --no-progress —
	// keep the exact tabwriter output below, byte-identical to prior releases.
	if wantPrettyProgress(g, false, false, false) {
		rows := make([][]string, 0, len(slots))
		for _, slot := range slots {
			rows = append(rows, []string{
				slot.Name, slot.Plugin, boolYesNoCLI(slot.Active),
				walStatusOrDash(slot.WALStatus),
				lsnOrDash(slot.RestartLSN), lsnOrDash(slot.ConfirmedFlushLSN),
			})
		}
		headers := []string{"NAME", "PLUGIN", "ACTIVE", "WAL_STATUS", "RESTART_LSN", "CONFIRMED_FLUSH_LSN"}
		// activeCol=2 colours the ACTIVE column (yes=live consumer).
		fmt.Fprintln(os.Stdout, progress.Table(fmt.Sprintf("Replication slots (%d)", len(slots)), headers, rows, 2))
		printPlatformSlotNotes(slots)
		return nil
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tPLUGIN\tACTIVE\tWAL_STATUS\tRESTART_LSN\tCONFIRMED_FLUSH_LSN")
	for _, slot := range slots {
		fmt.Fprintf(tw, "%s\t%s\t%v\t%s\t%s\t%s\n",
			slot.Name, slot.Plugin, slot.Active, walStatusOrDash(slot.WALStatus),
			lsnOrDash(slot.RestartLSN), lsnOrDash(slot.ConfirmedFlushLSN))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	printPlatformSlotNotes(slots)
	return nil
}

// printPlatformSlotNotes appends one line per platform-internal slot
// after the `slot list` table (both render modes). A trailing line
// rather than a table column so the column layout — which scripts
// parse — stays byte-identical when no platform slot is present, and
// so the "don't drop this" guidance reads as prose, not a cell.
func printPlatformSlotNotes(slots []ir.SlotInfo) {
	for _, slot := range slots {
		if slot.PlatformNote != "" {
			fmt.Fprintf(os.Stdout, "note: slot %q is platform-internal (%s) — leave it alone; it is not a leaked consumer and dropping it breaks the provider\n",
				slot.Name, slot.PlatformNote)
		}
	}
}

// SlotDropCmd drops a named replication slot. Destructive; refuses
// loudly (a ClassRefusal coded error, exit 3) without --yes rather
// than prompting — every sluice command is non-interactive.
type SlotDropCmd struct {
	SourceDriver string `help:"Source engine name (e.g. postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	// Name is the LITERAL slot name — deliberately not run through
	// pipeline.ResolveSlotName. `--slot-name` on sync/backup is a
	// SUFFIX (sluice prepends `sluice_`), but auto-prefixing here would
	// (a) make a literal unprefixed slot untargetable and (b) silently
	// retarget a destructive command at a different object than the one
	// the operator named. Instead the not-found path says "you probably
	// meant sluice_<name>" — see slotNameSuggestions.
	Name     string `arg:"" help:"The LITERAL, fully-qualified slot name to drop, exactly as the NAME column of 'sluice slot list' prints it. This is NOT the '--slot-name' suffix that sync/backup take: sluice prepends 'sluice_' to those, so '--slot-name shard_a' creates the slot 'sluice_shard_a' and that full name is what belongs here. Drop never auto-prefixes (a destructive command must act on the object you named); when the literal name is absent but its 'sluice_'-prefixed sibling exists, the error says so and gives the exact command." placeholder:"NAME"`
	IfExists bool   `help:"Treat a missing slot as success rather than an error."`
	Force    bool   `help:"Drop the slot even if it is active (a CDC consumer is currently connected). Use with care."`
	Yes      bool   `help:"Confirm this destructive operation. Required: without it, drop refuses loudly rather than prompting." short:"y"`
}

// Run implements `sluice slot drop`.
func (s *SlotDropCmd) Run(_ *Globals) error {
	if s.Name == "" {
		return errors.New("slot name is required")
	}

	// Non-interactive by contract (AGENTS.md): rather than prompt (which
	// on a non-TTY reads EOF and silently no-ops), refuse loudly and name
	// the remedy before touching the source. --yes is the explicit opt-in
	// every destructive op needs.
	if !s.Yes {
		return &sluicecode.CodedError{
			Code: sluicecode.CodeConfirmationRequired,
			Hint: "pass --yes (or -y) to confirm",
			Err:  fmt.Errorf("dropping replication slot %q on the source is destructive", s.Name),
		}
	}

	mgr, err := openSlotManager(s.SourceDriver, s.Source)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()

	return s.drop(kongContext(), mgr, os.Stdout)
}

// drop runs the drop against an already-open slot manager. Split out of
// Run so the not-found did-you-mean path is unit-testable against a
// fake [ir.SlotManager] with no database.
func (s *SlotDropCmd) drop(ctx context.Context, mgr ir.SlotManager, out io.Writer) error {
	err := mgr.Drop(ctx, s.Name, s.Force)
	if err == nil {
		fmt.Fprintf(out, "dropped slot %q\n", s.Name)
		return nil
	}
	if !isSlotNotFoundErr(err) {
		return err
	}

	// Not found. Before reporting it, work out whether the operator hit
	// sluice's own naming convention: `--slot-name shard_a` creates
	// `sluice_shard_a`, so `slot drop shard_a` is the natural — and
	// wrong — next command. Purely diagnostic: it runs on an
	// already-failing path and degrades to empty strings when the source
	// can't be listed, so a List failure never masks the not-found it
	// was trying to explain.
	sibling, nearby := s.slotNameSuggestions(ctx, mgr)

	if s.IfExists {
		// --if-exists means "a missing slot is not an ERROR" — it does
		// not mean "say nothing about it". Suppressing the failure is not
		// the same as suppressing the information, and the sibling case
		// is exactly where exiting 0 on "nothing to do" would otherwise
		// be a silent no-op over an object the operator meant to drop
		// (the cleanup-that-never-cleaned-up shape that exhausted
		// max_replication_slots in a regression cycle). So: still exit 0,
		// but WARN. The warning goes to the log (stderr / structured
		// under --log-format json), never stdout, so the stdout contract
		// scripts parse stays byte-identical on this path.
		fmt.Fprintf(out, "slot %q does not exist; nothing to do\n", s.Name)
		if sibling != "" {
			slog.WarnContext(
				ctx,
				"slot drop --if-exists: the named slot is absent but its sluice-convention sibling exists",
				slog.String("slot", s.Name),
				slog.String("hint", sibling),
			)
		}
		return nil
	}

	// Uncoded, exit 1, and still wrapping the engine's not-found error —
	// the guidance is appended to the prose, it does not reclassify the
	// failure.
	switch {
	case sibling != "":
		return fmt.Errorf("%w — %s", err, sibling)
	case nearby != "":
		return fmt.Errorf("%w — %s", err, nearby)
	default:
		return err
	}
}

// maxSuggestedSlots caps how many existing sluice-owned slot names the
// not-found guidance enumerates. A source can legitimately carry dozens
// (one per stream, per shard); a diagnostic line is not a slot listing,
// and `sluice slot list` is one command away.
const maxSuggestedSlots = 5

// slotNameSuggestions inspects the source's actual slots and returns at
// most one of two guidance strings for the not-found path:
//
//   - sibling: the strong signal — the literal name the operator gave
//     does NOT exist but its `sluice_`-prefixed form DOES, i.e. they
//     passed the `--slot-name` suffix rather than the full name. Names
//     the convention and gives the exact command.
//   - nearby: the weak signal — neither form exists, but the source does
//     carry sluice-owned slots worth naming (a typo, or the wrong
//     source).
//
// Both are empty when List fails or has nothing useful to say; the
// caller then reports today's bare not-found error unchanged.
func (s *SlotDropCmd) slotNameSuggestions(ctx context.Context, mgr ir.SlotManager) (sibling, nearby string) {
	slots, err := mgr.List(ctx)
	if err != nil {
		return "", ""
	}

	exists := make(map[string]bool, len(slots))
	owned := make([]string, 0, len(slots))
	for _, slot := range slots {
		exists[slot.Name] = true
		if strings.HasPrefix(slot.Name, pipeline.SluiceSlotPrefix) {
			owned = append(owned, slot.Name)
		}
	}

	resolved := pipeline.ResolveSlotName(s.Name)
	if resolved != s.Name && exists[resolved] && !exists[s.Name] {
		return fmt.Sprintf(
			"but the slot %q does exist: sluice treats a sync/backup --slot-name as a SUFFIX and prepends %q to it, "+
				"so `--slot-name %s` created the slot %q. `slot drop` takes the literal name and never prefixes it, "+
				"so if that is the slot you mean, run: sluice slot drop %s --source-driver %s --source <the same DSN> %s",
			resolved, pipeline.SluiceSlotPrefix, s.Name, resolved,
			resolved, s.SourceDriver, s.dropFlagsForSuggestion(),
		), ""
	}

	if len(owned) == 0 {
		return "", ""
	}
	shown := owned
	suffix := ""
	if len(shown) > maxSuggestedSlots {
		shown = shown[:maxSuggestedSlots]
		suffix = fmt.Sprintf(" (and %d more)", len(owned)-maxSuggestedSlots)
	}
	return "", fmt.Sprintf(
		"sluice-owned slots that DO exist on this source: %s%s. `slot drop` takes the literal slot name — the NAME "+
			"column of `sluice slot list` — and sluice prepends %q to every slot it creates from a --slot-name suffix",
		strings.Join(shown, ", "), suffix, pipeline.SluiceSlotPrefix,
	)
}

// dropFlagsForSuggestion echoes the operator's own destructive-intent
// flags back into the suggested command so the copy-paste doesn't lose
// the --force they already decided they needed.
func (s *SlotDropCmd) dropFlagsForSuggestion() string {
	if s.Force {
		return "--yes --force"
	}
	return "--yes"
}

// openSlotManager resolves the engine and opens its slot manager,
// surfacing a clear error when the engine doesn't support slot
// management (e.g. MySQL).
func openSlotManager(driver, dsn string) (ir.SlotManager, error) {
	eng, err := resolveEngine(driver)
	if err != nil {
		return nil, fmt.Errorf("--source-driver: %w", err)
	}
	opener, ok := eng.(ir.SlotManagerOpener)
	if !ok {
		return nil, fmt.Errorf(
			"engine %q does not support replication-slot management (slots are a Postgres-specific concept)",
			driver,
		)
	}
	mgr, err := opener.OpenSlotManager(context.Background(), dsn)
	if err != nil {
		// Connect-phase hint routing (Bug 196 residual): the slot
		// commands are a diagnostic door operators reach when CDC is
		// already misbehaving, so a resolve failure against an AAAA-only
		// host (Supabase direct endpoints) must carry the coded
		// IPv6-only remedy here too, not the resolver's bare error.
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("open slot manager: %w", err))
	}
	return mgr, nil
}

// confirmDestructive prompts the operator on stdout and reads a
// single line from stdin. Returns true only on an explicit "y" or
// "yes" (case-insensitive). Any other answer — empty, "n", "no", a
// typo — is treated as a refusal.
func confirmDestructive(in io.Reader, out io.Writer, prompt string) (bool, error) {
	fmt.Fprint(out, prompt)
	scanner := bufio.NewScanner(in)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return false, fmt.Errorf("read confirmation: %w", err)
		}
		return false, nil
	}
	answer := strings.ToLower(strings.TrimSpace(scanner.Text()))
	return answer == "y" || answer == "yes", nil
}

// errConfirmDeclined is the typed-confirmation abort sentinel: the
// operator answered a destructive-action prompt with anything other
// than the expected token. It is a non-nil error on purpose — an
// aborted run must exit non-zero (the taxonomy's generic 1: no data
// work was attempted, but the command did NOT complete) and, under
// --format json, render status "aborted" rather than "completed"
// (see envelope.go). exitcode_test.go pins the exit code.
var errConfirmDeclined = errors.New("aborted: destructive-action confirmation declined (type the confirmation token to proceed, or pass --yes)")

// confirmTypedDestructive prompts the operator and accepts only an
// exact match (after trim) against the supplied expected token. The
// match is case-sensitive on the token: muscle-memory enter or "y"
// will not pass. Used by `--reset-target-data` (ADR-0023), which sits
// at a higher friction tier than `slot drop` because it destroys
// target data.
//
// ctx-aware: by the time this prompt shows, the caller has usually
// already installed the process's signal.NotifyContext (engine
// resolution calls kongContext), so a Ctrl-C no longer kills the
// process — it cancels ctx. Without the select below the cancellation
// was swallowed: the blocking stdin read kept the prompt alive and
// the operator's interrupt was silently ignored. The reader goroutine
// deliberately leaks when ctx wins the select — it stays blocked on
// stdin, and the command is about to return/exit anyway.
func confirmTypedDestructive(ctx context.Context, in io.Reader, out io.Writer, prompt, expected string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	fmt.Fprint(out, prompt)
	type answer struct {
		ok  bool
		err error
	}
	ch := make(chan answer, 1)
	go func() {
		scanner := bufio.NewScanner(in)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				ch <- answer{err: fmt.Errorf("read confirmation: %w", err)}
				return
			}
			ch <- answer{}
			return
		}
		ch <- answer{ok: strings.TrimSpace(scanner.Text()) == expected}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case a := <-ch:
		return a.ok, a.err
	}
}

// isSlotNotFoundErr returns true if err wraps a slot-not-found
// signal from any engine. Today only Postgres exposes the error;
// the helper string-matches the wrapped engine error rather than
// import an engine-package sentinel (which would couple cmd/ to a
// specific engine).
func isSlotNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "slot not found")
}

// walStatusOrDash renders an empty wal_status as a dash so the
// `slot list` table has a visible placeholder rather than an empty
// column on PG releases that omit the column.
func walStatusOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// lsnOrDash renders an empty LSN as a dash for the same reason as
// walStatusOrDash. A slot in the "creating" state may briefly have
// no restart_lsn.
func lsnOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
