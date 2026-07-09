package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"sluicesync.dev/sluice/internal/ir"
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
	List SlotListCmd `cmd:"" help:"List logical-replication slots on the source database."`
	Drop SlotDropCmd `cmd:"" help:"Drop a named replication slot on the source database."`
}

// SlotListCmd shows every replication slot visible on the source.
// One row per slot; columns mirror pg_replication_slots so operators
// can correlate against psql output without translation.
type SlotListCmd struct {
	SourceDriver string `help:"Source engine name (e.g. postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
}

// Run implements `sluice slot list`.
func (s *SlotListCmd) Run(_ *Globals) error {
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

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = tw.Flush() }()
	fmt.Fprintln(tw, "NAME\tPLUGIN\tACTIVE\tWAL_STATUS\tRESTART_LSN\tCONFIRMED_FLUSH_LSN")
	for _, slot := range slots {
		fmt.Fprintf(tw, "%s\t%s\t%v\t%s\t%s\t%s\n",
			slot.Name, slot.Plugin, slot.Active, walStatusOrDash(slot.WALStatus),
			lsnOrDash(slot.RestartLSN), lsnOrDash(slot.ConfirmedFlushLSN))
	}
	return nil
}

// SlotDropCmd drops a named replication slot. Destructive; refuses
// loudly (a ClassRefusal coded error, exit 3) without --yes rather
// than prompting — every sluice command is non-interactive.
type SlotDropCmd struct {
	SourceDriver string `help:"Source engine name (e.g. postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	Name     string `arg:"" help:"Slot name to drop." placeholder:"NAME"`
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

	ctx := kongContext()
	if err := mgr.Drop(ctx, s.Name, s.Force); err != nil {
		if s.IfExists && isSlotNotFoundErr(err) {
			fmt.Fprintf(os.Stdout, "slot %q does not exist; nothing to do\n", s.Name)
			return nil
		}
		return err
	}
	fmt.Fprintf(os.Stdout, "dropped slot %q\n", s.Name)
	return nil
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
		return nil, fmt.Errorf("open slot manager: %w", err)
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
