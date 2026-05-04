package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kong"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/pipeline"
)

// Globals are flags shared across every subcommand. Embedding into the
// top-level CLI makes them parse identically regardless of which
// subcommand the user runs; binding the value in main() makes it
// available to Run methods that declare a *Globals parameter.
type Globals struct {
	Config   string `help:"Path to a YAML config file." short:"c" type:"existingfile" placeholder:"PATH"`
	LogLevel string `help:"Log verbosity." short:"l" default:"info" enum:"debug,info,warn,error" placeholder:"LEVEL"`
}

// CLI is the root of the sluice command tree. Kong populates this from
// argv and dispatches to the matched subcommand's Run method.
type CLI struct {
	Globals

	// --version prints the build identifier and exits. The value is
	// supplied via kong.Vars{"version": ...} in main().
	Version kong.VersionFlag `help:"Print version and exit." short:"V"`

	Engines EnginesCmd `cmd:"" help:"List registered database engines."`
	Migrate MigrateCmd `cmd:"" help:"Run a one-time schema + data migration (simple mode)."`
	Sync    SyncCmd    `cmd:"" help:"Manage continuous-sync streams."`
	Slot    SlotCmd    `cmd:"" help:"Manage source-side replication slots (Postgres)."`
}

// EnginesCmd lists the database engines registered in the binary,
// along with their key declared capabilities. Useful for confirming
// which 'driver:' values a config file may use.
type EnginesCmd struct{}

// Run implements the engines subcommand.
func (*EnginesCmd) Run() error {
	names := engines.Names()
	if len(names) == 0 {
		fmt.Println("(no engines registered — this binary was built without engine packages)")
		return nil
	}
	fmt.Printf("%-12s  %-18s  %s\n", "NAME", "BULK LOAD", "CDC")
	for _, n := range names {
		e, ok := engines.Get(n)
		if !ok {
			continue
		}
		caps := e.Capabilities()
		fmt.Printf("%-12s  %-18s  %s\n", n, caps.BulkLoad, caps.CDC)
	}
	return nil
}

// MigrateCmd runs a one-shot migration from a source database to a
// target database. Schema is translated and applied first, then data
// is bulk-copied. Suitable for smaller databases willing to accept a
// downtime window.
type MigrateCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only migrate these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Migrate every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	DryRun bool `help:"Read the source schema and print the migration plan without applying changes." short:"n"`
}

// Run implements the migrate subcommand.
func (m *MigrateCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(m.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	target, err := resolveEngine(m.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	// CLI-side mutual exclusion: catching this here means the
	// operator gets a clean "you can't do that" before any DSN
	// dialing happens. NewTableFilter also enforces it for the
	// programmatic-construction path.
	if len(m.IncludeTable) > 0 && len(m.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(m.IncludeTable, m.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	mig := &pipeline.Migrator{
		Source:    source,
		Target:    target,
		SourceDSN: m.Source,
		TargetDSN: m.Target,
		DryRun:    m.DryRun,
		Mappings:  cfg.Mappings,
		Filter:    filter,
	}
	return mig.Run(kongContext())
}

// resolveTableFilterArgs picks the include/exclude list to use,
// preferring CLI flags over YAML config when both are supplied.
// CLI takes precedence wholesale: if --include-table is set it
// replaces the config's IncludeTables (and clears any config-side
// ExcludeTables for that command run, since the operator's intent
// is unambiguous). Same shape for --exclude-table.
func resolveTableFilterArgs(cliInclude, cliExclude []string, cfg *config.Config) (include, exclude []string) {
	if len(cliInclude) > 0 {
		return cliInclude, nil
	}
	if len(cliExclude) > 0 {
		return nil, cliExclude
	}
	return cfg.IncludeTables, cfg.ExcludeTables
}

// resolveEngine looks up an engine by registered name and returns a
// useful error message that lists the available options when the
// name doesn't match anything.
func resolveEngine(name string) (ir.Engine, error) {
	if name == "" {
		return nil, errors.New("engine name is empty")
	}
	e, ok := engines.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown engine %q (registered: %v)", name, engines.Names())
	}
	return e, nil
}

// SyncCmd groups the continuous-sync subcommands. Continuous sync is
// where source-side changes (binlog, logical replication) flow to the
// target on an ongoing basis — useful both as a low-downtime
// migration path and as a reporting/locality replication tool.
type SyncCmd struct {
	Start  SyncStartCmd  `cmd:"" help:"Start a continuous-sync stream from source to target."`
	Status SyncStatusCmd `cmd:"" help:"Show status of a running sync stream."`
	Stop   SyncStopCmd   `cmd:"" help:"Request a running sync stream to drain in-flight changes and exit cleanly."`
}

// SyncStartCmd starts (or resumes) a continuous-sync stream from a
// source database to a target. The stream captures a consistent
// snapshot, bulk-copies it, then streams ongoing changes via CDC
// until the operator interrupts it (Ctrl-C). Restarts with the
// same --stream-id resume from the persisted position rather than
// re-running the snapshot+bulk-copy phase.
type SyncStartCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only stream these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Stream every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	StreamID string `help:"Stream identifier; the key under which position is persisted on the target. Auto-generated from source/target host info when empty." placeholder:"ID"`
	DryRun   bool   `short:"n" help:"Print what would happen — cold-start vs warm-resume, source schema summary or persisted position — without modifying the target or starting the stream."`
}

// Run implements `sluice sync start`.
func (s *SyncStartCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(s.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	if len(s.IncludeTable) > 0 && len(s.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(s.IncludeTable, s.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	streamer := &pipeline.Streamer{
		Source:    source,
		Target:    target,
		SourceDSN: s.Source,
		TargetDSN: s.Target,
		StreamID:  s.StreamID,
		Mappings:  cfg.Mappings,
		DryRun:    s.DryRun,
		Filter:    filter,
	}
	return streamer.Run(kongContext())
}

// SyncStatusCmd reports the state of every continuous-sync stream
// the target database has been the destination for. Reads the
// per-target sluice_cdc_state control table directly — no need for
// a running sync process.
//
// When `--stream-id` is supplied, output is filtered to that one
// stream (matches by exact stream_id). Without it, every row in
// the control table is printed.
type SyncStatusCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string `help:"Filter to a specific stream id. When empty, every recorded stream is shown." placeholder:"ID"`
}

// Run implements `sluice sync status`.
func (s *SyncStatusCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	ctx := kongContext()
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return fmt.Errorf("open target applier: %w", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}

	if s.StreamID != "" {
		filtered := streams[:0]
		for _, st := range streams {
			if st.StreamID == s.StreamID {
				filtered = append(filtered, st)
			}
		}
		streams = filtered
	}

	if len(streams) == 0 {
		if s.StreamID != "" {
			fmt.Fprintf(os.Stdout, "no stream %q on target\n", s.StreamID)
			return nil
		}
		fmt.Fprintln(os.Stdout, "no streams recorded on target")
		return nil
	}

	// Sort for stable output across runs. Most-recently-updated
	// first matches the operator's interest: "what's been moving?"
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].UpdatedAt.After(streams[j].UpdatedAt)
	})

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = tw.Flush() }()
	fmt.Fprintln(tw, "STREAM\tUPDATED\tAGE\tPOSITION")
	now := time.Now()
	for _, st := range streams {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			st.StreamID,
			st.UpdatedAt.UTC().Format(time.RFC3339),
			humanAgo(now.Sub(st.UpdatedAt)),
			truncatePositionToken(st.Position.Token, 60),
		)
	}
	return nil
}

// SyncStopCmd asks a running `sluice sync start` to drain in-flight
// changes, persist its final position, and exit cleanly. The signal
// is delivered via the per-target sluice_cdc_state control table:
// the column stop_requested_at is set to NOW(), and the running
// streamer's polling loop observes the flag on its next tick (every
// 5s by default).
//
// This is additive to the existing Ctrl-C / SIGTERM behavior; it
// exists so operators can stop streams from a different host (k8s
// lifecycle hooks, systemd, ad-hoc operator runbooks) without
// needing PID files or cross-process signal delivery. See
// internal/pipeline/stop_signal.go for the full design rationale.
type SyncStopCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string `help:"Stream identifier to stop." required:"" placeholder:"ID"`
}

// Run implements `sluice sync stop`.
func (s *SyncStopCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	ctx := kongContext()
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return fmt.Errorf("open target applier: %w", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	if err := applier.RequestStop(ctx, s.StreamID); err != nil {
		// Mirrors `slot drop`'s shape: the CLI surfaces a friendly
		// "no stream X on target" rather than an engine-specific
		// stack trace when the operator typos a stream ID.
		if isStreamNotFoundErr(err) {
			fmt.Fprintf(os.Stdout, "no stream %q on target\n", s.StreamID)
			return nil
		}
		return fmt.Errorf("request stop: %w", err)
	}
	fmt.Fprintf(os.Stdout, "stop requested for stream %q on target; running process will drain and exit\n", s.StreamID)
	return nil
}

// isStreamNotFoundErr returns true when err wraps an engine's stream-
// not-found sentinel. The CLI string-matches the wrapped engine
// error rather than importing the sentinel from a specific engine
// package — same shape `isSlotNotFoundErr` uses.
func isStreamNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "stream not found")
}

// humanAgo returns a brief "5m ago" / "2h ago" / "3d ago" string
// for d. Operators glance at the column to spot stuck streams; a
// rough cadence is more useful than precise.
func humanAgo(d time.Duration) string {
	switch {
	case d < 0:
		return "in the future"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// truncatePositionToken returns token if it's no longer than max,
// otherwise the head of the token followed by an ellipsis. Position
// tokens are JSON blobs that can run hundreds of bytes; the status
// table stays readable.
func truncatePositionToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}

// kongContext returns a context.Context wired to OS signals so a
// long-running migration or sync stream cancels cleanly when the
// operator hits Ctrl-C. The context is cancelled on SIGINT or
// SIGTERM; the underlying pipeline goroutines unwind and the
// command exits with the cancellation propagated up.
func kongContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// We deliberately don't capture the cancel func: the signal
	// notifier itself triggers cancellation, and we want the
	// context to stay live for the entire process lifetime
	// (kong dispatches one Run call per process). The spurious
	// context-leak the linter flags here is intentional; see the
	// nolint directive.
	return ctx //nolint:contextcheck // ctx is scoped to the process lifetime
}
