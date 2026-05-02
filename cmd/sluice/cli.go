package main

import (
	"context"
	"errors"
	"fmt"
	"os"

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

	DryRun bool `help:"Read the source schema and print the migration plan without applying changes." short:"n"`
}

// Run implements the migrate subcommand.
func (m *MigrateCmd) Run(g *Globals) error {
	if _, err := config.Load(g.Config); err != nil {
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

	mig := &pipeline.Migrator{
		Source:    source,
		Target:    target,
		SourceDSN: m.Source,
		TargetDSN: m.Target,
		DryRun:    m.DryRun,
		Stdout:    os.Stdout,
	}
	return mig.Run(kongContext())
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
}

// SyncStartCmd starts (or resumes) a continuous-sync stream.
type SyncStartCmd struct {
	Source string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN"`
	Target string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN"`
}

// Run implements `sluice sync start`.
func (s *SyncStartCmd) Run(g *Globals) error {
	if _, err := config.Load(g.Config); err != nil {
		return err
	}
	return errors.New("sync: continuous-sync engine not yet implemented")
}

// SyncStatusCmd reports the state of a running sync stream. With no
// CDC engine yet there is nothing to report; this stub wires the
// command shape so the surface is in place when it lands.
type SyncStatusCmd struct{}

// Run implements `sluice sync status`.
func (*SyncStatusCmd) Run() error {
	return errors.New("sync: continuous-sync engine not yet implemented")
}

// kongContext returns a context.Context for use inside Run methods.
// Kept as a small helper so the CLI plumbing for cancellation can
// evolve in one place — for example to listen for SIGINT and cancel
// long-running migrations cleanly. For now it just returns the
// background context.
func kongContext() context.Context {
	return context.Background()
}
