package main

import (
	"errors"
	"fmt"

	"github.com/alecthomas/kong"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/engines"
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
	Source string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN"`
	Target string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN"`
	DryRun bool   `help:"Validate the plan and print what would happen, without applying changes." short:"n"`
}

// Run implements the migrate subcommand.
//
// Until the simple-mode orchestrator lands the command parses,
// validates flags, and loads any config file — but exits with a clear
// not-implemented error rather than running a partial migration.
func (m *MigrateCmd) Run(g *Globals) error {
	if _, err := config.Load(g.Config); err != nil {
		return err
	}
	return errors.New("migrate: simple-mode orchestrator not yet implemented; tracking as the next code chunk")
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
