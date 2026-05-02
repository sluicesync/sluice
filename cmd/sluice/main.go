// Command sluice is the CLI entry point for the sluice database
// migration and continuous-sync tool.
//
// At this stage the binary serves only --version; subcommands will land
// as the underlying engines and pipelines come online. See the design
// docs in the repository for the planned shape.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/orware/sluice/internal/engines"
	// Engine packages are imported for their init() side effects, which
	// register them with the engines registry. Add a new engine by
	// importing its package here.
	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// version, commit, and date are populated at build time via -ldflags.
// See the Makefile or .goreleaser.yaml for how they are set.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	var (
		showVersion bool
		listEngines bool
	)
	flag.BoolVar(&showVersion, "version", false, "print version information and exit")
	flag.BoolVar(&listEngines, "engines", false, "list registered engines and exit")
	flag.Usage = usage
	flag.Parse()

	switch {
	case showVersion:
		fmt.Printf("sluice %s (commit %s, built %s)\n", version, commit, date)
		return
	case listEngines:
		names := engines.Names()
		if len(names) == 0 {
			fmt.Println("(no engines registered)")
			return
		}
		for _, n := range names {
			fmt.Println(n)
		}
		return
	}

	fmt.Fprintln(os.Stderr, "sluice: no command specified")
	fmt.Fprintln(os.Stderr, "")
	flag.Usage()
	os.Exit(2)
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintln(out, "Usage: sluice [flags]")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Flags:")
	flag.PrintDefaults()
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Subcommands (migrate, sync, ...) are not yet implemented.")
	fmt.Fprintln(out, "See docs/architecture.md for the planned design.")
}
