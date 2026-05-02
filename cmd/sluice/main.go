// Command sluice is the CLI entry point for the sluice database
// migration and continuous-sync tool.
//
// The CLI is built with alecthomas/kong (declarative struct-tag
// parsing); see cli.go for the command tree and Run methods. Config
// file loading uses knadh/koanf — see internal/config.
package main

import (
	"fmt"

	"github.com/alecthomas/kong"

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
	cli := &CLI{}
	ctx := kong.Parse(cli,
		kong.Name("sluice"),
		kong.Description("Open-source database migration and continuous-sync tool."),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": fmt.Sprintf("sluice %s (commit %s, built %s)", version, commit, date),
		},
	)
	err := ctx.Run(&cli.Globals)
	ctx.FatalIfErrorf(err)
}
