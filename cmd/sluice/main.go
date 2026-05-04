// Command sluice is the CLI entry point for the sluice database
// migration and continuous-sync tool.
//
// The CLI is built with alecthomas/kong (declarative struct-tag
// parsing); see cli.go for the command tree and Run methods. Config
// file loading uses knadh/koanf — see internal/config.
//
// Logging is configured here via the stdlib log/slog: the default
// handler writes a text-formatted record to stderr at the level the
// operator requested with --log-level (info by default). The pipeline
// and engine packages emit through slog.Default(); the operator-
// facing CLI commands (engines, sync status, slot list) keep using
// fmt.Fprintf to stdout because they're table renders, not log
// streams. Stderr keeps the log noise out of pipes and redirects on
// the operator's stdout.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

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

// logLevels maps the values accepted by the kong enum on
// Globals.LogLevel to slog levels. Kept tight on purpose: format,
// destination, and structured-vs-text choices are all hard-coded
// today; if we ever need them configurable, they become flags.
var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

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
	configureLogging(cli.LogLevel)
	err := ctx.Run(&cli.Globals)
	ctx.FatalIfErrorf(err)
}

// configureLogging installs a stderr-bound text slog handler at the
// requested level on slog.Default. Unknown levels fall back to info
// without erroring — kong's enum tag already rejects bad values, so
// this only fires if the enum and map drift apart.
func configureLogging(level string) {
	lvl, ok := logLevels[strings.ToLower(level)]
	if !ok {
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h))
}
