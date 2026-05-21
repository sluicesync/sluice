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
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof" // GitHub #23 Phase A: pprof debug endpoints registered on the default mux
	"os"
	"strings"
	"time"

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
	ctx := kong.Parse(
		cli,
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
	startPprofIfRequested(cli.PprofListen)
	err := ctx.Run(&cli.Globals)
	ctx.FatalIfErrorf(err)
}

// startPprofIfRequested starts net/http/pprof on addr in a background
// goroutine when addr is non-empty. GitHub #23 Phase A diagnostic
// hook — when a sluice process silently stalls, the operator hits
// /debug/pprof/goroutine?debug=2 to dump every goroutine's stack so
// the wedge point can be localised.
//
// Bind failure is fatal: the operator explicitly asked for the
// endpoint, and a silent fallthrough would defeat the purpose. Other
// HTTP errors after the listener succeeds (handler panics, etc.) are
// logged at WARN but don't terminate the subcommand — pprof is
// auxiliary, not critical-path.
//
// The listener uses http.DefaultServeMux which `net/http/pprof`
// auto-registers its handlers on via its init() — importing the
// package for side effects above is intentional.
func startPprofIfRequested(addr string) {
	if addr == "" {
		return
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sluice: --pprof-listen %q: %v\n", addr, err)
		os.Exit(1)
	}
	slog.InfoContext(
		context.Background(), "pprof endpoint listening",
		slog.String("addr", addr),
		slog.String("hint", "fetch /debug/pprof/goroutine?debug=2 to dump goroutine stacks"),
	)
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.WarnContext(
				context.Background(), "pprof endpoint stopped",
				slog.String("err", err.Error()),
			)
		}
	}()
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
