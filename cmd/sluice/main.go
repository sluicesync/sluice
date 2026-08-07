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
	"runtime/debug"
	"strings"
	"time"

	"github.com/alecthomas/kong"

	// Engine packages are imported for their init() side effects, which
	// register them with the engines registry. Add a new engine by
	// importing its package here.
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// version, commit, and date are populated at build time via -ldflags.
// See the Makefile or .goreleaser.yaml for how they are set.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// logLevels maps the values accepted by the kong enum on
// Globals.LogLevel to slog levels. Kept tight on purpose: the
// destination (stderr) is hard-coded today; if we ever need it
// configurable, it becomes a flag (level and format already are).
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
	configureLogging(cli.LogLevel, cli.LogFormat)
	recordRunningCommand(ctx)
	// Flags can point at a secret by environment-variable NAME
	// (--encryption-passphrase-env VAR, --sign-key env:VAR, …). Register
	// those names so internal/config's SLUICE_ prefix scan doesn't warn
	// that sluice's own recommended pattern is a typo. One chokepoint,
	// after parse and before any subcommand's config.Load.
	registerClaimedEnvVars(cli)
	applyMaxMemory(cli.MaxMemory)
	startPprofIfRequested(cli.PprofListen)
	// The operator's value-fidelity flags (--mysql-sql-mode / --zero-date /
	// --sqlite-date-encoding) used to be threaded into the engine packages HERE
	// as process-wide mutable globals (SetSessionSQLMode / SetZeroDateMode /
	// SetDefaultDateEncoding). Audit task 2.5 (finding A-4) moved them onto
	// per-instance engine copies applied at engine resolution ([applyEngineOptions],
	// mirroring labelEngine), so a fleet `sync run` can carry distinct values per
	// sync instead of one global spanning the whole process. See cli.go.
	err := ctx.Run(&cli.Globals)
	// Exit boundary: surface any stable error code + remedy hint as a
	// structured slog record (the `code`/`hint` attrs agents branch on
	// under --log-format json), then let kong print the unchanged prose
	// to stderr and exit — via the ExitCoder contract, so a ClassRefusal
	// CodedError exits 3, a ConfigError 2, everything else 1 (see
	// docs/operator/error-codes.md for the taxonomy).
	logCodedError(err)
	ctx.FatalIfErrorf(err)
}

// recordRunningCommand records WHICH command the operator invoked, so a
// runtime hint on a SHARED pipeline phase names a remedy that command actually
// accepts (Bug 230). The path comes from kong's own selected node rather than
// a hand-kept roster, so it covers every command including ones that do not
// exist yet.
//
// Split out of main() — which no test can call — so the tests that drive a
// command through kong.Parse + Context.Run reproduce main's sequence rather
// than a subset of it. TestMainRecordsTheRunningCommand pins that main still
// calls it.
func recordRunningCommand(kctx *kong.Context) {
	migcore.SetRunningCommand(selectedCommandPath(kctx.Selected()))
}

// selectedCommandPath renders kong's selected node as the space-joined
// command path — "sync start", "schema add-table" — keeping ONLY command
// nodes.
//
// kong's own [kong.Node.Path] is not usable here: it interpolates argument
// nodes ("schema add-table <table>") and alias lists ("sync start (up)"),
// while the flag surface the gate grades against is keyed by the bare command
// path. An alias the operator typed still resolves to its canonical Name, so
// `sluice sync up` and `sluice sync start` record the same path.
// TestSelectedCommandPathMatchesTheGradedSurface pins both halves.
func selectedCommandPath(node *kong.Node) string {
	var parts []string
	for n := node; n != nil; n = n.Parent {
		if n.Type == kong.CommandNode {
			parts = append([]string{n.Name}, parts...)
		}
	}
	return strings.Join(parts, " ")
}

// logCodedError emits one ERROR-level slog record carrying the stable
// error code and remedy hint when err's chain holds a
// [sluicecode.CodedError]; uncoded errors emit nothing (kong's stderr
// line already covers them). Split out of main for the capture-handler
// unit test.
func logCodedError(err error) {
	ce, ok := sluicecode.FromError(err)
	if !ok {
		return
	}
	slog.ErrorContext(
		context.Background(), "command failed",
		slog.String("code", string(ce.Code)),
		slog.String("hint", ce.Hint),
		slog.String("err", err.Error()),
	)
}

// applyMaxMemory installs the operator's --max-memory ceiling via
// runtime/debug.SetMemoryLimit. An empty/"off" flag is a no-op so Go
// keeps honoring the GOMEMLIMIT env var natively; any non-empty value
// is parsed strictly and a bad size is fatal (the operator explicitly
// asked for a ceiling — a silent fallthrough would defeat the purpose,
// the same discipline as --pprof-listen bind failure). See the
// Globals.MaxMemory doc for why a heap ceiling complements
// --max-buffer-bytes.
func applyMaxMemory(raw string) {
	limit, err := parseMaxMemory(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sluice: %v\n", err)
		os.Exit(1)
	}
	if limit <= 0 {
		return
	}
	debug.SetMemoryLimit(limit)
	slog.InfoContext(
		context.Background(), "max-memory ceiling applied",
		slog.Int64("bytes", limit),
		slog.String("hint", "GC defends this soft heap limit; pair with headroom over the live set"),
	)
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

// configureLogging installs a stderr-bound slog handler at the
// requested level and format on slog.Default. Unknown levels/formats
// fall back to info/text without erroring — kong's enum tags already
// reject bad values, so the fallbacks only fire if an enum and this
// function drift apart.
//
// "json" emits one JSON object per line, the shape log aggregators
// (Loki, Datadog, CloudWatch) ingest natively — the long-running
// `sync` mode ships /metrics + /healthz and a k8s-probed container
// image, and a structured log stream is the missing third leg of that
// operational story.
func configureLogging(level, format string) {
	lvl, ok := logLevels[strings.ToLower(level)]
	if !ok {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.EqualFold(format, "json") {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}
