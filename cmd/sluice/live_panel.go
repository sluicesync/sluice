// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/progress"
)

// ADR-0156: the TTY-aware CONTINUOUS live status panel for `sluice sync start`.
//
// Gating is IDENTICAL to ADR-0155's one-shot pretty view (see
// [wantPrettyProgress]): the panel renders only when stdout is a TTY AND
// --log-format=text AND --no-progress is unset AND the run is not a
// --format json envelope / --dry-run / multi-namespace fan-out. Every other
// invocation keeps today's byte-identical structured slog stream.
//
// The panel is wired ENTIRELY in the CLI + presentation layer — the streamer
// orchestrator is untouched:
//
//   - The initial-copy checklist + per-table bar are fed by the SAME
//     [progress.Sink] events the shared bulk-copy phase already emits via
//     [progress.FromContext]; we simply attach the [progress.LiveTTYSink] to
//     the run context.
//   - The CDC body (position + freshness) is fed by a CLI-side status poller
//     that reads the target's control table via [ir.ChangeApplier.ListStreams]
//     — the same read `sync status` / the metrics endpoint use — on its OWN
//     connection, so the streamer's apply connection is untouched.
//   - WARN/ERROR records are forwarded (not buffered) into the panel's bounded
//     recent-events ring by a continuous slog gate; INFO/DEBUG are dropped on
//     the TTY (they still exist on the non-TTY / json path).
//   - `q` / ctrl+c triggers the EXACT graceful drain path: it calls
//     [ir.ChangeApplier.RequestStop] (what `sync stop` writes), which trips the
//     streamer's stop-signal poll so in-flight changes drain before exit.

// liveStatusPollInterval is the cadence the CLI-side status poller reads the
// control table at. 1s is responsive for a freshness readout without hammering
// the target (matching `sync stop --wait`'s poll cadence).
const liveStatusPollInterval = 1 * time.Second

// runSyncStartLivePanel runs `sync start` under the continuous live panel
// (ADR-0156). It owns a dedicated control-table connection for status polling
// and the graceful drain-stop, runs the streamer in a separate goroutine
// (renderer isolation — a panel failure never aborts the sync), and tears the
// panel down when the streamer exits.
func runSyncStartLivePanel(ctx context.Context, s *SyncStartCmd, source, target ir.Engine, streamer *pipeline.Streamer, crashWrap func(error) error) error {
	// A dedicated read/write control applier for status polling + the graceful
	// drain-stop request. Its own connection, so it never contends with the
	// streamer's apply connection. If it can't be opened, fall back to the
	// structured-log path — the panel is presentation-only and must never be
	// the reason a sync refuses to start.
	ctlApplier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		slog.WarnContext(ctx, "live panel: could not open status connection; running with structured logs instead",
			slog.String("error", err.Error()))
		return crashWrap(streamer.Run(ctx))
	}
	defer func() {
		if c, ok := ctlApplier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// adoptedID is the stream id the poller observed — used by the drain-stop
	// when --stream-id was left empty (the streamer auto-generates one). Guarded
	// because the poller writes it and the drain tea.Cmd reads it.
	var idMu sync.Mutex
	adoptedID := s.StreamID

	// stopCmd is the drain-and-stop side effect the panel returns on q/ctrl+c.
	// It requests a GRACEFUL drain (RequestStop trips the streamer's stop-signal
	// poll so in-flight changes drain before exit) — the load-bearing
	// correctness point of the panel. Before any stream id is known (still
	// cold-starting, nothing to drain) it cancels the run as the backstop.
	var stopCmd tea.Cmd = func() tea.Msg {
		idMu.Lock()
		id := adoptedID
		idMu.Unlock()
		if id == "" {
			cancel()
			return progress.NewStopResultMsg(nil)
		}
		return progress.NewStopResultMsg(ctlApplier.RequestStop(runCtx, id))
	}

	// restore reinstates the previous slog handler; wrapped in a Once so the
	// teardown branches and the renderer-panic handler can't double-restore.
	var restoreOnce sync.Once
	var restore func()
	restoreAll := func() {
		restoreOnce.Do(func() {
			if restore != nil {
				restore()
			}
		})
	}
	onRendererPanic := func(r any) {
		// Renderer isolation: the panel died, but the streamer keeps running.
		// Restore structured logging so the rest of the run is legible.
		restoreAll()
		slog.Error("live panel: renderer panicked; continuing with structured logs",
			slog.Any("panic", r))
	}

	sink := progress.NewLiveTTYSink(os.Stdout, pipeline.MigrateProgressSpec,
		progress.LiveHeader{Source: source.Name(), Target: target.Name(), StreamID: s.StreamID},
		stopCmd, onRendererPanic)
	restore = silenceSlogForLivePanel(sink)

	// Attach the sink to the run context so the streamer's shared bulk-copy
	// phase feeds the initial-copy checklist/bar via progress.FromContext.
	sinkCtx := progress.NewContext(runCtx, sink)

	streamerErrCh := make(chan error, 1)
	go func() { streamerErrCh <- crashWrap(streamer.Run(sinkCtx)) }()

	pollCtx, pollCancel := context.WithCancel(runCtx)
	go pollLiveStatus(pollCtx, ctlApplier, s.StreamID, sink, &idMu, &adoptedID)

	// The panel's own goroutine returns when the operator force-quits (a second
	// q/ctrl+c). Select that against the streamer's exit.
	panelDone := make(chan struct{})
	go func() {
		sink.Wait()
		close(panelDone)
	}()

	var streamerErr error
	select {
	case streamerErr = <-streamerErrCh:
		// Streamer exited on its own (graceful drain after q, source EOF, or an
		// error). Tear the panel down (releasing the terminal) BEFORE restoring
		// slog, so no stray log line interleaves with the final frame.
		pollCancel()
		sink.Quit()
		restoreAll()
	case <-panelDone:
		// Operator force-quit while the streamer was still running: the panel
		// already released the terminal, so restore slog, then cancel to stop
		// the streamer and collect its error.
		pollCancel()
		restoreAll()
		cancel()
		streamerErr = <-streamerErrCh
	}

	if streamerErr == nil {
		fmt.Fprintln(os.Stdout, "sluice sync stopped.")
	}
	return streamerErr
}

// pickLiveStream selects the stream row the panel tracks: the one matching
// --stream-id when set, else the sole stream when exactly one is present (an
// auto-generated id). Returns ok=false while no matching row exists yet (still
// cold-starting) so the poller leaves the panel in initial-copy mode rather
// than fabricating a CDC reading.
func pickLiveStream(streams []ir.StreamStatus, wantID string) (ir.StreamStatus, bool) {
	if wantID != "" {
		for _, st := range streams {
			if st.StreamID == wantID {
				return st, true
			}
		}
		return ir.StreamStatus{}, false
	}
	if len(streams) == 1 {
		return streams[0], true
	}
	return ir.StreamStatus{}, false
}

// pollLiveStatus reads the control table on a cadence and feeds the panel's CDC
// body: last-applied position and freshness (now − UpdatedAt, the load-bearing
// cross-engine lag signal), plus connection health. A read error flips health
// to reconnecting and bumps the reconnect counter; it never crashes the panel
// or the sync (failure-isolated, presentation-only).
func pollLiveStatus(ctx context.Context, applier ir.ChangeApplier, wantID string, sink *progress.LiveTTYSink, idMu *sync.Mutex, adoptedID *string) {
	ticker := time.NewTicker(liveStatusPollInterval)
	defer ticker.Stop()
	restarts := 0
	connected := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			streams, err := applier.ListStreams(ctx)
			if err != nil {
				if connected {
					restarts++
					connected = false
				}
				sink.HealthReconnecting(restarts)
				continue
			}
			st, ok := pickLiveStream(streams, wantID)
			if !ok {
				continue
			}
			if st.StreamID != "" {
				idMu.Lock()
				*adoptedID = st.StreamID
				idMu.Unlock()
			}
			connected = true
			// A written position (non-zero UpdatedAt) means CDC is applying:
			// flip the panel to the CDC body. Absence stays "unknown" — never a
			// fabricated 0-second freshness (the *Known honesty contract).
			if !st.UpdatedAt.IsZero() {
				sink.EnterCDC()
				sink.Status(progress.LiveStatus{
					Position:    st.Position.Token,
					Freshness:   time.Since(st.UpdatedAt),
					Known:       true,
					RowsApplied: st.RowsApplied,
					PolledAt:    time.Now(),
				})
			}
			sink.HealthConnected(restarts)
		}
	}
}

// silenceSlogForLivePanel makes the live panel the ONLY writer to the terminal
// for the run's duration (ADR-0156 §"Log handling — surface, don't buffer").
// Unlike ADR-0155's one-shot [silenceSlogForTTY], it does NOT buffer: a
// continuous run can last days, so WARN/ERROR records are FORWARDED into the
// panel's bounded recent-events ring as they occur, and INFO/DEBUG are dropped
// on the TTY (they still exist on the non-TTY / json path, which never installs
// this gate). The returned restore reinstalls the previous handler.
func silenceSlogForLivePanel(sink *progress.LiveTTYSink) func() {
	prev := slog.Default()
	slog.SetDefault(slog.New(&liveGateHandler{sink: sink}))
	return func() { slog.SetDefault(prev) }
}

// liveEventSink is the narrow surface the slog gate needs from the panel — the
// bounded recent-events ring. *progress.LiveTTYSink satisfies it; an interface
// so the gate is unit-testable without a live bubbletea program.
type liveEventSink interface {
	Event(level, text string)
}

// liveGateHandler is the slog.Handler installed while the live panel owns the
// terminal. It drops records below WARN entirely; WARN/ERROR are forwarded to
// the panel's recent-events ring (never to stderr, so the render can't be
// corrupted by an interleaved raw line).
type liveGateHandler struct {
	sink  liveEventSink
	attrs []string
}

func (h *liveGateHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (h *liveGateHandler) Handle(_ context.Context, r slog.Record) error {
	level := "WARN"
	if r.Level >= slog.LevelError {
		level = "ERROR"
	}
	parts := append([]string(nil), h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		parts = append(parts, a.Key+"="+a.Value.String())
		return true
	})
	text := r.Message
	if len(parts) > 0 {
		text += " (" + strings.Join(parts, " ") + ")"
	}
	h.sink.Event(level, text)
	return nil
}

func (h *liveGateHandler) WithAttrs(as []slog.Attr) slog.Handler {
	na := append([]string(nil), h.attrs...)
	for _, a := range as {
		na = append(na, a.Key+"="+a.Value.String())
	}
	return &liveGateHandler{sink: h.sink, attrs: na}
}

func (h *liveGateHandler) WithGroup(_ string) slog.Handler {
	// Group nesting has no bearing on the flat one-line event rendering; keep
	// the same handler so grouped attrs still flatten into the event text.
	return h
}
