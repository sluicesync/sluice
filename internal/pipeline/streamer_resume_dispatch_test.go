// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Unit coverage for Bug 20 — cross-engine warm-resume dispatch. The
// applier's ReadPosition stamps recovered positions with the
// applier's own (target's) engine name, but the position itself is a
// source-side artifact (a MySQL GTID set, a Postgres LSN). Without
// the dispatch-site re-tag, the source CDC reader's decoder rejects
// the position because the Engine tag refers to the target engine
// instead of the source.
//
// v0.1.0's Bug 2 fix patched the same-family case (PS↔MySQL applier
// stamping "mysql" for a "planetscale" reader). Bug 20 covers the
// truly cross-engine pair (PS-source → PG-target, and the symmetric
// PG↔MySQL pairs). The fix is the source-keyed re-stamp at the
// streamer's resume dispatch site; these tests pin the four-pair
// matrix so a future change can't silently regress one pair.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRetagPositionForSource_FourEnginePairs pins the dispatch
// behaviour for all four (source, target) pairs plus the
// PlanetScale flavor. The persisted position carries the target's
// engine name (the applier always stamps that on read); after the
// re-tag, every position comes back tagged with the source's
// engine name so the source CDC reader's decoder accepts it.
func TestRetagPositionForSource_FourEnginePairs(t *testing.T) {
	const (
		mysqlGTIDToken = `{"mode":"gtid","gtid_set":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000"}`
		mysqlVStream   = `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-100"}]`
		pgLSNToken     = `{"slot":"sluice_slot","lsn":"0/16B7350"}`
	)

	cases := []struct {
		name         string
		sourceEngine string
		targetEngine string
		token        string
	}{
		{
			// Bug 20's headline case: the workaround scenario.
			name:         "planetscale_to_postgres",
			sourceEngine: "planetscale",
			targetEngine: "postgres",
			token:        mysqlVStream,
		},
		{
			// v0.1.0's Bug 2 same-family case — must keep working.
			name:         "mysql_to_postgres",
			sourceEngine: "mysql",
			targetEngine: "postgres",
			token:        mysqlGTIDToken,
		},
		{
			// Symmetric to mysql_to_postgres; the same generalisation
			// covers both directions.
			name:         "postgres_to_mysql",
			sourceEngine: "mysql", // the source-key drives the stamp
			targetEngine: "mysql", // applier stamped the target's name
			token:        pgLSNToken,
		},
		{
			// Same-engine MySQL→MySQL: re-stamp is a no-op (stamp
			// already matches), included so the four-pair matrix is
			// complete and visibly safe under the same code path.
			name:         "mysql_to_mysql",
			sourceEngine: "mysql",
			targetEngine: "mysql",
			token:        mysqlGTIDToken,
		},
		{
			// Same-engine Postgres→Postgres for the symmetric case.
			name:         "postgres_to_postgres",
			sourceEngine: "postgres",
			targetEngine: "postgres",
			token:        pgLSNToken,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			persisted := ir.Position{Engine: c.targetEngine, Token: c.token}
			got := retagPositionForSource(persisted, c.sourceEngine)
			if got.Engine != c.sourceEngine {
				t.Errorf("retag engine = %q; want %q (source name)",
					got.Engine, c.sourceEngine)
			}
			if got.Token != c.token {
				t.Errorf("retag token mutated:\n got:  %q\n want: %q",
					got.Token, c.token)
			}
		})
	}
}

// TestRetagPositionForSource_FromNowSentinel confirms the helper
// leaves the (Engine="", Token="") sentinel alone. Every CDC reader
// treats that pair as "start from the source's current position";
// stamping a non-empty Engine on it would change the meaning.
func TestRetagPositionForSource_FromNowSentinel(t *testing.T) {
	got := retagPositionForSource(ir.Position{}, "mysql")
	if got.Engine != "" || got.Token != "" {
		t.Errorf("from-now sentinel mutated: got %+v; want zero", got)
	}
}

// TestRetagPositionForSource_EmptyTokenPassesThrough confirms the
// helper does not invent a tag for a non-sentinel-but-empty-token
// position. The source decoder is the right place to surface a
// "missing token" error; stamping it here would mask that.
func TestRetagPositionForSource_EmptyTokenPassesThrough(t *testing.T) {
	got := retagPositionForSource(ir.Position{Engine: "postgres", Token: ""}, "mysql")
	if got.Engine != "postgres" {
		t.Errorf("empty-token engine mutated: got %q; want %q", got.Engine, "postgres")
	}
	if got.Token != "" {
		t.Errorf("empty-token token mutated: got %q; want \"\"", got.Token)
	}
}

// TestStreamer_WarmResume_CrossEngine_Retag is the end-to-end-shape
// pin for the dispatch fix. A recording source engine returns a CDC
// reader that captures the Position handed to StreamChanges; a
// recording applier returns a stored position tagged with the
// target's engine name (mimicking what the real applier's
// ReadPosition does). After Run, the captured Position must carry
// the SOURCE engine's name, not the target's — proving the dispatch-
// site re-tag fired before the source decoder could see it.
//
// This is the test that would have caught Bug 20 had it existed at
// v0.1.0 and would now catch any regression that re-introduces a
// target-keyed dispatch.
func TestStreamer_WarmResume_CrossEngine_Retag(t *testing.T) {
	cases := []struct {
		name         string
		sourceEngine string
		targetEngine string
		token        string
	}{
		{
			"planetscale_source_postgres_target",
			"planetscale", "postgres",
			`[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-100"}]`,
		},
		{
			"mysql_source_postgres_target",
			"mysql", "postgres",
			`{"mode":"gtid","gtid_set":"abc:1-1"}`,
		},
		{
			"postgres_source_mysql_target",
			"postgres", "mysql",
			`{"slot":"sluice_slot","lsn":"0/16B7350"}`,
		},
		{
			"mysql_source_mysql_target",
			"mysql", "mysql",
			`{"mode":"gtid","gtid_set":"abc:1-1"}`,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cdcReader := &capturingCDCReader{captured: make(chan struct{})}
			source := &resumeDispatchEngine{
				name:      c.sourceEngine,
				cdcReader: cdcReader,
				caps:      ir.Capabilities{CDC: ir.CDCBinlog}, // any non-None is fine
			}
			target := &resumeDispatchEngine{name: c.targetEngine}

			applier := &resumeDispatchApplier{
				stored: ir.Position{Engine: c.targetEngine, Token: c.token},
				found:  true,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			s := &Streamer{
				Source:    source,
				Target:    target,
				SourceDSN: "src",
				TargetDSN: "tgt",
				StreamID:  "test-stream",
				Applier:   applier,
			}
			// Run will block in dispatchApply on the empty change channel
			// returned by capturingCDCReader; cancel the context to
			// unblock once the resume dispatch has fired.
			runErr := make(chan error, 1)
			go func() { runErr <- s.Run(ctx) }()

			// Wait for the CDC reader to record the position before
			// cancelling, so the assertion below isn't racing the
			// goroutine that captured it.
			select {
			case <-cdcReader.captured:
			case <-time.After(time.Second):
				cancel()
				<-runErr
				t.Fatal("StreamChanges was not called within 1s; resume dispatch did not run")
			}
			cancel()
			err := <-runErr
			// ctx.Cancel paths return nil from Run (graceful drain shape).
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Run returned unexpected error: %v", err)
			}

			got := cdcReader.position
			if got.Engine != c.sourceEngine {
				t.Errorf("CDC reader saw Engine=%q; want %q (source name) — dispatch did not re-tag",
					got.Engine, c.sourceEngine)
			}
			if got.Token != c.token {
				t.Errorf("CDC reader saw Token=%q; want %q (token must round-trip unchanged)",
					got.Token, c.token)
			}
		})
	}
}

// resumeDispatchEngine is a minimal ir.Engine the cross-engine
// dispatch test uses for both the source (where OpenCDCReader
// matters) and the target (where it doesn't, because the test
// supplies the applier directly).
type resumeDispatchEngine struct {
	name      string
	caps      ir.Capabilities
	cdcReader *capturingCDCReader
}

func (e *resumeDispatchEngine) Name() string                  { return e.name }
func (e *resumeDispatchEngine) Capabilities() ir.Capabilities { return e.caps }

func (e *resumeDispatchEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("not used in dispatch test")
}

func (e *resumeDispatchEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("not used in dispatch test")
}

func (e *resumeDispatchEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not used in dispatch test")
}

func (e *resumeDispatchEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("not used in dispatch test")
}

func (e *resumeDispatchEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	if e.cdcReader == nil {
		return nil, errors.New("no CDC reader configured")
	}
	return e.cdcReader, nil
}

func (e *resumeDispatchEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not used in dispatch test")
}

func (e *resumeDispatchEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not used in dispatch test")
}

// capturingCDCReader records the Position the streamer hands to
// StreamChanges and returns an empty (closed) channel so the apply
// loop has nothing to do until ctx cancels. captured is closed
// exactly once on the first StreamChanges call so the test can
// synchronise on the dispatch firing.
type capturingCDCReader struct {
	position ir.Position
	captured chan struct{}
	once     bool
	// closed counts Close() calls. The reader-lifetime pin asserts
	// the streamer Closes the CDC reader before Run returns — see
	// TestStreamer_ClosesCDCReader_BeforeRunReturns.
	closed atomic.Int32
}

func (r *capturingCDCReader) StreamChanges(_ context.Context, from ir.Position) (<-chan ir.Change, error) {
	r.position = from
	if !r.once {
		r.once = true
		close(r.captured)
	}
	ch := make(chan ir.Change)
	close(ch) // empty stream — apply loop returns immediately
	return ch, nil
}

// Close makes capturingCDCReader an io.Closer so the streamer's
// deterministic-teardown closure (warmResume's stop = closeIf(cdc))
// can join it. Counting calls lets the pin assert Close fired —
// and fired before Run returned.
func (r *capturingCDCReader) Close() error {
	r.closed.Add(1)
	return nil
}

// TestStreamer_ClosesCDCReader_BeforeRunReturns is the regression pin
// for the v0.68.2 cross-test DATA RACE. Root cause: warmResume/
// coldStart spawned an engine-side streaming goroutine (real engine:
// go-mysql BinlogSyncer) but discarded the closeable handle, relying
// on "ctx cancel → pump exits → process-exit reclaim". The pump
// exiting does NOT stop the syncer goroutine; only Close does. That
// orphaned goroutine kept logging via slog.Default() and, when a
// later test in the same binary swapped slog.Default() via
// captureSlog, the go race detector flagged a write-vs-finished-read
// on the global default logger across the test boundary.
//
// The fix gives warmResume/coldStart a `stop` closure (closeIf(cdc) /
// stream.Close()) that runOnce defers — so the CDC reader is Closed,
// and the engine-side goroutine joined, BEFORE Run returns and the
// test that started it observes <-runErr. This pin asserts exactly
// that ordering with a fake reader so it runs in the unit suite (no
// Docker / -race needed to catch a regression that drops the defer).
func TestStreamer_ClosesCDCReader_BeforeRunReturns(t *testing.T) {
	cdcReader := &capturingCDCReader{captured: make(chan struct{})}
	source := &resumeDispatchEngine{
		name:      "mysql",
		cdcReader: cdcReader,
		caps:      ir.Capabilities{CDC: ir.CDCBinlog},
	}
	target := &resumeDispatchEngine{name: "mysql"}
	applier := &resumeDispatchApplier{
		stored: ir.Position{Engine: "mysql", Token: `{"mode":"gtid","gtid_set":"abc:1-1"}`},
		found:  true, // drives the warmResume branch
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s := &Streamer{
		Source:    source,
		Target:    target,
		SourceDSN: "src",
		TargetDSN: "tgt",
		StreamID:  "test-stream",
		Applier:   applier,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	select {
	case <-cdcReader.captured:
	case <-time.After(time.Second):
		cancel()
		<-runErr
		t.Fatal("StreamChanges not called within 1s; warm-resume did not run")
	}

	// capturingCDCReader.StreamChanges returns an already-closed
	// channel, so dispatchApply returns immediately and Run unwinds
	// on its own; cancel() is a belt-and-suspenders no-op here. (In
	// production the channel stays open until ctx-cancel, and stop
	// fires on the same defer after cancel — the invariant asserted
	// below is identical in both cases.)
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	// The load-bearing assertion: by the time <-runErr unblocked, the
	// CDC reader MUST already be Closed. If it isn't, runOnce dropped
	// the stop defer and the real engine's syncer goroutine would be
	// leaking past Run's return — the exact cross-test race shape.
	if got := cdcReader.closed.Load(); got == 0 {
		t.Fatal("CDC reader NOT Closed by the time Run returned — " +
			"engine-side streaming goroutine would leak past Run (v0.68.2 cross-test slog race regression)")
	}
}

// resumeDispatchApplier returns a fixed pre-stored position from
// ReadPosition and stubs every other method as no-ops. The
// streamer's Run path needs EnsureControlTable, ClearStopRequested,
// ReadPosition, and Apply — nothing else.
type resumeDispatchApplier struct {
	stored ir.Position
	found  bool
}

func (a *resumeDispatchApplier) EnsureControlTable(context.Context) error { return nil }

func (a *resumeDispatchApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return a.stored, a.found, nil
}

func (a *resumeDispatchApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}
func (a *resumeDispatchApplier) RequestStop(context.Context, string) error        { return nil }
func (a *resumeDispatchApplier) ClearStopRequested(context.Context, string) error { return nil }

func (a *resumeDispatchApplier) Apply(ctx context.Context, _ string, changes <-chan ir.Change) error {
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
