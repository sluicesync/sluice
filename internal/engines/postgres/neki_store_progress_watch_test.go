//go:build integration || nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// nekiStoreProgressWatch watches a backup's OUTPUT for progress and snapshots
// the source's backends the first time it stops moving.
//
// # What it measures, and why the store rather than the log
//
// A backup that is working writes chunk bytes. So "has anything happened in
// the last minute" is answerable by sizing the store directory, with no hook
// into the run at all — and that property is the whole design, arrived at the
// hard way.
//
// The first cut tee'd `slog`'s default handler to watch for the backup's own
// `"backup: table complete"` records. It deadlocked the process on the first
// log line, and the reason is worth recording because it is a trap for any
// test that wants to observe logging: `slog.SetDefault` installs
// `log.SetOutput(&handlerWriter{newHandler})` for any handler that is not the
// built-in `*defaultHandler` — and the built-in `*defaultHandler` emits by
// calling `log.Output`. So a tee that WRAPS the default handler and is then
// installed as the default creates a cycle through `log`'s mutex, which is
// not reentrant. The stdlib says so in `SetDefault`'s own comment. A local
// Docker run of the per-PR plumbing test is what caught it — against the live
// cluster it would have presented as the suite hanging for its whole budget,
// which is indistinguishable from the platform stall this exists to explain.
//
// Sizing a directory mutates no global, hooks nothing, and cannot deadlock.
//
// # What this measures that a per-table timing would not
//
// It detects a stall WITHIN a table as well as between tables, which is the
// shape actually observed: run 34932058458 hung on `nk_restored` and never
// completed it, so a boundary-to-boundary timer would have had nothing to
// report until the run was already dead.
//
// The per-table split is not lost. The backup emits `backup: table complete
// table=… rows=… chunks=…` on every table, and CI timestamps every line — that
// is exactly how the 2.0 s baseline for the healthy run was measured. What the
// log could not produce is the census, which is what this adds.
type nekiStoreProgressWatch struct {
	t          *testing.T
	sample     func(why string)
	root       string
	stallAfter time.Duration

	start time.Time

	mu         sync.Mutex
	lastChange time.Time
	lastSize   int64
	trace      []string
	snapped    bool
	stopped    bool

	done chan struct{}
	gone chan struct{}
}

// nekiWatchStoreProgress starts watching root. sample is called at most once,
// with a reason, the first time the store has not grown for stallAfter.
// [nekiStoreProgressWatch.stop] MUST be called before the enclosing test ends.
func nekiWatchStoreProgress(t *testing.T, root string, stallAfter time.Duration, sample func(why string)) *nekiStoreProgressWatch {
	t.Helper()

	now := time.Now()
	w := &nekiStoreProgressWatch{
		t:          t,
		sample:     sample,
		root:       root,
		stallAfter: stallAfter,
		start:      now,
		lastChange: now,
		lastSize:   -1,
		done:       make(chan struct{}),
		gone:       make(chan struct{}),
	}
	go w.run()

	// A backstop: a t.Fatalf between here and the caller's explicit stop would
	// otherwise leave the goroutine running past the test's end, where its
	// t.Logf panics. stop is idempotent so the normal path still reports once.
	t.Cleanup(w.stop)
	return w
}

// stop joins the sampling goroutine — so no t.Logf can race the test's end —
// and writes the progress trace. Idempotent.
func (w *nekiStoreProgressWatch) stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	w.mu.Unlock()

	close(w.done)
	<-w.gone

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.trace) == 0 {
		w.t.Logf("backup store progress: NOTHING was ever written to %s while the watch was running", w.root)
		return
	}
	w.t.Logf("backup store progress at %s: %s", w.root, strings.Join(w.trace, ", "))
}

// snapshotTaken reports whether the stall sample fired. Its consumers are the
// failure message in [nekiBackupCoreFromPG] — where "did this run go quiet?"
// is the fact that separates a stall from a slow start — and the gate that
// mutation-proves this watch in both directions.
func (w *nekiStoreProgressWatch) snapshotTaken() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapped
}

// progressTrace returns the growth samples recorded so far.
func (w *nekiStoreProgressWatch) progressTrace() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.trace)
}

func (w *nekiStoreProgressWatch) run() {
	defer close(w.gone)
	// The polling interval tracks the threshold rather than being a fixed ten
	// seconds: a live arm's 60 s threshold still samples every 10 s, and a gate
	// that sets a sub-second threshold to prove this fires does not have to
	// sleep for ten seconds to find out.
	interval := 10 * time.Second
	if half := w.stallAfter / 2; half < interval {
		interval = max(half, 20*time.Millisecond)
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-w.done:
			w.poll()
			return
		case <-tick.C:
			if why := w.poll(); why != "" {
				w.sample(why)
			}
		}
	}
}

// poll sizes the store once and returns a non-empty reason when the caller
// should take a sample. It never samples itself, so the lock is never held
// across a database round trip.
func (w *nekiStoreProgressWatch) poll() string {
	size := nekiDirBytes(w.root)

	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if size != w.lastSize {
		w.lastSize = size
		w.lastChange = now
		w.trace = append(w.trace, fmt.Sprintf("t+%s=%dB", now.Sub(w.start).Round(time.Millisecond), size))
		return ""
	}
	quiet := now.Sub(w.lastChange)
	if w.snapped || quiet <= w.stallAfter {
		return ""
	}
	w.snapped = true
	return fmt.Sprintf("the backup store has not grown for %s, past the %s stall threshold (size %d bytes)",
		quiet.Round(time.Millisecond), w.stallAfter, size)
}

// nekiDirBytes totals the bytes under root, or -1 if it cannot be walked.
//
// -1 rather than 0 for the unreadable case, deliberately: a directory that
// does not exist yet and one that holds nothing must not compare equal, or the
// watch would read "the store was created" as "nothing changed".
func nekiDirBytes(root string) int64 {
	var total int64
	if err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	}); err != nil {
		return -1
	}
	return total
}
