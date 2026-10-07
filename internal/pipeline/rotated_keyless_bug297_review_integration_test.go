//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Bug 297 review's two repros, ported as pins (real MySQL, doors ON — no
// legacy seam). Fixtures and helpers are rotated_keyless_bug297_integration_test.go's.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// r297AssertNothingWritten fails when restore left any of the named tables on
// the target: every door these pins grade fires before the first segment.
func r297AssertNothingWritten(t *testing.T, side r297Side, tgt string, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		if r297TableExists(t, side, tgt, tbl) {
			t.Errorf("restore refused but table %q exists on the target: the refusal fired after a write", tbl)
		}
	}
}

// r297StartKeyedThenKeylessTable, with a rotating stream already running on a
// keyed-only source, creates the keyless kl (NOT NULL a, so ADD PRIMARY KEY
// (a) is legal later) with 10 rows and waits until a rotation is refused with
// the code.
func r297StartKeyedThenKeylessTable(t *testing.T, side r297Side, src string, logBuf *logcapture.Buffer) {
	t.Helper()
	time.Sleep(3 * time.Second)
	side.exec(t, src, `CREATE TABLE kl (a INT NOT NULL, b TEXT) ENGINE=InnoDB`)
	for i := 1; i <= 10; i++ {
		side.exec(t, src, fmt.Sprintf(`INSERT INTO kl VALUES (%d,'w'); INSERT INTO kd VALUES (%d,'w');`, i, i+100))
		time.Sleep(150 * time.Millisecond)
	}
	deadline := time.Now().Add(45 * time.Second)
	for !strings.Contains(logBuf.String(), string(sluicecode.CodeBackupRotatedKeylessTable)) && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
	}
	if !strings.Contains(logBuf.String(), string(sluicecode.CodeBackupRotatedKeylessTable)) {
		t.Fatal("fixture: no rotation was refused after the keyless table appeared")
	}
}

func r297CaptureLogs(t *testing.T) *logcapture.Buffer {
	t.Helper()
	logBuf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logBuf
}

// TestBug297_KeyAddedInRotationGap_MySQL is the review's silent HIGH. kl is
// created keyless (an AddTable delta), rotation is refused, then ADD PRIMARY
// KEY lands between the last rollover's schema refresh and the rotation
// full's schema read (the "post-drain" FSM hook), so the later full RECORDS
// kl keyed and no delta records the change. The target's kl is the keyless
// one the AddTable delta creates; a DataOnly full never re-keys it. Before
// the projection judgment: verify nil, restore nil, target kl 170 rows for
// 10. Now both refuse, before anything is written.
func TestBug297_KeyAddedInRotationGap_MySQL(t *testing.T) {
	side := r297MySQL()
	src, tgt, cleanup := side.start(t)
	defer cleanup()
	side.exec(t, src, `CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, v TEXT) ENGINE=InnoDB; INSERT INTO kd VALUES (1,'seed');`)
	eng, _ := engines.Get("mysql")
	ctx := context.Background()
	logBuf := r297CaptureLogs(t)

	var armed, altered atomic.Bool
	rotationCrashPoint = func(edge string) error {
		if edge == "post-drain" && armed.Load() && altered.CompareAndSwap(false, true) {
			side.exec(t, src, `ALTER TABLE kl ADD PRIMARY KEY (a)`)
		}
		return nil
	}
	defer func() { rotationCrashPoint = nil }()

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := side.seed(t, store, eng, src)
	stream := &BackupStream{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		RolloverWindow: 900 * time.Millisecond, RolloverMaxChanges: 6, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 50, RetainRotateAtChainLength: 1, SluiceVersion: "test",
	}
	sctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(sctx) }()

	r297StartKeyedThenKeylessTable(t, side, src, logBuf)
	armed.Store(true)
	for i := 200; i < 240; i++ {
		side.exec(t, src, fmt.Sprintf(`INSERT INTO kd VALUES (%d,'w')`, i))
		time.Sleep(150 * time.Millisecond)
	}
	segs := func() int {
		cat, ok, _ := lineage.LoadLineageCatalog(ctx, store)
		if !ok {
			return 0
		}
		return len(cat.Segments)
	}
	deadline := time.Now().Add(45 * time.Second)
	for segs() < 3 && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
	}
	time.Sleep(4 * time.Second)
	cancel()
	if err := <-streamErr; err != nil && !errors.Is(err, context.Canceled) { // see r297BuildRotatedChain
		t.Fatalf("stream: %v", err)
	}
	if !altered.Load() || segs() < 3 {
		t.Fatalf("fixture: altered=%v segments=%d; the gap shape needs the key added inside a rotation and a later full after it",
			altered.Load(), segs())
	}

	_, _, verr := backup.VerifyBackupCoded(ctx, store, backup.VerifyOptions{})
	r297AssertCode(t, "backup verify", verr, `"kl"`, "this chain's own replay")
	rerr := (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store}).Run(ctx)
	r297AssertCode(t, "restore", rerr, `"kl"`, "this chain's own replay", "Nothing has been written")
	r297AssertNothingWritten(t, side, tgt, "kd", "kl")
}

// TestBug297_KeyAddedWhileStopped_MySQL is the review's MEDIUM: the operator
// answers "give the table a key" with ADD PRIMARY KEY while the stream is
// stopped, and restarts with rotation. The chain now carries a primary-key
// delta, which replay always refuses. Before the delta preflight: verify nil,
// and restore refused AT that incremental after writing (kd 11 of 51). Now
// both refuse up front, coded, nothing written.
func TestBug297_KeyAddedWhileStopped_MySQL(t *testing.T) {
	side := r297MySQL()
	src, tgt, cleanup := side.start(t)
	defer cleanup()
	side.exec(t, src, `CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, v TEXT) ENGINE=InnoDB; INSERT INTO kd VALUES (1,'seed');`)
	eng, _ := engines.Get("mysql")
	ctx := context.Background()
	logBuf := r297CaptureLogs(t)

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := side.seed(t, store, eng, src)
	run := func(parent string, body func()) {
		stream := &BackupStream{
			Source: eng, SourceDSN: src, Store: store, ParentRef: parent,
			RolloverWindow: 900 * time.Millisecond, RolloverMaxChanges: 6, RolloverMaxBytes: 1 << 30,
			ChunkChanges: 50, RetainRotateAtChainLength: 1, SluiceVersion: "test",
		}
		sctx, cancel := context.WithTimeout(ctx, 120*time.Second)
		defer cancel()
		streamErr := make(chan error, 1)
		go func() { streamErr <- stream.Run(sctx) }()
		body()
		time.Sleep(4 * time.Second)
		cancel()
		if err := <-streamErr; err != nil && !errors.Is(err, context.Canceled) { // see r297BuildRotatedChain
			t.Fatalf("stream: %v", err)
		}
	}
	run(full.BackupID, func() { r297StartKeyedThenKeylessTable(t, side, src, logBuf) })
	side.exec(t, src, `ALTER TABLE kl ADD PRIMARY KEY (a)`)
	run("", func() {
		for i := 200; i < 240; i++ {
			side.exec(t, src, fmt.Sprintf(`INSERT INTO kd VALUES (%d,'w')`, i))
			time.Sleep(150 * time.Millisecond)
		}
	})

	_, _, verr := backup.VerifyBackupCoded(ctx, store, backup.VerifyOptions{})
	rerr := (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store}).Run(ctx)
	for _, c := range []struct {
		what string
		err  error
	}{{"backup verify", verr}, {"restore", rerr}} {
		ce, ok := sluicecode.FromError(c.err)
		if !ok || ce.Code != sluicecode.CodeBackupSchemaDeltaUnsupported {
			t.Fatalf("%s: err = %v; want %s", c.what, c.err, sluicecode.CodeBackupSchemaDeltaUnsupported)
		}
		if !strings.Contains(c.err.Error(), "primary-key") || !strings.Contains(c.err.Error(), "before writing anything") {
			t.Errorf("%s: refusal does not name the primary-key delta and the up-front refusal:\n%v", c.what, c.err)
		}
	}
	r297AssertNothingWritten(t, side, tgt, "kd", "kl")
}
