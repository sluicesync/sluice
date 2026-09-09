// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// A STOP after rows have landed must KEEP the slot; a genuine failure
// must still drop it.
//
// The defect this pins was found by CI on 2026-09-08 and predates that
// session by two months: coldStartRunCopy abandoned on ANY copy error,
// including a plain context.Canceled, and Abandon drops the replication
// slot. So Ctrl-C during a long index build destroyed the slot for a copy
// that had already committed every row — leaving a target warm resume
// cannot recover (no position) and cold start refuses (populated), whose
// only escape is --reset-target-data and a full re-copy.
func TestAbandonUnlessStopped(t *testing.T) {
	t.Parallel()

	newStream := func(abandoned, closed *bool) *ir.SnapshotStream {
		return &ir.SnapshotStream{
			AbandonFn: func() error { *abandoned = true; return nil },
			CloseFn:   func() error { *closed = true; return nil },
		}
	}

	t.Run("a genuine failure still ABANDONS (Bug 177: no orphaned WAL-pinning slot)", func(t *testing.T) {
		t.Parallel()
		var abandoned, closed bool
		s := &Streamer{}
		s.abandonUnlessStopped(context.Background(), newStream(&abandoned, &closed), errors.New("disk full"))
		if !abandoned {
			t.Error("a real failure did not abandon; a refused cold start would leave a slot pinning WAL forever")
		}
		if closed {
			t.Error("a real failure took the close path")
		}
	})

	t.Run("a stop KEEPS the slot", func(t *testing.T) {
		t.Parallel()
		var abandoned, closed bool
		s := &Streamer{}
		s.abandonUnlessStopped(context.Background(), newStream(&abandoned, &closed),
			fmt.Errorf("pipeline: create indexes: %w", context.Canceled))
		if abandoned {
			t.Error("an operator stop ABANDONED the stream. The bulk copy may have committed every row; " +
				"dropping the slot there costs the whole copy and forces --reset-target-data")
		}
		if !closed {
			t.Error("the stopped stream was not closed")
		}
	})

	t.Run("a cancelled CONTEXT is a stop even when the error lost its wrapping", func(t *testing.T) {
		t.Parallel()
		// A driver that reports "connection closed" when its ctx dies, or a
		// phase that wraps without %w, still means the operator stopped.
		// Grading that as a failure is the expensive direction.
		var abandoned, closed bool
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s := &Streamer{}
		s.abandonUnlessStopped(ctx, newStream(&abandoned, &closed), errors.New("connection closed"))
		if abandoned {
			t.Error("a cancelled context with an unwrapped error was graded a failure and abandoned")
		}
		if !closed {
			t.Error("the stopped stream was not closed")
		}
	})

	t.Run("the WARN names the slot and BOTH ways out", func(t *testing.T) {
		t.Parallel()
		// Preserving silently would trade a recoverable re-copy for an
		// unrecoverable outage: a kept slot pins WAL and can fill a busy
		// source's disk. The warning is half the fix, so it is pinned.
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		var abandoned, closed bool
		s := &Streamer{SlotName: "custom_slot"}
		s.abandonUnlessStopped(context.Background(), newStream(&abandoned, &closed), context.Canceled)

		got := buf.String()
		for _, want := range []string{
			stoppedSlotKeptMarker,
			"custom_slot",              // WHICH slot
			"PINS",                     // the cost
			"sync start",               // way out 1: resume
			"pg_drop_replication_slot", // way out 2: abandon deliberately
			"fill the disk",            // why it matters on a busy source
		} {
			if !strings.Contains(got, want) {
				t.Errorf("the stopped-slot WARN does not mention %q — an operator cannot act on it:\n%s", want, got)
			}
		}
	})

	t.Run("the WARN still names a slot when none was configured", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		var abandoned, closed bool
		(&Streamer{}).abandonUnlessStopped(context.Background(), newStream(&abandoned, &closed), context.Canceled)
		if !strings.Contains(buf.String(), "sluice_slot") {
			t.Errorf("with no --slot-name the WARN does not name the default, so the operator has nothing to "+
				"look for on the source:\n%s", buf.String())
		}
	})
}

// Every POST-COPY abandon goes through the door, and the roster derives
// itself.
//
// The bug this gate exists for was a single call site that predated the
// rule by two months. Its siblings are the other error paths that can run
// AFTER the bulk copy has written rows to the target, where abandoning
// destroys a completed copy rather than cleaning up debris — and a new
// one of those is exactly what nobody would notice.
//
// Scope, stated so the name cannot be read as broader than the truth:
// this walks streamer_coldstart.go only, and grades only the functions
// listed in postCopyFuncs. Abandon calls in PRE-copy functions (schema
// read, preflights, snapshot open) are correct and deliberately out of
// scope: nothing has been written yet, so there is no completed work to
// destroy.
func TestPostCopyAbandonsGoThroughTheStopDoor(t *testing.T) {
	t.Parallel()

	// The functions that can run after rows exist on the target. A new
	// post-copy phase belongs here; that is a deliberate edit, which is
	// the point.
	postCopyFuncs := map[string]string{
		"coldStartRunCopy": "runs the bulk copy; on error the copy may already have committed every row",
		"coldStart":        "the float-repair arm runs after the copy completes",
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "streamer_coldstart.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse streamer_coldstart.go: %v", err)
	}

	srcLines := readSourceLines(t, "streamer_coldstart.go")
	graded, viaDoor, marked := 0, 0, 0
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if _, want := postCopyFuncs[fn.Name.Name]; !want {
			continue
		}
		graded++
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "abandonUnlessStopped":
				viaDoor++
			case "Abandon":
				pos := fset.Position(call.Pos())
				if abandonMarked(srcLines, pos.Line) {
					marked++
					return true
				}
				t.Errorf("%s:%d: %s calls stream.Abandon() directly.\n"+
					"  This function can run AFTER rows are on the target, where an operator STOP arrives as an "+
					"ordinary error and abandoning drops the slot for a copy that already succeeded — the target "+
					"is then unresumable and needs --reset-target-data.\n"+
					"  Route it through s.abandonUnlessStopped(ctx, stream, err), or, if abandoning is genuinely "+
					"right here (a provable correctness violation, not a stop), move the call into its own "+
					"function and record it in this test's exempt map with the reason.",
					pos.Filename, pos.Line, fn.Name.Name)
			}
			return true
		})
	}

	// Anti-vacuity, both halves. A roster that graded nothing, or one whose
	// door is never actually used, passes by checking nothing — which is
	// the failure mode that makes a gate worse than none.
	if graded != len(postCopyFuncs) {
		t.Errorf("graded %d of %d post-copy functions; the rest were not found in streamer_coldstart.go. "+
			"If they were renamed, re-point this roster rather than letting it shrink.", graded, len(postCopyFuncs))
	}
	if viaDoor < 2 {
		t.Errorf("found only %d call(s) to abandonUnlessStopped across the post-copy functions; expected at "+
			"least 2 (the copy error and the float-repair arm). The door has been bypassed or removed.", viaDoor)
	}
	if marked == 0 {
		t.Error("no deliberate Abandon carried the marker. Either every one now routes through the door — in " +
			"which case delete the marker branch rather than leaving a check that grades nothing — or the " +
			"marker spelling drifted and this gate is silently accepting bare Abandons.")
	}
}

// abandonOnPurposeMarker opts a single Abandon call out of the stop-door
// requirement. It must sit on the call's own line or the line above, and
// it must carry a reason — the whole point is that a deliberate abandon
// is a recorded DECISION rather than an oversight that looks identical.
const abandonOnPurposeMarker = "//sluice:abandon-on-purpose"

// abandonMarked reports whether the marker sits on the call's line or the
// one above it, WITH a reason after it. A bare marker is not an
// exemption: the reason is the thing that makes the next reader able to
// judge whether it is still true.
func abandonMarked(lines []string, callLine int) bool {
	for _, idx := range []int{callLine - 1, callLine - 2} {
		if idx < 0 || idx >= len(lines) {
			continue
		}
		line := lines[idx]
		i := strings.Index(line, abandonOnPurposeMarker)
		if i < 0 {
			continue
		}
		if strings.TrimSpace(line[i+len(abandonOnPurposeMarker):]) != "" {
			return true
		}
	}
	return false
}

// readSourceLines reads a file in this package for the marker scan.
// Comment positions inside a call's neighbourhood are not attached to the
// call by go/ast, so a line scan is the honest way to find them.
func readSourceLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.Split(string(b), "\n")
}
