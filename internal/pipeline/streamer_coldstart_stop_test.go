// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
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

	t.Run("the WARN names the slot, how to resume, and the way out when the resume does not apply", func(t *testing.T) {
		// No t.Parallel: this swaps the GLOBAL slog default to capture output,
		// so parallel siblings would write into each other's buffers.
		// Preserving silently would trade a recoverable re-copy for an
		// unrecoverable outage: a kept slot pins WAL and can fill a busy
		// source's disk. The warning is half the fix, so it is pinned.
		var buf logcapture.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		var abandoned, closed bool
		s := &Streamer{SlotName: "custom_slot"}
		s.abandonUnlessStopped(context.Background(), newStream(&abandoned, &closed), context.Canceled)

		got := buf.String()
		for _, want := range []string{
			stoppedSlotKeptMarker,
			"custom_slot", // WHICH slot
			"PINS",        // the cost
			// A0909-STOP-1 shipped the resume this message used to say did
			// not exist, so the WARN now has to carry BOTH outcomes: the
			// resume, what it requires, and the drop-and-re-copy exit for
			// the cases it cannot cover. A message that promised only the
			// resume would be the same defect in the other direction —
			// v0.148.0's, which told every stopped operator to re-run.
			"TO RESUME",
			"same --stream-id",     // how
			coldStartResumedMarker, // what proves it happened
			"PostgreSQL source",    // what it requires
			"IF IT REFUSES",        // and the branch for everyone else
			"sluice slot drop",     // the way out: our OWN command, not raw SQL
			"--yes",                // which drop refuses without
			"--reset-target-data",  // and the re-copy that follows it
			"fill the disk",        // why it matters on a busy source
		} {
			if !strings.Contains(got, want) {
				t.Errorf("the stopped-slot WARN does not mention %q — an operator cannot act on it:\n%s", want, got)
			}
		}
	})

	t.Run("the WARN still names a slot when none was configured", func(t *testing.T) {
		var buf logcapture.Buffer
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

// The recovery advice must name the slot that actually EXISTS.
//
// `--slot-name` is a SUFFIX: sluice prepends "sluice_". An operator who
// passed `--slot-name prod` has `sluice_prod` on the server, and a WARN
// that told them to look for `prod` would send them hunting for an object
// that does not exist -- in the one message whose entire job is telling
// them what to act on. `sluice slot drop` takes the LITERAL name and does
// no prefixing of its own, so the advice has to be pre-resolved.
func TestStoppedSlotAdviceNamesTheRealSlot(t *testing.T) {
	// Not parallel at any level: every cell swaps the global slog default.

	capture := func(slotName string) string {
		var buf logcapture.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		var a, c bool
		s := &Streamer{SlotName: slotName}
		s.abandonUnlessStopped(context.Background(), &ir.SnapshotStream{
			AbandonFn: func() error { a = true; return nil },
			CloseFn:   func() error { c = true; return nil },
		}, context.Canceled)
		_, _ = a, c
		return buf.String()
	}

	t.Run("a bare suffix is resolved to the real slot", func(t *testing.T) {
		got := capture("prod")
		if !strings.Contains(got, "sluice_prod") {
			t.Errorf("the advice does not name sluice_prod. --slot-name is a SUFFIX, so an operator told to "+
				"look for %q would find nothing on the source:\n%s", "prod", got)
		}
	})

	t.Run("an already-prefixed name is not double-prefixed", func(t *testing.T) {
		got := capture("sluice_prod")
		if strings.Contains(got, "sluice_sluice_prod") {
			t.Errorf("the advice double-prefixed an already-qualified slot name:\n%s", got)
		}
	})

	t.Run("the unset default is the advice's constant", func(t *testing.T) {
		// That the constant equals the engine's `defaultSlot` is
		// TestStoppedSlotAdviceNamesTheRealDefault's job, below; this half
		// only checks the advice renders it.
		if !strings.Contains(capture(""), defaultSlotNameForAdvice) {
			t.Errorf("with no --slot-name the advice does not name %q", defaultSlotNameForAdvice)
		}
	})
}

// TestStoppedSlotAdviceNamesTheRealDefault binds every VALUE-bearing copy
// of the default slot name to the engine's own `defaultSlot` — the one
// the Postgres reader actually creates.
//
// This package cannot import the engine, so it carries three copies
// (defaultSlotNameForAdvice, defaultPGSlotName, defaultActiveSlotName)
// and the CLI carries a fourth. TestDefaultSlotLiteralHasNoNewHome stops
// a sixth home appearing but binds none of the five to each other; two
// comments cited THIS test as the binding and it did not exist under
// this name — the check lived as an unnamed subtest binding one copy
// (2026-09-15 audit, LOW: sluice_slot has four value copies and both comments cite a test that did not exist). A divergence in defaultPGSlotName is the
// A0909-AQ-M-1 shape exactly: a slot lookup against a name nothing
// created returns no rows, which the caller cannot tell from a healthy
// slot with nothing to report.
func TestStoppedSlotAdviceNamesTheRealDefault(t *testing.T) {
	t.Parallel()
	engine := literalAssignedTo(t, "../engines/postgres/cdc_reader.go", `defaultSlot\s*=\s*"([^"]+)"`)
	cli := literalAssignedTo(t, "../../cmd/sluice/sync_run.go", `return "([^"]+)"\s*\n\s*}\s*\n\s*return pipeline\.ResolveSlotName`)

	copies := map[string]string{
		"defaultSlotNameForAdvice (streamer_coldstart_stop.go)": defaultSlotNameForAdvice,
		"defaultPGSlotName (streamer_slot_health.go)":           defaultPGSlotName,
		"defaultActiveSlotName (add_table.go)":                  defaultActiveSlotName,
		"cmd/sluice resolvedSlotName literal (sync_run.go)":     cli,
	}
	for name, got := range copies {
		if got != engine {
			t.Errorf("%s = %q, but the PG engine creates %q. Re-point the copy rather than deleting this "+
				"check: advice or a probe naming a slot that does not exist is worse than none.", name, got, engine)
		}
	}
}

// literalAssignedTo reads a Go source file and returns the first capture
// of re, failing if the pattern no longer matches — a renamed anchor must
// fail here rather than silently bind to nothing.
func literalAssignedTo(t *testing.T, path, re string) string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := regexp.MustCompile(re).FindSubmatch(src)
	if m == nil {
		t.Fatalf("%s no longer matches %q — re-anchor this binding rather than deleting it", path, re)
	}
	return string(m[1])
}
