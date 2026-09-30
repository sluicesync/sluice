// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestControlTimestampAge_Boundary pins the tolerance edge in both
// directions (GC-40 (c)): a row up to 60 s in the future is clock skew and
// readable; one second past it is not; any past row is readable.
func TestControlTimestampAge_Boundary(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		at       time.Time
		readable bool
	}{
		{"an hour old", now.Add(-time.Hour), true},
		{"now", now, true},
		{"skew inside the tolerance", now.Add(ControlTimestampSkewTolerance), true},
		{"one second past the tolerance", now.Add(ControlTimestampSkewTolerance + time.Second), false},
		{"a pre-v0.156.5 row on Asia/Tokyo (measured)", now.Add(32395 * time.Second), false},
	} {
		age, readable := ControlTimestampAge(now, tc.at)
		if readable != tc.readable {
			t.Errorf("%s: readable = %v, want %v", tc.name, readable, tc.readable)
		}
		if age != now.Sub(tc.at) {
			t.Errorf("%s: age = %v, want the raw %v", tc.name, age, now.Sub(tc.at))
		}
	}
}

// TestEmitMetrics_FutureDatedRowFailsClosed pins the metric half of GC-40
// (c): sluice_seconds_since_last_apply never emits a negative number. A
// future-dated row past the tolerance emits +Inf (which trips every `> N`
// alert) under a CONTROL-TIMESTAMP-IN-FUTURE comment; skew inside the
// tolerance reads 0.
func TestEmitMetrics_FutureDatedRowFailsClosed(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	emitMetrics(&buf, []ir.StreamStatus{
		{StreamID: "tokyo", UpdatedAt: now.Add(32395 * time.Second)},
		{StreamID: "skewed", UpdatedAt: now.Add(5 * time.Second)},
		{StreamID: "old", UpdatedAt: now.Add(-90 * time.Second)},
	}, now)
	out := buf.String()
	for _, want := range []string{
		`# CONTROL-TIMESTAMP-IN-FUTURE stream_id="tokyo": updated_at is 32395s in the future`,
		`sluice_seconds_since_last_apply{stream_id="tokyo"} +Inf`,
		`sluice_seconds_since_last_apply{stream_id="skewed"} 0`,
		`sluice_seconds_since_last_apply{stream_id="old"} 90`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape lacks %q; got:\n%s", want, out)
		}
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "sluice_seconds_since_last_apply{") && strings.Contains(line, "} -") {
			t.Errorf("negative sample emitted: %q", line)
		}
	}
	// The marker comment sits directly above the sample it explains.
	if !strings.Contains(out, "next position write\nsluice_seconds_since_last_apply{stream_id=\"tokyo\"} +Inf") {
		t.Errorf("marker comment is not directly above its sample:\n%s", out)
	}
}

// TestControlTimestampAge_EveryAgeConsumerRoutesThroughIt is the GC-40 (c)
// roster gate. It walks every non-test Go file under cmd/ and internal/ and
// finds each call that ages a `.UpdatedAt` field directly — `x.Sub(y.UpdatedAt)`
// or `time.Since(y.UpdatedAt)`. Such a call reads a negative (future-dated)
// age as fresh, which is the fail-open GC-40 (c) closed; a freshness
// consumer must go through [ControlTimestampAge] instead. Every remaining
// direct call must be listed below with the reason it is not a stream
// freshness verdict, keyed by file, enclosing function and the base
// identifier of the aged value, and every listed exemption must still
// match (a stale entry fails too).
//
// Reach, stated: the gate sees the `.UpdatedAt` spelling only. An age taken
// from a copied local (`t := st.UpdatedAt; now.Sub(t)`) is outside it.
func TestControlTimestampAge_EveryAgeConsumerRoutesThroughIt(t *testing.T) {
	exempt := map[string]string{
		// ir.MigrationState rows: migration_state.go has written UTC since
		// before GC-39, so no binary produced the zone-shifted row, and
		// these ages describe cold-start progress, not stream freshness.
		"cmd/sluice/status_render.go:writeColdStartsText:st": "cold-start progress row (sluice_migration_state, always UTC)",
		"cmd/sluice/status_render.go:renderStatusJSON:cs":    "cold-start progress row (sluice_migration_state, always UTC)",
		"cmd/sluice/sync_health.go:coldStartInFlight:st":     "cold-start progress row on the not-found (exit 2) path",
		// The summary aggregates are the raw per-row ages by design; each
		// row the aggregate spans is flagged individually in the same
		// render (AGE cell / control_timestamp_in_future).
		"cmd/sluice/status_render.go:agesSpan:streams": "aggregate of raw ages; each future-dated row is flagged in the same render",
		"cmd/sluice/status_render.go:agesSpan:st":      "aggregate of raw ages; each future-dated row is flagged in the same render",
		// A negative heartbeat age reads as FRESH, and fresh REFUSES a
		// second concurrent backfill: the fail-closed direction.
		"internal/pipeline/backfill.go:runWalk:state": "concurrent-run guard; a future-dated heartbeat refuses (fail closed)",
	}

	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	found := map[string]bool{}
	var violations []string
	files, routed := 0, 0
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			files++
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "ControlTimestampAge" {
							routed++
						}
						return true
					}
					if sel.Sel.Name == "ControlTimestampAge" {
						routed++
						return true
					}
					if sel.Sel.Name != "Sub" && sel.Sel.Name != "Since" {
						return true
					}
					for _, arg := range call.Args {
						base, ok := updatedAtBase(arg)
						if !ok {
							continue
						}
						key := rel + ":" + fn.Name.Name + ":" + base
						if _, ok := exempt[key]; ok {
							found[key] = true
							continue
						}
						violations = append(violations, key+" at "+fset.Position(call.Pos()).String())
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	// Anti-vacuity: the walk covered the tree, and the consumers that were
	// routed are still routed (health, status text, status JSON, the
	// metric, the live panel). Errorf, not Fatalf, so a detection below
	// is reported beside a floor breach rather than hidden behind it.
	if files < 500 {
		t.Errorf("walked only %d Go files; the gate is not seeing the tree", files)
	}
	if routed < 5 {
		t.Errorf("found %d ControlTimestampAge call sites, want >= 5; a consumer stopped routing through it", routed)
	}
	for _, v := range violations {
		t.Errorf("%s ages a .UpdatedAt directly: a future-dated row would read as fresh (GC-40 (c)); route it through pipeline.ControlTimestampAge or exempt it here with a reason", v)
	}
	for key := range exempt {
		if !found[key] {
			t.Errorf("exemption %q matches nothing; remove it", key)
		}
	}
}

// updatedAtBase reports whether e is `<base>.UpdatedAt` and returns the base
// identifier's name (the indexed identifier for `xs[i].UpdatedAt`).
func updatedAtBase(e ast.Expr) (string, bool) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "UpdatedAt" {
		return "", false
	}
	x := sel.X
	if ix, ok := x.(*ast.IndexExpr); ok {
		x = ix.X
	}
	if id, ok := x.(*ast.Ident); ok {
		return id.Name, true
	}
	return "<expr>", true
}
