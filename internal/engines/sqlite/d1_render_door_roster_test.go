// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The D1 render-fidelity door, and every path that could bypass it.
//
// # Why a roster and not a promise
//
// Audit LA-5 was a guard that reached one lane of two. The `!`-flag premise
// under [CapturedValueExpr] has been probed at runtime since v0.131.2 — in the
// TRIGGER readers. The D1 READER lane projects the identical expression
// against the identical remote engine and probed nothing, because the
// rationale for the trigger door was written in terms of triggers ("on D1 the
// probed engine IS the engine that fires the triggers") and a `migrate` has no
// triggers. Nobody had to be careless for that to happen; the guard's scope
// was simply narrower than the class its own reasoning named.
//
// So the enumeration is mechanical rather than remembered. Every construction
// of a [D1RowReader] — the type that owns the projection — must be classified
// here as either reaching the door or exempt WITH A REASON. A new construction
// site fails this test until someone writes down which it is.
//
// # The classification, and why the exempt one is exempt
//
// `verifier.go`'s construction calls only `countRows`. A COUNT renders no
// value at all, so there is no REAL for the `!` flag to clamp — the exemption
// is about the absence of the rendered expression, not about the caller being
// trusted. If that site ever grows a row read, this roster's reason stops
// being true and the entry has to be re-argued.
func TestD1RenderDoorRoster_EveryRowReaderConstructionClassified(t *testing.T) {
	// file → why that construction does not need its own door call. A site
	// absent from this map must call verifyD1RenderFidelity in its own
	// function or in the function that constructs its client.
	exempt := map[string]string{
		"verifier.go": "count-only: calls countRows, which renders no value, so no REAL passes " +
			"through format('%!.20g') on this path",
	}
	// Files whose construction is reached by a door. Named explicitly so a
	// door that gets DELETED or moved shows up here as an unclassified site
	// rather than as silence.
	guarded := map[string]string{
		"d1.go":       "OpenRowReader calls verifyD1RenderFidelity before returning the reader",
		"stage_d1.go": "stageD1ClientToLocalFile calls verifyD1RenderFidelity before reading anything",
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	found := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, ok := cl.Type.(*ast.Ident)
			if !ok || id.Name != "D1RowReader" {
				return true
			}
			found[name]++
			return true
		})
	}

	// Anti-vacuity floor. If the AST walk stops finding constructions — a
	// rename, a builder function, a move to another file — this test would
	// otherwise pass by finding nothing to classify, which is the exact
	// failure it exists to prevent.
	if len(found) < 2 {
		t.Fatalf("the roster found D1RowReader constructions in only %d file(s): %v. "+
			"Expected at least the reader door and the stage-local path. The walk has stopped "+
			"seeing the constructions (renamed type? moved to a builder?), so this gate is "+
			"classifying nothing.", len(found), found)
	}

	for file := range found {
		if _, ok := guarded[file]; ok {
			continue
		}
		if reason, ok := exempt[file]; ok {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is exempt with an empty reason", file)
			}
			continue
		}
		t.Errorf("%s constructs a D1RowReader but is classified neither guarded nor exempt.\n"+
			"  That reader projects REAL values through format('%%!.20g', …). An engine that "+
			"ignores the `!` flag clamps to 16 significant digits and SILENTLY alters every one\n"+
			"  of them (the v0.131.2 CRITICAL). Either call verifyD1RenderFidelity on this path, "+
			"or add %q to the exempt map with a reason that says why no REAL is rendered here.",
			file, file)
	}

	// The other direction: a file claimed as guarded must actually name the
	// door. A stale claim here would be the roster defending the defect.
	for file, claim := range guarded {
		src, rerr := os.ReadFile(filepath.Clean(file))
		if rerr != nil {
			t.Errorf("guarded file %s is unreadable: %v", file, rerr)
			continue
		}
		if !strings.Contains(string(src), "verifyD1RenderFidelity") {
			t.Errorf("%s is claimed guarded (%q) but does not mention verifyD1RenderFidelity. "+
				"The door was moved or removed and this roster still says it is there.", file, claim)
		}
	}
}

// TestVerifyD1RenderFidelity_BothDirections grades the door itself against a
// stub client, including the fail-closed arm.
//
// The lossy case uses the render a 16-digit-clamping engine actually produces
// ("0.3"), measured on live Cloudflare D1 on 2026-09-07 by running the same
// format WITHOUT the `!` flag — so the negative fixture is a real engine's
// output rather than a value invented to fail.
func TestVerifyD1RenderFidelity_BothDirections(t *testing.T) {
	ctx := context.Background()

	t.Run("an honouring engine passes", func(t *testing.T) {
		// The exact bytes live D1 returned for the production expression.
		c := stubRenderClient(t, `"0.300000000000000044"`)
		if err := verifyD1RenderFidelity(ctx, c); err != nil {
			t.Fatalf("a correct render was refused: %v\n"+
				"This would refuse every working D1 source.", err)
		}
	})

	t.Run("a 16-digit clamp is refused", func(t *testing.T) {
		c := stubRenderClient(t, `"0.3"`)
		err := verifyD1RenderFidelity(ctx, c)
		if err == nil {
			t.Fatal("the 16-digit clamp was ACCEPTED. Every REAL read from this source would be " +
				"silently altered at exit 0 — the v0.131.2 CRITICAL, on the reader lane.")
		}
		if !strings.Contains(err.Error(), "LOSSILY") {
			t.Errorf("the refusal does not name the loss: %v", err)
		}
	})

	t.Run("a probe that cannot run fails CLOSED", func(t *testing.T) {
		c := stubRenderClientErr(t)
		if err := verifyD1RenderFidelity(ctx, c); err == nil {
			t.Fatal("an unrunnable probe was treated as permission to read. Reading without the " +
				"premise verified is what this door exists to prevent; it must fail closed, as " +
				"the trigger lane's door does.")
		}
	})

	t.Run("a non-text render is refused rather than coerced", func(t *testing.T) {
		// A JSON number means the format() arm did not run. Coercing it would
		// grade a value that had already been through a double.
		c := stubRenderClient(t, `0.30000000000000004`)
		if err := verifyD1RenderFidelity(ctx, c); err == nil {
			t.Fatal("a JSON-number render was accepted; that value cannot evidence the text render")
		}
	})
}

// stubRenderClient returns a d1Client, over the package's own mock D1 server,
// whose every query answers with a single row {"p": <raw>}. raw is spliced as
// literal JSON so a test can supply a non-string render (the coercion case),
// which d1OK's map[string]any could not express faithfully.
func stubRenderClient(t *testing.T, raw string) *d1Client {
	t.Helper()
	body := []byte(`{"success":true,"errors":[],"messages":[],"result":[{"success":true,` +
		`"results":[{"p":` + raw + `}],"meta":{}}]}`)
	var probe any
	if err := json.Unmarshal(body, &probe); err != nil {
		// A malformed fixture would fail the door for the wrong reason and
		// read as evidence about the door.
		t.Fatalf("stub envelope is not valid JSON: %v", err)
	}
	return startMockD1(t, func(string, []string) (int, []byte) {
		return http.StatusOK, body
	})
}

// stubRenderClientErr answers every query with a transport-level failure, so
// the probe cannot run at all.
func stubRenderClientErr(t *testing.T) *d1Client {
	t.Helper()
	return startMockD1(t, func(string, []string) (int, []byte) {
		return http.StatusInternalServerError, []byte(`{"success":false,"errors":[{"code":1,` +
			`"message":"probe unavailable"}],"messages":[],"result":[]}`)
	})
}
