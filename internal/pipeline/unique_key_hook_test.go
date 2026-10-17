// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUniqueKeyBoundaryHook_SchemaIntercepts pins that the schema intercepts
// that forward a snapshot — forward and refuse here, Shape A by the same
// call at both of its snapshot sends — judge it through the unique-key
// boundary hook first, and withhold it when the hook stops the stream. The
// hook replaced a downstream channel stage, which held earlier changes past
// the DDL the forward intercept had already applied (round-3 review).
func TestUniqueKeyBoundaryHook_SchemaIntercepts(t *testing.T) {
	snap := addColForwardSnap(addColForwardTable("users"))
	added := addColForwardSnap(addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true}))
	for _, verdict := range []bool{true, false} {
		var judged []string
		hook := func(_ context.Context, s ir.SchemaSnapshot) bool {
			judged = append(judged, s.Table)
			return verdict
		}
		want := 0
		if verdict {
			want = 1
		}

		// Forward: the anchor (the seed branch) and then an ADD COLUMN (the
		// routed branch's accept()), each judged before it is forwarded.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		in := make(chan ir.Change, 2)
		in <- snap
		in <- added
		close(in)
		out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
			applier: &fakeShapeApplier{}, sourceEngineName: "postgres", targetEngineName: "postgres", uniqueKeys: hook,
		}, &atomic.Pointer[error]{})
		wantJudged := "users,users"
		if !verdict {
			wantJudged = "users" // the anchor's refusal stops it
		}
		if got := drainChannel(t, out, 5*time.Second); len(got) != 2*want || strings.Join(judged, ",") != wantJudged {
			t.Errorf("forward, verdict %v: forwarded %d, judged %v", verdict, len(got), judged)
		}
		cancel()
		if !verdict {
			// The routed branch alone: an anchor the hook passes, then the
			// ADD COLUMN it stops.
			judged = nil
			n := 0
			routed := func(_ context.Context, s ir.SchemaSnapshot) bool {
				judged = append(judged, s.Table)
				n++
				return n == 1
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			in := make(chan ir.Change, 2)
			in <- snap
			in <- added
			close(in)
			out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
				applier: &fakeShapeApplier{}, sourceEngineName: "postgres", targetEngineName: "postgres", uniqueKeys: routed,
			}, &atomic.Pointer[error]{})
			if got := drainChannel(t, out, 5*time.Second); len(got) != 1 || strings.Join(judged, ",") != "users,users" {
				t.Errorf("forward accept(): forwarded %d, judged %v; want the ADD COLUMN judged and withheld", len(got), judged)
			}
			cancel()
		}

		judged = nil
		got, err := runRefuseIntercept(t, unforwardedBoundaryDeps{why: "--schema-changes=refuse", uniqueKeys: hook}, snap)
		if err != nil || len(got) != want || strings.Join(judged, ",") != "users" {
			t.Errorf("refuse, verdict %v: forwarded %d, judged %v, err %v", verdict, len(got), judged, err)
		}
	}
}

// ukReadFailEngine is an engine whose schema reader cannot be opened.
type ukReadFailEngine struct{ stubEngine }

func (ukReadFailEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("catalog down")
}

// TestAddTableUniqueKeyDoor_ReadFailureRefuses pins the round-3 add-table
// fix's fail-closed half: a target catalog that cannot be read refuses the
// command, where the shape gate's read only WARNed and skipped the door.
func TestAddTableUniqueKeyDoor_ReadFailureRefuses(t *testing.T) {
	a := &AddTable{Source: ukReadFailEngine{}, Target: ukReadFailEngine{}}
	err := a.refuseTargetOnlyUniqueKeys(context.Background(), &ir.Table{Name: "kd"}, "")
	if err == nil || !strings.Contains(err.Error(), "catalog down") {
		t.Fatalf("err = %v; want the read failure to refuse", err)
	}
}

// TestUniqueKeyBoundaryGate_BeforeDDL pins the gate's two calls (round-4
// review): a boundary that only adds columns is judged at the early call,
// before its DDL, and the late call for it then judges nothing; a boundary
// that drops a column is not judged early (its DDL can change the target's
// keys) and is judged at the late call.
func TestUniqueKeyBoundaryGate_BeforeDDL(t *testing.T) {
	var judged []string
	gate := newUniqueKeyBoundaryGate(uniqueKeyChecks{
		boundary: func(_ context.Context, s ir.SchemaSnapshot) (*ir.SourceUniqueKeyAbsentError, error) {
			judged = append(judged, columnList(s.IR))
			return nil, nil
		},
		colsKey:  func(_, table string) string { return table },
		seedCols: map[string]string{"kd": columnList(ukrSnap("kd", "id", "k", "doomed").IR)},
	}, &atomic.Pointer[error]{})
	early, late := gate.beforeDDLHook(), gate.hook()
	ctx := context.Background()
	snap := func(cols ...string) ir.SchemaSnapshot {
		s := ukrSnap("kd", cols...)
		s.Schema = "public"
		return s
	}

	added := snap("id", "k", "doomed", "n")
	if !early(ctx, added) || len(judged) != 1 {
		t.Fatalf("an added column was not judged early: %d checks", len(judged))
	}
	if !late(ctx, added) || len(judged) != 1 {
		t.Fatalf("the late call judged the early-judged boundary again: %d checks", len(judged))
	}

	dropped := snap("id", "k", "n")
	if !early(ctx, dropped) || len(judged) != 1 {
		t.Fatalf("a dropped column was judged before its DDL: %d checks", len(judged))
	}
	if !late(ctx, dropped) || len(judged) != 2 {
		t.Fatalf("a dropped column was not judged after its DDL: %d checks", len(judged))
	}
}
