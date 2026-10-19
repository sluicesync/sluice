// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// identitySource is a source whose table ids are fixed: proof says whether
// they survive a rebuild (Postgres) or not (InnoDB).
type identitySource struct {
	stubEngine
	ids   map[string]string
	proof bool
}

func (s identitySource) TableIdentities(_ context.Context, _ string, tables []string) (map[string]string, error) {
	out := map[string]string{}
	for _, t := range tables {
		if id, ok := s.ids[t]; ok {
			out[t] = id
		}
	}
	return out, nil
}

func (s identitySource) TableIdentitySurvivesRebuild() bool { return s.proof }

var _ ir.TableIdentityReader = identitySource{}

func identitySchema(names ...string) *ir.Schema {
	sch := &ir.Schema{}
	for _, n := range names {
		sch.Tables = append(sch.Tables, &ir.Table{Name: n})
	}
	return sch
}

// TestResumeTableIdentityGate covers the gate's arms: what stands the
// resume down, what refuses, what passes.
func TestResumeTableIdentityGate(t *testing.T) {
	schema := identitySchema("ka", "kb")
	for _, tc := range []struct {
		name      string
		recorded  string // the copy shape's table_ids aspect; "-" for none
		current   map[string]string
		proof     bool
		pass      bool
		refusal   []string // substrings the refusal must name
		copiedIDs map[string]string
	}{
		{name: "no table_ids key", recorded: "-", current: map[string]string{"ka": "1", "kb": "2"}, proof: true},
		{name: "count mismatch", recorded: "1", current: map[string]string{"ka": "1", "kb": "2"}, proof: true},
		{name: "recorded id empty", recorded: ".2", current: map[string]string{"ka": "1", "kb": "2"}, proof: true},
		{name: "current id empty", recorded: "1.2", current: map[string]string{"kb": "2"}, proof: true},
		{name: "ids that are not proof", recorded: "1.2", current: map[string]string{"ka": "1", "kb": "2"}},
		{
			name: "swapped ids", recorded: "1.2", current: map[string]string{"ka": "2", "kb": "1"}, proof: true,
			refusal: []string{"TABLE-IDENTITY-CHANGED", "ka (copied as id 1, now id 2)", "kb (copied as id 2, now id 1)"},
		},
		{
			name: "equal", recorded: "1.2", current: map[string]string{"ka": "1", "kb": "2"}, proof: true, pass: true,
			copiedIDs: map[string]string{"ka": "1", "kb": "2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Streamer{Source: identitySource{ids: tc.current, proof: tc.proof}, SourceDSN: "src"}
			shape := "tables=x"
			if tc.recorded != "-" {
				shape += ";" + copyShapeTableIDsKey + "=" + tc.recorded
			}
			ok, refusal := s.resumeTableIdentityGate(context.Background(), "s1", ir.MigrationState{CopyShape: shape}, schema)
			switch {
			case tc.refusal != nil:
				if ok || refusal == nil {
					t.Fatalf("got ok=%v refusal=%v; want a refusal", ok, refusal)
				}
				for _, want := range tc.refusal {
					if !strings.Contains(refusal.Error(), want) {
						t.Fatalf("the refusal does not name %q: %v", want, refusal)
					}
				}
			case tc.pass:
				if !ok || refusal != nil {
					t.Fatalf("got ok=%v refusal=%v; want it to pass", ok, refusal)
				}
				if !reflect.DeepEqual(s.coldStartIDs.copied, tc.copiedIDs) || !s.coldStartIDs.proof {
					t.Fatalf("a passing gate left the lane's copy-time ids %+v; want %v", s.coldStartIDs, tc.copiedIDs)
				}
			default:
				if ok || refusal != nil {
					t.Fatalf("got ok=%v refusal=%v; want the resume to stand down", ok, refusal)
				}
			}
		})
	}
}

// TestAddTableVerifiedTableIdentity: the add records the id its copy
// started with, refuses one that moved, and keeps a fresh read where ids
// are not proof.
func TestAddTableVerifiedTableIdentity(t *testing.T) {
	ctx := context.Background()
	moved := &AddTable{Source: identitySource{ids: map[string]string{"kx": "9"}, proof: true}, TableName: "kx", copyTimeID: "1"}
	if _, err := moved.verifiedTableIdentity(ctx); err == nil || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") ||
		!strings.Contains(err.Error(), "copied as id 1, now id 9") {
		t.Fatalf("a table re-created during the add: %v; want TABLE-IDENTITY-CHANGED naming both ids", err)
	}
	same := &AddTable{Source: identitySource{ids: map[string]string{"kx": "1"}, proof: true}, TableName: "kx", copyTimeID: "1"}
	if id, err := same.verifiedTableIdentity(ctx); err != nil || id != "1" {
		t.Fatalf("an unchanged table: %q, %v; want 1", id, err)
	}
	innodb := &AddTable{Source: identitySource{ids: map[string]string{"kx": "9"}}, TableName: "kx", copyTimeID: "1"}
	if id, err := innodb.verifiedTableIdentity(ctx); err != nil || id != "9" {
		t.Fatalf("ids that are not proof: %q, %v; want the fresh read 9", id, err)
	}
}

// TestAnchorTableIDs covers the anchor's re-read: a moved id refuses
// naming both, the copy-time ids are what is recorded, and a source whose
// ids are not proof records a fresh read as before.
func TestAnchorTableIDs(t *testing.T) {
	keys := []string{"ka", "kb"}
	moved := &Streamer{Source: identitySource{ids: map[string]string{"ka": "1", "kb": "9"}, proof: true}, SourceDSN: "src"}
	moved.coldStartIDs = coldStartIdentities{copied: map[string]string{"ka": "1", "kb": "2"}, proof: true}
	if _, err := moved.anchorTableIDs(context.Background(), keys); err == nil ||
		!strings.Contains(err.Error(), "kb (copied as id 2, now id 9)") || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") {
		t.Fatalf("a moved id at the anchor: %v; want TABLE-IDENTITY-CHANGED naming kb", err)
	}
	unread := &Streamer{Source: identitySource{ids: map[string]string{"ka": "1"}, proof: true}, SourceDSN: "src"}
	unread.coldStartIDs = coldStartIdentities{copied: map[string]string{"ka": "1", "kb": "2"}, proof: true}
	ids, err := unread.anchorTableIDs(context.Background(), keys)
	if err != nil || !reflect.DeepEqual(ids, map[string]string{"ka": "1", "kb": "2"}) {
		t.Fatalf("an id the re-read cannot see: %v, %v; want the copy-time ids recorded", ids, err)
	}
	if got := unread.recordedTableIDs(context.Background(), keys); !reflect.DeepEqual(got, ids) {
		t.Fatalf("the record after the anchor carries %v; want the anchor's %v", got, ids)
	}
	uncaptured := &Streamer{Source: identitySource{ids: map[string]string{"ka": "1", "kb": "2"}, proof: true}, SourceDSN: "src"}
	if ids, err := uncaptured.anchorTableIDs(context.Background(), keys); err != nil || len(ids) != 0 {
		t.Fatalf("a lane that captured no copy-time ids: %v, %v; want none recorded", ids, err)
	}
	innodb := &Streamer{Source: identitySource{ids: map[string]string{"ka": "5", "kb": "6"}}, SourceDSN: "src"}
	innodb.coldStartIDs = coldStartIdentities{copied: map[string]string{"ka": "1", "kb": "2"}}
	if ids, err := innodb.anchorTableIDs(context.Background(), keys); err != nil || !reflect.DeepEqual(ids, map[string]string{"ka": "5", "kb": "6"}) {
		t.Fatalf("ids that are not proof: %v, %v; want the fresh read", ids, err)
	}
}
