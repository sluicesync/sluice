// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// copyShapeBaseline is the run every case below perturbs by exactly
// one input. Every covered aspect is ENGAGED here, so each case can
// also be read in the other direction: removing the flag drifts too.
func copyShapeBaseline() (*Streamer, *ir.Schema) {
	reg := redact.New()
	reg.Set("public", "users", "email", redact.MaskEmail{})
	s := &Streamer{
		RowFilters:         map[string]string{"orders": "created_at >= '2026-01-01'"},
		Mappings:           []config.Mapping{{Table: "users", Column: "id", TargetType: "BIGINT"}},
		ExpressionMappings: []config.ExpressionMapping{{Table: "users", Column: "full", Expression: "a || b"}},
		Redactor:           reg,
		InjectShardColumn:  ShardColumnSpec{Name: "shard_id", Value: "us-east-1"},
		TargetSchema:       "analytics",
	}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Schema: "public", Name: "users"},
		{Schema: "public", Name: "orders"},
	}}
	return s, schema
}

// TestColdStartCopyShape_EveryAspectDrifts is the enumeration held to
// the code: for each operator input the fingerprint claims to cover,
// change it and require EXACTLY its key to drift.
//
// "Exactly" is the load-bearing half in both directions. A key that
// does not drift is an input the resume would inherit silently — the
// C-1 class. A key that drifts when something unrelated changed is a
// false refusal that costs a re-copy.
func TestColdStartCopyShape_EveryAspectDrifts(t *testing.T) {
	cases := []struct {
		key    string
		change func(*Streamer, *ir.Schema)
		why    string
	}{
		{
			key:    "where",
			change: func(s *Streamer, _ *ir.Schema) { s.RowFilters = nil },
			why:    "a REMOVED --where: run 1 copied a subset and run 2 believes it has everything",
		},
		{
			key:    "where",
			change: func(s *Streamer, _ *ir.Schema) { s.RowFilters = map[string]string{"orders": "1=1"} },
			why:    "a WIDENED --where: the rows the old predicate excluded are absent forever",
		},
		{
			key: "tables",
			change: func(_ *Streamer, sc *ir.Schema) {
				sc.Tables = append(sc.Tables, &ir.Table{Schema: "public", Name: "audit"})
			},
			why: "a table added to the scope was never copied",
		},
		{
			key:    "types",
			change: func(s *Streamer, _ *ir.Schema) { s.Mappings[0].TargetType = "TEXT" },
			why:    "a --type-override change describes columns run 1 created differently",
		},
		{
			key:    "types",
			change: func(s *Streamer, _ *ir.Schema) { s.ExpressionMappings[0].Expression = "b || a" },
			why:    "an expression override is the same class as a type override",
		},
		{
			key: "redact",
			change: func(s *Streamer, _ *ir.Schema) {
				reg := redact.New()
				reg.Set("public", "users", "phone", redact.MaskEmail{})
				s.Redactor = reg
			},
			why: "a --redact change leaves the target redacted by copy-vs-CDC provenance",
		},
		{
			key:    "redact",
			change: func(s *Streamer, _ *ir.Schema) { s.Redactor = nil },
			why:    "redaction REMOVED is the direction that leaks",
		},
		{
			key:    "shard",
			change: func(s *Streamer, _ *ir.Schema) { s.InjectShardColumn = ShardColumnSpec{} },
			why:    "the discriminator column and its value are part of every copied row",
		},
		{
			key:    "shard",
			change: func(s *Streamer, _ *ir.Schema) { s.InjectShardColumn.Value = "eu-west-1" },
			why:    "the same column with a different value stamps different rows",
		},
		{
			key:    "target_schema",
			change: func(s *Streamer, _ *ir.Schema) { s.TargetSchema = "public" },
			why:    "run 1's rows are in the other namespace",
		},
		{
			key:    "skip_fks",
			change: func(s *Streamer, _ *ir.Schema) { s.SkipForeignKeys = true },
			why:    "advisory, but it must still be DETECTED to be reportable",
		},
		{
			key: "views",
			change: func(_ *Streamer, sc *ir.Schema) {
				sc.Views = []*ir.View{{Schema: "public", Name: "extra", Definition: "SELECT 1"}}
			},
			why: "advisory, same reason",
		},
	}

	for _, tc := range cases {
		t.Run(tc.key+": "+tc.why, func(t *testing.T) {
			base, baseSchema := copyShapeBaseline()
			recorded := coldStartCopyShape(base, baseSchema)

			changed, changedSchema := copyShapeBaseline()
			tc.change(changed, changedSchema)
			current := coldStartCopyShape(changed, changedSchema)

			drifted := copyShapeDrift(recorded, current)
			if !reflect.DeepEqual(drifted, []string{tc.key}) {
				t.Fatalf("changing %s drifted %v; want exactly [%s].\nrecorded: %s\ncurrent:  %s",
					tc.key, drifted, tc.key, recorded, current)
			}
		})
	}
}

// TestColdStartCopyShape_StableAcrossIrrelevantChanges is the other
// half: inputs that do NOT change what the copy leaves behind must not
// refuse a healthy resume.
func TestColdStartCopyShape_StableAcrossIrrelevantChanges(t *testing.T) {
	base, schema := copyShapeBaseline()
	recorded := coldStartCopyShape(base, schema)

	changed, changedSchema := copyShapeBaseline()
	// Performance and post-copy-phase knobs, each with its reason in
	// coldStartCopyShape's doc.
	changed.UpfrontIndexes = true
	changed.AnalyzeAfter = true
	changed.BulkParallelism = 8
	changed.TableParallelism = 4
	changed.MaxBufferBytes = 1 << 20
	changed.SlotName = "other"
	// And the table set in a different ORDER: the scope is a set, not a
	// sequence, so a schema reader that returned it differently must not
	// refuse.
	changedSchema.Tables = []*ir.Table{
		{Schema: "public", Name: "orders"},
		{Schema: "public", Name: "users"},
	}

	if drifted := copyShapeDrift(recorded, coldStartCopyShape(changed, changedSchema)); len(drifted) != 0 {
		t.Fatalf("irrelevant changes drifted %v; a false refusal here costs the operator a full re-copy", drifted)
	}
}

// TestCopyShapeAdvisory_EveryKeyIsClassified is the policy pin: every
// key the fingerprint can emit lands on one side of the
// refuse-or-warn split, and the DEFAULT for an unrecognised key is to
// refuse.
//
// The default matters more than the current membership. A key added
// later without a decision must fail closed — the alternative is a new
// shaping input that silently warns, which is the class this whole
// gate exists to close.
func TestColdStartCopyShape_EveryKeyIsClassified(t *testing.T) {
	shape := coldStartCopyShape(&Streamer{}, &ir.Schema{Tables: []*ir.Table{{Name: "t"}}})
	fields := strings.Split(shape, ";")
	keys := make([]string, 0, len(fields))
	for _, field := range fields {
		k, _, _ := strings.Cut(field, "=")
		keys = append(keys, k)
	}
	refusing, advisory := copyShapeAdvisory(keys)
	if len(refusing)+len(advisory) != len(keys) {
		t.Fatalf("the split dropped keys: %d in, %d refusing + %d advisory", len(keys), len(refusing), len(advisory))
	}
	if !reflect.DeepEqual(advisory, []string{"skip_fks", "views"}) {
		t.Errorf("advisory keys = %v; want exactly [skip_fks views] — a key becomes advisory only with the "+
			"argument in coldStartCopyShape's doc, which is that a difference reaches the target as DDL "+
			"rather than as rows", advisory)
	}
	if len(refusing) < 6 {
		t.Errorf("only %d refusing keys (%v); the row-shaping inputs must not drift into the advisory set",
			len(refusing), refusing)
	}
	// An unrecognised key — the one a future change adds without
	// deciding — must refuse.
	r, a := copyShapeAdvisory([]string{"some_future_input"})
	if len(a) != 0 || len(r) != 1 {
		t.Errorf("an unclassified key split as refusing=%v advisory=%v; it must fail CLOSED", r, a)
	}
}

// TestCopyShapeDrift_UnknownAndMissingKeys pins the two asymmetries
// the comparison deliberately has.
func TestCopyShapeDrift_UnknownAndMissingKeys(t *testing.T) {
	t.Run("a key this binary grades but the record lacks is DRIFT", func(t *testing.T) {
		// An older sluice that did not grade `redact` recorded nothing
		// for it. That is unproven, not unchanged — and the direction
		// matters: treating it as unchanged would inherit a copy whose
		// redaction policy nobody checked.
		drifted := copyShapeDrift("where=aaaa;tables=bbbb", "where=aaaa;tables=bbbb;redact=cccc")
		if !reflect.DeepEqual(drifted, []string{"redact"}) {
			t.Fatalf("drifted = %v; want [redact]", drifted)
		}
	})
	t.Run("a key the record has and this binary does not is ignored", func(t *testing.T) {
		// A NEWER sluice recorded an aspect this binary cannot compute.
		// Refusing on it would break every resume after a downgrade,
		// and this binary has nothing to compare it against anyway.
		if drifted := copyShapeDrift("where=aaaa;future=zzzz", "where=aaaa"); len(drifted) != 0 {
			t.Fatalf("drifted = %v; want none", drifted)
		}
	})
	t.Run("an empty recorded value drifts everything", func(t *testing.T) {
		// The caller treats "" as no-evidence BEFORE calling this, but
		// if it ever reaches here it must not read as agreement.
		if drifted := copyShapeDrift("", "where=aaaa"); len(drifted) == 0 {
			t.Fatal("an EMPTY recorded shape compared equal to a populated one")
		}
	})
	t.Run("identical shapes do not drift", func(t *testing.T) {
		if drifted := copyShapeDrift("a=1;b=2", "a=1;b=2"); len(drifted) != 0 {
			t.Fatalf("drifted = %v on identical shapes", drifted)
		}
	})
}

// TestColdStartCopyShape_Rendering pins the wire shape: sorted, one
// `key=hash` per aspect, every aspect present even when its flag is
// unset. The last part is what makes a REMOVED flag detectable — an
// encoding that omitted absent aspects would render the expensive
// drift as no change at all.
func TestColdStartCopyShape_Rendering(t *testing.T) {
	shape := coldStartCopyShape(&Streamer{}, &ir.Schema{Tables: []*ir.Table{{Name: "t"}}})
	wantKeys := []string{"redact", "shard", "skip_fks", "tables", "target_schema", "types", "views", "where"}
	fields := strings.Split(shape, ";")
	gotKeys := make([]string, 0, len(fields))
	for _, field := range fields {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			t.Fatalf("field %q is not key=value in %q", field, shape)
		}
		if v == "" {
			t.Errorf("aspect %q rendered an EMPTY hash; an unset flag must still hash to something, or "+
				"removing it would read as no change", k)
		}
		gotKeys = append(gotKeys, k)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("rendered keys %v; want %v in that order", gotKeys, wantKeys)
	}
	// The fingerprint must carry no operator content: a `--where`
	// predicate can name business data, and this value lands in a
	// control table an operator may share in a bug report.
	leaky, _ := copyShapeBaseline()
	_, leakySchema := copyShapeBaseline()
	if got := coldStartCopyShape(leaky, leakySchema); strings.Contains(got, "created_at") ||
		strings.Contains(got, "us-east-1") || strings.Contains(got, "analytics") {
		t.Errorf("the copy shape leaked operator input verbatim: %q", got)
	}
}
