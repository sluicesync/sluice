// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFirstBoundaryWitness_ShapeAKeylessBoundary pins SHAPE-A-KEYLESS-BOUNDARY:
// a Shape A stream's boundary from a PlanetScale/Vitess source is projected
// from VStream FIELD events, which carry no key, so the witness must compare
// it without the discriminator's key fold — and still see a real column
// difference. Measured before the fix on vttestserver: every Shape A stream
// from a VStream source stopped at its first change with "InjectShardColumn:
// table has no primary key".
func TestFirstBoundaryWitness_ShapeAKeylessBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dt := func(p int) ir.Type { return ir.DateTime{Precision: p} }
	// The consolidated target as the cold start created it: the discriminator
	// appended and leading the composite key.
	target := witnessTable(wcol("ts", dt(6)), &ir.Column{Name: "src_shard", Type: ir.Varchar{Length: 64}, SluiceInjected: true})
	target.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "src_shard"}, {Column: "id"}}}
	keylessProjection := func(cols ...*ir.Column) *ir.Table {
		tbl := witnessTable(cols...)
		tbl.PrimaryKey = nil
		return tbl
	}
	for _, pair := range [][2]string{{"mysql", "mysql"}, {"mysql", "postgres"}} {
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": target}}, pair[0], pair[1])
		w.shardColumn = "src_shard"

		// A lane whose boundaries carry the key (binlog, pgoutput): a
		// keyless boundary is a source that lost its key, still refused.
		if _, err := w.verdict(ctx, keylessProjection(wcol("ts", dt(6))), ir.Position{}, nil); err == nil ||
			!strings.Contains(err.Error(), "has no primary key") {
			t.Errorf("%s→%s: keyed-boundary lane, keyless boundary: err = %v; want the no-primary-key refusal", pair[0], pair[1], err)
		}
		w.keylessBoundaries = true

		// A source column already named like the discriminator: refused, as
		// InjectShardColumn refuses it. Matching would let the applier's
		// shard stamp overwrite the source's own column on every row.
		_, err := w.verdict(ctx, keylessProjection(wcol("ts", dt(6)), wcol("src_shard", ir.Integer{Width: 32})), ir.Position{}, nil)
		if err == nil || !strings.Contains(err.Error(), `already has a column named "src_shard"`) {
			t.Errorf("%s→%s: keyless boundary carrying its own src_shard: err = %v; want the column-collision refusal", pair[0], pair[1], err)
		}

		v, err := w.verdict(ctx, keylessProjection(wcol("ts", dt(6))), ir.Position{}, nil)
		if err != nil || v.kind != witnessMatch {
			t.Errorf("%s→%s: the keyless projection of an unchanged table: verdict = %d, %v (%s); want match",
				pair[0], pair[1], v.kind, err, v.render())
		}

		v, err = w.verdict(ctx, keylessProjection(wcol("ts", dt(0))), ir.Position{}, nil)
		if err != nil || v.kind == witnessMatch {
			t.Errorf("%s→%s: a narrowed column in a keyless projection: verdict = %d, %v; want a difference, not a match",
				pair[0], pair[1], v.kind, err)
		}

		v, err = w.verdict(ctx, witnessTable(wcol("ts", dt(6))), ir.Position{}, nil)
		if err != nil || v.kind != witnessMatch {
			t.Errorf("%s→%s: a keyed boundary (the binlog / pgoutput shape): verdict = %d, %v (%s); want match",
				pair[0], pair[1], v.kind, err, v.render())
		}
	}
}
