//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_MarksCoverReason is the MySQL half of ADR-0191 §3.5's
// target rows, graded against a real server (the Postgres twin is
// postgres.TestChangeApplier_MarksCoverReason): a keyless target table is
// covered by table-wide marks, one keyed only on an AUTO_INCREMENT surrogate
// the rows do not carry is not, an absent one is judged by its recorded key,
// and a mark table this role cannot use is APPLY-MARKS-UNAVAILABLE.
func TestChangeApplier_MarksCoverReason(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `
		CREATE TABLE kl (v INT NOT NULL, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE sur (sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
		CREATE TABLE keyed (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB;`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	prober := applier.(ir.ApplyMarksCoverageProber)
	recorded := func(name string, cols ...string) *ir.Table {
		t := &ir.Table{Name: name}
		for _, c := range cols {
			t.Columns = append(t.Columns, &ir.Column{Name: c, Type: ir.Integer{Width: 32}})
		}
		return t
	}
	for _, tc := range []struct {
		name  string
		table *ir.Table
		want  string
	}{
		{"keyless on the target: table-wide marks", recorded("kl", "v", "note"), ""},
		{"keyed on an unsupplied AUTO_INCREMENT surrogate", recorded("sur", "v"), `"sid"`},
		{"keyed on a supplied column", recorded("keyed", "id", "v"), ""},
		{"absent: judged by its recorded key (none)", recorded("absent", "v"), ""},
		{"target level only", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			why, err := prober.MarksCoverReason(ctx, tc.table)
			if err != nil {
				t.Fatal(err)
			}
			if (tc.want == "") != (why == "") || !strings.Contains(why, tc.want) {
				t.Errorf("MarksCoverReason = %q; want %q", why, tc.want)
			}
		})
	}
	t.Run("mark table unusable", func(t *testing.T) {
		applyMySQLApplier(t, dsn, `DROP TABLE sluice_cdc_apply_marks;`)
		why, err := prober.MarksCoverReason(ctx, recorded("kl", "v", "note"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(why, applymarks.UnavailableMarker) {
			t.Errorf("MarksCoverReason with no mark table = %q; want %s", why, applymarks.UnavailableMarker)
		}
	})
}
