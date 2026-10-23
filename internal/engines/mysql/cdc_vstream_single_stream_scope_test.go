// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// singleStreamScopeDSN is a VStream DSN whose endpoint nothing listens on:
// a refusal must come back before anything dials it.
func singleStreamScopeDSN(param string) string {
	dsn := "u:p@tcp(127.0.0.1:1)/commerce?vstream_endpoint=127.0.0.1:1&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"
	if param != "" {
		dsn += "&vstream_copy_single_stream=" + param
	}
	return dsn
}

func scopeOf(n int) []string {
	if n < 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("t%02d", i)
	}
	return out
}

// TestCheckSnapshotScope_SingleStreamMultiTable pins the
// VSTREAM-SINGLE-STREAM-MULTI-TABLE predicate across scope sizes (none
// listed, one, two, many) × the parameter's spellings × flavor. Refused
// exactly when a VStream flavor is given `vstream_copy_single_stream=true`
// with more than one listed table: the only combination in which the open
// copies several tables in one stream. A spelling the open does not read as
// the opt-out ("TRUE", "1") copies per table, so it is not refused either.
func TestCheckSnapshotScope_SingleStreamMultiTable(t *testing.T) {
	sizes := []struct {
		name string
		n    int
	}{{"none-listed", -1}, {"empty", 0}, {"one", 1}, {"two", 2}, {"many", 7}}
	params := []struct {
		value     string
		singleOpt bool
	}{{"", false}, {"true", true}, {"false", false}, {"TRUE", false}, {"1", false}}
	flavors := []Flavor{FlavorVanilla, FlavorMariaDB, FlavorPlanetScale, FlavorVitess}

	for _, fl := range flavors {
		for _, sz := range sizes {
			for _, p := range params {
				name := fmt.Sprintf("%v/%s/param=%q", fl, sz.name, p.value)
				t.Run(name, func(t *testing.T) {
					tables := scopeOf(sz.n)
					err := Engine{Flavor: fl}.CheckSnapshotScope(singleStreamScopeDSN(p.value), tables)
					want := fl.usesVStream() && p.singleOpt && len(tables) > 1
					if !want {
						if err != nil {
							t.Fatalf("CheckSnapshotScope = %v; want nil", err)
						}
						return
					}
					if !errors.Is(err, ir.ErrVStreamSingleStreamMultiTable) {
						t.Fatalf("CheckSnapshotScope = %v; want the VSTREAM-SINGLE-STREAM-MULTI-TABLE refusal", err)
					}
					for _, w := range []string{"VSTREAM-SINGLE-STREAM-MULTI-TABLE", "Remove vstream_copy_single_stream", fmt.Sprintf("%d tables", len(tables)), tables[0]} {
						if !strings.Contains(err.Error(), w) {
							t.Errorf("refusal does not name %q: %v", w, err)
						}
					}
				})
			}
		}
	}
}

// TestVStreamSnapshotOpeners_RefuseSingleStreamMultiTableBeforeDialing pins
// the door inside the open, through every exported VStream opener that takes
// a table list: each refuses a two-table single-stream scope before it dials
// (the endpoint is closed, so a dial would fail differently and later). The
// cursor-seeded resumes (OpenSnapshotStreamFromPosition*) reach the same
// openVStreamSnapshotStreamFrom after decoding a cursor and have no
// production caller (VSTREAM-COPY-RESUME-DEAD-MACHINERY).
func TestVStreamSnapshotOpeners_RefuseSingleStreamMultiTableBeforeDialing(t *testing.T) {
	eng := Engine{Flavor: FlavorPlanetScale}
	dsn := singleStreamScopeDSN("true")
	tables := []string{"a", "b"}
	openers := map[string]func(ctx context.Context) error{
		"OpenSnapshotStreamForTables": func(ctx context.Context) error {
			_, err := eng.OpenSnapshotStreamForTables(ctx, dsn, tables)
			return err
		},
		"OpenSnapshotStreamForTablesFiltered": func(ctx context.Context) error {
			_, err := eng.OpenSnapshotStreamForTablesFiltered(ctx, dsn, tables, map[string]string{"a": "id > 0"})
			return err
		},
		"OpenBackupSnapshotForTables": func(ctx context.Context) error {
			_, err := eng.OpenBackupSnapshotForTables(ctx, dsn, irbackup.SnapshotOptions{}, tables)
			return err
		},
	}
	for name, open := range openers {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := open(ctx); !errors.Is(err, ir.ErrVStreamSingleStreamMultiTable) {
				t.Fatalf("%s = %v; want the VSTREAM-SINGLE-STREAM-MULTI-TABLE refusal", name, err)
			}
		})
	}
}
