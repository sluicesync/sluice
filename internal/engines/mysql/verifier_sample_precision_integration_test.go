//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Two ways a MySQL sample hash could read different values as equal that
// the family matrix's ordinary-sized values cannot reach.
//
//   - A FLOAT KEY. The key string is what pairs a source row with its
//     target row; rendered through CAST(… AS CHAR) at 6 significant
//     digits, keys 1234567 and 1234568 both print as 1234570.
//   - A value bigger than max_allowed_packet. MySQL's CONCAT returns NULL
//     (warning 1301, nothing louder) when its result would exceed it, and
//     CONCAT_WS skips a NULL argument — so a length-prefixed text token
//     for a large value silently dropped out of the row hash, and two
//     different LONGTEXT values hashed equal. Each value is now digested
//     on its own (fixed width). The row is also joined with CONCAT, which
//     a NULL token nulls, so a token that still went NULL would refuse
//     the table naming the row instead of shortening the hash. With
//     fixed-width tokens no real row reaches that backstop (a row's join
//     can only outgrow max_allowed_packet once the query's own text,
//     about five times longer per column, has — and the server refuses
//     that query outright, error 1153); it was seen to fire, naming
//     "row 1", when the per-value digest was mutated back to a
//     length-prefixed text token.
//
// Independent expected value: both fixtures are written here with values
// known to differ.

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// openSampleVerifier opens the vanilla engine's schema reader on dsn and
// returns its tables by name.
func openSampleVerifier(t *testing.T, ctx context.Context, dsn string) (ir.SampleVerifier, map[string]*ir.Table) {
	t.Helper()
	rdr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tables := map[string]*ir.Table{}
	for _, tb := range schema.Tables {
		tables[tb.Name] = tb
	}
	return rdr.(ir.SampleVerifier), tables
}

func TestSampleRowHashes_FloatKeyRendersExactly(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vsfkey")
	defer cleanup()
	applyMySQL(t, dsn, "CREATE TABLE kf (id FLOAT PRIMARY KEY, v TEXT); INSERT INTO kf VALUES (1234567, 'a'), (1234568, 'b');")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sv, tables := openSampleVerifier(t, ctx, dsn)
	rows, err := sv.SampleRowHashes(ctx, tables["kf"], ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"id", "v"}}, 10, 1, ir.HashMD5)
	if err != nil {
		t.Fatalf("SampleRowHashes: %v", err)
	}
	if len(rows) != 2 || rows[0].PrimaryKey != "1234567" || rows[1].PrimaryKey != "1234568" {
		t.Fatalf("keys = %+v; want 1234567 and 1234568, each exact", rows)
	}
}

func TestSampleRowHashes_ValueLargerThanMaxAllowedPacket(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vsbig")
	defer cleanup()
	applyMySQL(t, dsn, `CREATE TABLE la (id BIGINT PRIMARY KEY, t LONGTEXT);
		CREATE TABLE lb (id BIGINT PRIMARY KEY, t LONGTEXT);
		INSERT INTO la VALUES (1, REPEAT('a', 10000));
		INSERT INTO lb VALUES (1, CONCAT(REPEAT('a', 9999), 'b'));
		CREATE TABLE ja (id BIGINT PRIMARY KEY, j JSON);
		CREATE TABLE jb (id BIGINT PRIMARY KEY, j JSON);
		INSERT INTO ja VALUES (1, JSON_ARRAY(REPEAT('a', 10000)));
		INSERT INTO jb VALUES (1, JSON_ARRAY(CONCAT(REPEAT('a', 9999), 'b')));`)

	// Lower the server's packet limit below the values, for connections
	// opened from here on (the reader below opens fresh ones), and put it
	// back however the test ends — the container is shared.
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = admin.Close() }()
	var original int64
	if err := admin.QueryRow("SELECT @@GLOBAL.max_allowed_packet").Scan(&original); err != nil {
		t.Fatalf("read max_allowed_packet: %v", err)
	}
	if _, err := admin.Exec("SET GLOBAL max_allowed_packet = 4096"); err != nil {
		t.Fatalf("lower max_allowed_packet: %v", err)
	}
	defer func() {
		if _, err := admin.Exec("SET GLOBAL max_allowed_packet = ?", original); err != nil {
			t.Errorf("restore max_allowed_packet: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sv, tables := openSampleVerifier(t, ctx, dsn)
	plan := ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"id", "t"}}
	for _, algo := range []ir.HashAlgorithm{ir.HashMD5, ir.HashSHA256} {
		// A large VALUE is digested on its own, so it must hash — and differ.
		a, errA := sv.SampleRowHashes(ctx, tables["la"], plan, 10, 1, algo)
		b, errB := sv.SampleRowHashes(ctx, tables["lb"], plan, 10, 1, algo)
		if errA != nil || errB != nil {
			t.Fatalf("%s: a value larger than max_allowed_packet failed the table: %v / %v", algo, errA, errB)
		}
		if len(a) != 1 || len(b) != 1 {
			t.Fatalf("%s: sampled %d/%d rows; want 1 each", algo, len(a), len(b))
		}
		if a[0].Hash == b[0].Hash {
			t.Errorf("%s: two different LONGTEXT values larger than max_allowed_packet hash EQUAL — false clean", algo)
		}

		// A large JSON document is the one value MySQL 8 does not render
		// as NULL past the packet: CAST(j AS CHAR) comes back as the EMPTY
		// string (warning 1301), so its digest is the same for every
		// document. It must refuse, naming the row — never match.
		jplan := ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"id", "j"}}
		ja, errJA := sv.SampleRowHashes(ctx, tables["ja"], jplan, 10, 1, algo)
		jb, errJB := sv.SampleRowHashes(ctx, tables["jb"], jplan, 10, 1, algo)
		for name, err := range map[string]error{"ja": errJA, "jb": errJB} {
			if err == nil || !strings.Contains(err.Error(), "row 1:") {
				t.Errorf("%s: %s: a JSON document larger than max_allowed_packet did not refuse naming the row: %v", algo, name, err)
			} else if !strings.Contains(err.Error(), "Verify this table with a larger max_allowed_packet") ||
				!strings.Contains(err.Error(), "--include-table "+name) {
				t.Errorf("%s: %s: the refusal does not name its remedy (a larger max_allowed_packet for this table): %v", algo, name, err)
			}
		}
		if errJA == nil && errJB == nil && len(ja) == 1 && len(jb) == 1 && ja[0].Hash == jb[0].Hash {
			t.Errorf("%s: two different JSON documents larger than max_allowed_packet hash EQUAL — false clean", algo)
		}
	}

	// The remedy the refusal names must work: with the packet limit raised
	// above the documents, a fresh verifier hashes both JSON tables, and
	// differently.
	if _, err := admin.Exec("SET GLOBAL max_allowed_packet = ?", original); err != nil {
		t.Fatalf("raise max_allowed_packet: %v", err)
	}
	raised, rtables := openSampleVerifier(t, ctx, dsn)
	jplan := ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"id", "j"}}
	ja, errJA := raised.SampleRowHashes(ctx, rtables["ja"], jplan, 10, 1, ir.HashMD5)
	jb, errJB := raised.SampleRowHashes(ctx, rtables["jb"], jplan, 10, 1, ir.HashMD5)
	if errJA != nil || errJB != nil {
		t.Fatalf("with max_allowed_packet raised to %d the JSON tables still refuse: %v / %v", original, errJA, errJB)
	}
	if len(ja) != 1 || len(jb) != 1 || ja[0].Hash == jb[0].Hash {
		t.Errorf("with max_allowed_packet raised, two different JSON documents = %+v / %+v; want one row each, hashing differently", ja, jb)
	}
}
