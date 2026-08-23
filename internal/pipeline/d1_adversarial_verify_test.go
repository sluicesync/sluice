//go:build d1verify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Live-D1 adversarial end-to-end rounds: the int64 > 2^53 / worst-case
// value matrix driven through the three PRODUCT paths a real D1 source
// rides — migrate, backup→restore, and d1-trigger CDC — against a real
// Cloudflare D1 database (the reader-level matrix + keyset pins live in
// internal/engines/sqlite/d1_adversarial_verify_test.go).
//
// The target for the migrate/restore rounds is a LOCAL SQLite file, so
// the only cloud dependency is D1 itself, and the ground-truth probes
// read the file through the modernc driver — an independent reader from
// sluice's D1 HTTP path end to end.
//
// D1 as a migrate/backup TARGET is unconstructible BY DESIGN (the d1
// engine is a source only); that refusal is pinned loudly here rather
// than left implied (the sibling-enumeration discipline).
//
// Lifecycle: each test creates a throwaway D1 database via the REST API
// and deletes it on cleanup; skip-clean without credentials.
//
//	go test -tags=d1verify -v -count=1 -timeout=15m \
//	  -run 'TestD1Verify' ./internal/pipeline/...

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // independent probe reader for the target file

	"sluicesync.dev/sluice/internal/engines"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// ---- REST lifecycle helpers (test-side, independent of sluice's own
// d1Client — the transport under test) ----

const d1AdvAPIBase = "https://api.cloudflare.com/client/v4/accounts/"

func d1AdvCreds(t *testing.T) (account, token string) {
	t.Helper()
	token = os.Getenv("CLOUDFLARE_API_TOKEN")
	account = os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if token == "" || account == "" {
		t.Skip("CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID not set; d1verify needs live credentials")
	}
	return account, token
}

// d1AdvRequest is a minimal Cloudflare REST call (create/delete/query).
func d1AdvRequest(ctx context.Context, method, url, token, body string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, url, resp.StatusCode, out)
	}
	return out, nil
}

// d1AdvCreateThrowaway creates a uniquely-named throwaway D1 database
// and registers deletion + a post-delete listing check on cleanup, so a
// leaked database is a loud test failure, never a silent leftover.
func d1AdvCreateThrowaway(ctx context.Context, t *testing.T, account, token string) (dbID, name string) {
	t.Helper()
	base := d1AdvAPIBase + account + "/d1/database"
	name = fmt.Sprintf("sluice-d1adv-%d", time.Now().UnixNano())
	body, err := d1AdvRequest(ctx, http.MethodPost, base, token, `{"name":"`+name+`"}`)
	if err != nil {
		t.Fatalf("create throwaway D1 database: %v", err)
	}
	var created struct {
		Success bool `json:"success"`
		Result  struct {
			UUID string `json:"uuid"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &created); err != nil || !created.Success || created.Result.UUID == "" {
		t.Fatalf("create response not usable (err=%v): %s", err, body)
	}
	dbID = created.Result.UUID

	t.Cleanup(func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := d1AdvRequest(delCtx, http.MethodDelete, base+"/"+dbID, token, ""); err != nil {
			t.Errorf("FAILED to delete throwaway D1 database %s (name %s) — delete it manually: %v", dbID, name, err)
			return
		}
		// Confirm the deletion against the account's live list.
		listing, err := d1AdvRequest(delCtx, http.MethodGet, base+"?per_page=100", token, "")
		if err != nil {
			t.Logf("post-delete list failed (delete itself succeeded): %v", err)
			return
		}
		if bytes.Contains(listing, []byte(name)) {
			t.Errorf("throwaway D1 database %s still listed after delete — remove it manually", name)
		}
	})
	return dbID, name
}

// d1AdvQuery posts one SQL statement to the throwaway database via the
// raw REST API (independent of sluice's transport) and returns the
// first statement's result rows.
func d1AdvQuery(ctx context.Context, t *testing.T, account, dbID, token, sqlText string) []map[string]json.RawMessage {
	t.Helper()
	url := d1AdvAPIBase + account + "/d1/database/" + dbID + "/query"
	req, err := json.Marshal(map[string]string{"sql": sqlText})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	body, err := d1AdvRequest(ctx, http.MethodPost, url, token, string(req))
	if err != nil {
		t.Fatalf("query %q: %v", sqlText, err)
	}
	var env struct {
		Success bool `json:"success"`
		Result  []struct {
			Success bool                         `json:"success"`
			Results []map[string]json.RawMessage `json:"results"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &env); err != nil || !env.Success || len(env.Result) == 0 || !env.Result[0].Success {
		t.Fatalf("query %q response not usable (err=%v): %s", sqlText, err, body)
	}
	return env.Result[0].Results
}

// d1AdvSeedMatrix creates + seeds the adversarial table on the live D1
// database via SQL literals (server-side exact-digit parse — nothing on
// the way in rides a JSON number), then ground-truths the stored values
// server-side (hex/CAST) as the anti-vacuity floor.
func d1AdvSeedMatrix(ctx context.Context, t *testing.T, account, dbID, token string) {
	t.Helper()
	d1AdvQuery(ctx, t, account, dbID, token, `CREATE TABLE adv (
		id INTEGER PRIMARY KEY,
		i_53p1 INTEGER NOT NULL,
		i_max INTEGER NOT NULL,
		i_min INTEGER NOT NULL,
		i_snow INTEGER NOT NULL,
		r_17 REAL NOT NULL,
		t_emoji TEXT NOT NULL,
		t_empty TEXT NOT NULL,
		t_json TEXT NOT NULL,
		b_nul BLOB NOT NULL,
		n_dec NUMERIC NOT NULL,
		v_null TEXT
	)`)
	d1AdvQuery(ctx, t, account, dbID, token, `INSERT INTO adv VALUES
		(9007199254740993, 9007199254740993, 9223372036854775807, -9223372036854775808,
		 1837113234971131904, 0.1234567890123456789, 'crab 🦀 café ✅', '',
		 '{"n":9007199254740993}', x'00FF7F00', 19.99, NULL),
		(9007199254740995, 9007199254740994, 9223372036854775806, -9223372036854775807,
		 1837113234971131905, 0.30000000000000004, 'zwölf 🦞', 'x',
		 '{"n":9007199254740995}', x'DEADBEEF', 1234567.89, NULL)`)

	rows := d1AdvQuery(ctx, t, account, dbID, token,
		`SELECT CAST(id AS TEXT) AS id, CAST(i_53p1 AS TEXT) AS a, hex(b_nul) AS h FROM adv ORDER BY id LIMIT 1`)
	if len(rows) != 1 {
		t.Fatalf("ground-truth returned %d rows", len(rows))
	}
	if got := string(rows[0]["id"]); got != `"9007199254740993"` {
		t.Fatalf("server holds PK %s; the seed did not land exactly — nothing below measures sluice", got)
	}
	if got := string(rows[0]["a"]); got != `"9007199254740993"` {
		t.Fatalf("server holds i_53p1 %s; seed did not land exactly", got)
	}
	if got := string(rows[0]["h"]); got != `"00FF7F00"` {
		t.Fatalf("server holds b_nul %s; seed did not land exactly", got)
	}
}

// d1AdvCell is one target-file probe cell. This file is d1verify-tagged
// (not integration-tagged), so it deliberately does NOT reuse the
// integration corpus's advCell machinery — the probes here are a local,
// self-contained mirror of the same model.
type d1AdvCell struct {
	family string
	col    string
	probe  string // %s → column; run against the target SQLite file
	want   string // byte-exact expected text
}

// d1AdvTargetProbes are the per-cell ground-truth probes run on the
// LOCAL SQLite target file (modernc — an independent reader) after a
// migrate or restore from the live D1 source. Rows are keyed by the
// > 2^53 PRIMARY KEY itself, so the PK column is probed implicitly by
// every WHERE — and explicitly by the id cell.
func d1AdvTargetProbes() []d1AdvCell {
	return []d1AdvCell{
		{family: "integer", col: "id", probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#9007199254740993"},
		{family: "integer", col: "i_53p1", probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#9007199254740993"},
		{family: "integer", col: "i_max", probe: "CAST(%s AS TEXT)", want: "9223372036854775807"},
		{family: "integer", col: "i_min", probe: "CAST(%s AS TEXT)", want: "-9223372036854775808"},
		{family: "integer", col: "i_snow", probe: "CAST(%s AS TEXT)", want: "1837113234971131904"},
		// CAST probe: round-trip-exact on modernc's SQLite ≥ 3.43 (a
		// format('%.Ng') probe would itself be lossy — that renderer is
		// what the capture-expression fix replaced).
		{family: "float", col: "r_17", probe: "CAST(%s AS TEXT)", want: "0.12345678901234568"},
		{family: "text", col: "t_emoji", probe: "typeof(%s) || '#' || %s", want: "text#crab 🦀 café ✅"},
		{family: "text", col: "t_empty", probe: "typeof(%s) || '#' || length(%s)", want: "text#0"},
		{family: "text", col: "t_json", probe: "%s", want: `{"n":9007199254740993}`},
		{family: "binary", col: "b_nul", probe: "typeof(%s) || '#' || lower(hex(%s))", want: "blob#00ff7f00"},
		// NUMERIC → ir.Decimal{Unconstrained} → SQLite-target TEXT
		// affinity (Bug 162): the decimal string, byte-exact.
		{family: "decimal", col: "n_dec", probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "text#19.99"},
		{family: "null", col: "v_null", probe: "CASE WHEN %s IS NULL THEN 'null' ELSE 'NOT-NULL:' || CAST(%s AS TEXT) END", want: "null"},
	}
}

// d1AdvProbeTargetFile ground-truths the migrated/restored rows in the
// local SQLite file. Row 1 is keyed by the > 2^53 PK.
func d1AdvProbeTargetFile(t *testing.T, path string) {
	t.Helper()
	cells := d1AdvTargetProbes()
	// Anti-vacuity floor: a shrunken probe list fails loudly first.
	families := map[string]bool{}
	for _, c := range cells {
		families[c.family] = true
	}
	if len(families) < 6 || len(cells) < 12 {
		t.Fatalf("anti-vacuity floor: d1→sqlite probe list has %d families / %d cells; floor is 6 / 12",
			len(families), len(cells))
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open target file %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()
	d1AdvProbeCells(t, db, "adv", cells, 9007199254740993)

	// Row count: the independent expected value is 2 (the seed's own
	// row count), compared against the file, not against sluice.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM adv").Scan(&n); err != nil {
		t.Fatalf("count target rows: %v", err)
	}
	if n != 2 {
		t.Errorf("target holds %d rows; want 2", n)
	}
}

// d1AdvProbeCells runs each cell's probe against the target file, keyed
// by an int64 row id (the key being a > 2^53 integer is itself part of
// what is under test — the driver binds it exactly).
func d1AdvProbeCells(t *testing.T, db *sql.DB, table string, cells []d1AdvCell, id int64) {
	t.Helper()
	for _, c := range cells {
		c := c
		t.Run(c.family+"/"+c.col, func(t *testing.T) {
			expr := strings.ReplaceAll(c.probe, "%s", c.col)
			q := fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", expr, table)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var got sql.NullString
			if err := db.QueryRowContext(ctx, q, id).Scan(&got); err != nil {
				t.Fatalf("probe %s: %v\nquery: %s", c.col, err, q)
			}
			if !got.Valid {
				t.Fatalf("probe %s returned SQL NULL; want %q (value silently dropped?)", c.col, c.want)
			}
			if got.String != c.want {
				t.Errorf("SILENT VALUE ALTERATION on %s (family %s):\n  target holds: %q\n  want:         %q\n  probe: %s",
					c.col, c.family, got.String, c.want, expr)
			}
		})
	}
}

// TestD1Verify_AdversarialMigrate_D1ToSQLiteFile is the migrate round:
// live D1 source → local SQLite file target, every cell ground-truthed
// through the independent modernc reader.
func TestD1Verify_AdversarialMigrate_D1ToSQLiteFile(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dbID, _ := d1AdvCreateThrowaway(ctx, t, account, token)
	d1AdvSeedMatrix(ctx, t, account, dbID, token)

	d1Eng, ok := engines.Get("d1")
	if !ok {
		t.Fatal("d1 engine not registered")
	}
	sqliteEng, _ := engines.Get("sqlite")
	dst := filepath.Join(t.TempDir(), "d1adv.db")
	mig := &Migrator{
		Source: d1Eng, Target: sqliteEng,
		SourceDSN: "d1://" + account + "/" + dbID, TargetDSN: dst,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (live D1 adversarial matrix must migrate cleanly): %v", err)
	}
	d1AdvProbeTargetFile(t, dst)
}

// TestD1Verify_AdversarialBackupRestore_D1Source is the backup-codec
// round: full backup of the live D1 source (a serialization boundary of
// its own) → restore into a local SQLite file → the same independent
// probes. Also pins that D1 as a backup/restore TARGET refuses loudly
// (the by-design unconstructible cell, stated rather than implied).
func TestD1Verify_AdversarialBackupRestore_D1Source(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dbID, _ := d1AdvCreateThrowaway(ctx, t, account, token)
	d1AdvSeedMatrix(ctx, t, account, dbID, token)

	d1Eng, _ := engines.Get("d1")
	sqliteEng, _ := engines.Get("sqlite")
	srcDSN := "d1://" + account + "/" + dbID

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{
		Source: d1Eng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run (live D1 source): %v", err)
	}

	dst := filepath.Join(t.TempDir(), "d1adv-restore.db")
	if err := (&backup.Restore{
		Target: sqliteEng, TargetDSN: dst, Store: store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run (D1 backup → SQLite file): %v", err)
	}
	d1AdvProbeTargetFile(t, dst)

	t.Run("d1_as_restore_target_refuses_loudly", func(t *testing.T) {
		err := (&backup.Restore{
			Target: d1Eng, TargetDSN: srcDSN, Store: store,
		}).Run(ctx)
		if err == nil {
			t.Fatal("HEADLINE: restore INTO a D1 target exited clean — the d1 engine is a source only; " +
				"a clean exit means rows were silently dropped or a writer appeared without this pin being updated")
		}
		if !strings.Contains(err.Error(), "not implemented") {
			t.Errorf("restore into D1 refused, but without naming the source-only posture: %v", err)
		}
		t.Logf("restore into D1 refused loudly: %v", err)
	})
}

// TestD1Verify_AdversarialTriggerCDC_D1Source drives the adversarial
// values through the d1-trigger CDC lane: capture triggers on the live
// D1 database, then INSERT/UPDATE/DELETE with > 2^53 integers, blobs,
// and multi-byte text, asserting the EXACT decoded images out of the
// real CDC reader (capture → change-log → HTTP poll → reconstruct).
func TestD1Verify_AdversarialTriggerCDC_D1Source(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dbID, _ := d1AdvCreateThrowaway(ctx, t, account, token)
	dsn := "d1://" + account + "/" + dbID

	d1AdvQuery(ctx, t, account, dbID, token, `CREATE TABLE ev (
		id INTEGER PRIMARY KEY,
		big INTEGER NOT NULL,
		blb BLOB,
		note TEXT
	)`)
	if _, err := sqlitetrigger.SetupD1(ctx, dsn, sqlitetrigger.SetupOptions{Tables: []string{"ev"}}); err != nil {
		t.Fatalf("SetupD1: %v", err)
	}

	reader, err := sqlitetrigger.OpenD1CDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenD1CDCReader: %v", err)
	}
	if closer, ok := reader.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}

	// Anchor "from now", then produce the three ops (each POST is its
	// own committed transaction).
	ch, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	d1AdvQuery(ctx, t, account, dbID, token,
		`INSERT INTO ev VALUES (9007199254740993, 9223372036854775807, x'00FF7F00', 'crab 🦀')`)
	d1AdvQuery(ctx, t, account, dbID, token,
		`UPDATE ev SET big = 9007199254740995, note = 'zwölf' WHERE id = 9007199254740993`)
	d1AdvQuery(ctx, t, account, dbID, token,
		`DELETE FROM ev WHERE id = 9007199254740993`)

	var got []ir.Change
	timeout := time.After(90 * time.Second)
collect:
	for len(got) < 3 {
		select {
		case c, ok := <-ch:
			if !ok {
				break collect
			}
			got = append(got, c)
		case <-timeout:
			break collect
		}
	}
	if cerr := reader.(interface{ Err() error }).Err(); cerr != nil {
		t.Fatalf("CDC reader error: %v", cerr)
	}
	if len(got) != 3 {
		t.Fatalf("collected %d changes; want 3 (insert/update/delete)", len(got))
	}

	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change 0 is %T; want ir.Insert", got[0])
	}
	if v, _ := ins.Row["id"].(int64); v != 9007199254740993 {
		t.Errorf("SILENT INT ALTERATION: insert image id = %v (%T); want 9007199254740993", ins.Row["id"], ins.Row["id"])
	}
	if v, _ := ins.Row["big"].(int64); v != 9223372036854775807 {
		t.Errorf("insert image big = %v; want int64 max", ins.Row["big"])
	}
	if b, _ := ins.Row["blb"].([]byte); !bytes.Equal(b, []byte{0x00, 0xFF, 0x7F, 0x00}) {
		t.Errorf("insert image blb = %x; want 00ff7f00", ins.Row["blb"])
	}
	if s, _ := ins.Row["note"].(string); s != "crab 🦀" {
		t.Errorf("insert image note = %q", ins.Row["note"])
	}

	upd, ok := got[1].(ir.Update)
	if !ok {
		t.Fatalf("change 1 is %T; want ir.Update", got[1])
	}
	if v, _ := upd.Before["big"].(int64); v != 9223372036854775807 {
		t.Errorf("update BEFORE image big = %v; want int64 max (the before-image path is its own decode)", upd.Before["big"])
	}
	if v, _ := upd.After["big"].(int64); v != 9007199254740995 {
		t.Errorf("SILENT INT ALTERATION: update AFTER image big = %v; want 9007199254740995 "+
			"(a JSON-number capture lands …996)", upd.After["big"])
	}
	if s, _ := upd.After["note"].(string); s != "zwölf" {
		t.Errorf("update after note = %q", upd.After["note"])
	}

	del, ok := got[2].(ir.Delete)
	if !ok {
		t.Fatalf("change 2 is %T; want ir.Delete", got[2])
	}
	if v, _ := del.Before["id"].(int64); v != 9007199254740993 {
		t.Errorf("delete before-image id = %v; want the exact > 2^53 key (an altered key deletes the WRONG row downstream)", del.Before["id"])
	}

	if _, err := sqlitetrigger.TeardownD1(ctx, dsn, sqlitetrigger.TeardownOptions{}); err != nil {
		t.Logf("TeardownD1 (database is deleted on cleanup anyway): %v", err)
	}
}
