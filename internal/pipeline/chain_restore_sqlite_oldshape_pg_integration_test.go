//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-4 + GC-22 across the BACKUP wire: a chain whose FULL was written by an
// older binary from a SQLite source carries that binary's shape — a
// `BIGINT PRIMARY KEY` marked AutoIncrement, and the UNIQUE constraint's
// auto-index under its reserved `sqlite_autoindex_<t>_1` name — while an
// incremental taken by THIS binary carries the corrected shape. The
// incremental's schema delta is therefore spurious (nothing changed on the
// source), and a chain restore onto Postgres must replay it cleanly: drop
// the old-named index, create the generated-named one, keep every row, and
// leave a target that still refuses a duplicate email.
//
// The old-shape full is HAND-WRITTEN (an IR the old reader produced, fed
// through the real Backup pipeline via the recorder engine) — never derived
// from the new reader, which would make the gate self-referential
// (CLAUDE.md, the item-104 lesson). The new shape IS taken from the new
// reader over a real SQLite file with the same DDL, so the delta is the one
// a real upgrade produces.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "modernc.org/sqlite"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

const oldShapeSourceDDL = `CREATE TABLE accounts (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE)`

// oldShapeAccounts is what the pre-GC-4/GC-22 SQLite reader produced for
// oldShapeSourceDDL: every integer PK marked AutoIncrement, the UNIQUE's
// auto-index carried by its reserved name, no ConstraintBacked.
func oldShapeAccounts() *ir.Table {
	return &ir.Table{
		Name: "accounts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}, Nullable: true},
			{Name: "email", Type: ir.Text{Size: ir.TextLong}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
		Indexes: []*ir.Index{{
			Name: "sqlite_autoindex_accounts_1", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email"}},
		}},
	}
}

// newShapeAccounts reads oldShapeSourceDDL through THIS binary's reader.
func newShapeAccounts(t *testing.T) *ir.Table {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), oldShapeSourceDDL); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	sqliteEng, _ := engines.Get("sqlite")
	sr, err := sqliteEng.OpenSchemaReader(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tbl := findTable(schema, "accounts")
	if tbl == nil {
		t.Fatal("accounts missing from the new reader's output")
	}
	// The premise the delta rests on, asserted rather than assumed: the two
	// shapes differ exactly where GC-4 and GC-22 changed the reader.
	if iv := tbl.Columns[0].Type.(ir.Integer); iv.AutoIncrement {
		t.Fatal("new reader marked BIGINT PRIMARY KEY AutoIncrement; the delta this test replays would not exist")
	}
	if len(tbl.Indexes) != 1 || tbl.Indexes[0].Name != "accounts_email_key" || !tbl.Indexes[0].ConstraintBacked {
		t.Fatalf("new reader indexes = %+v; want accounts_email_key ConstraintBacked", tbl.Indexes)
	}
	if reflect.DeepEqual(tbl, oldShapeAccounts()) {
		t.Fatal("old and new shapes are equal; the fixture no longer models an upgrade")
	}
	return tbl
}

func TestChainRestore_OldShapeSQLiteFull_NewShapeIncremental_ReplaysOnPG(t *testing.T) {
	_, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	ctx := ctx2min(t)
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// The old binary's full: its IR, through the real backup pipeline.
	oldSchema := &ir.Schema{Tables: []*ir.Table{oldShapeAccounts()}}
	src := newBackupRecorderEngine("sqlite-trigger", oldSchema, map[string][]ir.Row{
		"accounts": {{"id": int64(1), "email": "a@example.com"}, {"id": int64(2), "email": "b@example.com"}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run (old-shape full): %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	full.Kind = irbackup.BackupKindFull
	full.SourceEngine = "sqlite-trigger"
	full.EndPosition = ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":100}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}

	// This binary's incremental. The writer's "before" baseline is the
	// PARENT manifest's recorded schema (the old shape above) and it reads
	// the source once, at window end — so handing it the new shape is what
	// makes it record the spurious AlterTable delta itself. One insert.
	newSchema := &ir.Schema{Tables: []*ir.Table{newShapeAccounts(t)}}
	cdc := &fakeCDCEngine{
		name:           "sqlite-trigger",
		schemaSequence: []*ir.Schema{newSchema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":101}`}},
			ir.Insert{
				Position: ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":102}`},
				Table:    "accounts",
				Row:      ir.Row{"id": int64(3), "email": "c@example.com"},
			},
			ir.TxCommit{Position: ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":103}`}},
		},
	}
	ib := &IncrementalBackup{Source: cdc, SourceDSN: "src", Store: store, ParentRef: full.BackupID, Window: 5 * time.Minute}
	if err := ib.Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	links, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || len(links[1].Manifest.SchemaDelta) == 0 {
		t.Fatalf("chain = %d links, incremental deltas = %v; want 2 links and a recorded AlterTable delta", len(links), links[1].Manifest.SchemaDelta)
	}

	pgEng, _ := engines.Get("postgres")
	if err := (&backup.ChainRestore{Target: pgEng, TargetDSN: pgTarget, Store: store}).Run(ctx); err != nil {
		t.Fatalf("ChainRestore.Run onto Postgres: %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM public.accounts`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("accounts rows = %d; want 3 (2 from the full + 1 from the incremental)", n)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.accounts (id, email) VALUES (9, 'a@example.com')`); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Errorf("target accepted a duplicate email after the replay (err=%v); uniqueness was lost", err)
	}
	var stale int
	if err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'accounts' AND indexname LIKE '%sqlite_autoindex%'`,
	).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Errorf("%d index(es) still carry the old reserved name after the delta replayed", stale)
	}
}
