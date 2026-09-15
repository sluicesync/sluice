//go:build integration || nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
)

// # The backup/restore core the Neki arms drive, and the vanilla-Postgres
// # run that proves its plumbing before a paid one
//
// Two Tier-2 arms need this: `restore INTO a sharded Neki` and `backup FROM a
// sharded Neki`. Both are expensive — a provisioned cluster, minutes of wall
// clock, a real bill — and neither can be run on this machine. So everything
// that is NOT about the router lives here, under `integration || nekiverify`,
// and is exercised on every PR against an ordinary PostgreSQL container with
// [forceNekiFlavor] on ([TestPostgresSuite_NekiBackupRestorePlumbing]).
//
// That split is the point. When the live arm fails, the question that decides
// whether the failure is a PLATFORM finding or a bug in this test is "did the
// same code path work against vanilla PostgreSQL an hour ago?" — and the
// per-PR run answers it for free. The suite's history is six MoveTables
// dispatches that each failed at a different point; every one of those was a
// harness question answered at the cost of a cluster.
//
// # Why the backup SOURCE for the restore arm is SQLite
//
// The restore arm needs a backup to restore. Taking it from the Neki fixture
// itself would make the backup side Neki too, and then a failure has two
// candidate causes and the arm cannot say which — the same confound the
// coverage doc records for the CDC arm's router-side premise. The runner also
// cannot be assumed to have a second engine available inside the nekiverify
// job. SQLite is in-process, pure Go, already a first-class sluice source, and
// completely uninvolved in anything Neki does: a backup taken from it is a
// pure artifact, and everything that happens afterwards is the target's.
//
// The source database is built through the IR (schema writer + row writer)
// rather than through SQL text, so the shape the arm claims to restore — the
// shard key inside the PRIMARY KEY, one secondary index, a NULL — is stated
// once, in the IR, and cannot drift from what the backup actually carries.
//
// # The independent expected value (the 2026-08-01 rule), named
//
// The restore core's expected value is the row set this test GENERATED, folded
// in Go and digested before anything is written anywhere. It is not a count
// the target reports about itself, and not a re-read of the manifest. The
// backup core's expected value is an ordered SELECT of the SOURCE, digested in
// Go, compared against rows read out of the backup by restoring it into a
// LOCAL SQLite file — a target that shares no code with the Neki read path
// under test.
//
// `backup verify --depth read` is run too, and is deliberately NOT the content
// evidence: it streams each chunk through the same chunk reader restore uses,
// so it is internally consistent with the artifact by construction. It answers
// a different and narrower question — "is every chunk byte-intact and
// decodable, and does its decoded row count match what the manifest recorded"
// — and is reported as such.

// nekiBackupRestoreSpec parameterises the shared cores. The same values are
// used by the live arms and by the vanilla-Postgres proving run, so the
// per-PR run exercises the real shape rather than a miniature of it.
type nekiBackupRestoreSpec struct {
	// table is the wide table restored into the target; tableSmall is its
	// sibling. TWO tables on purpose: restore's cross-table pool is one of
	// the two axes whose PRODUCT the Neki copy ceiling bounds, and with a
	// single table the ceiling assertion would be vacuous.
	table      string
	tableSmall string

	// rows is the wide table's row count. >= 20k so the copy is more than
	// one chunk and more than one COPY, which is the condition under which
	// the router's concurrent-COPY admission is actually tested.
	rows int

	// smallRows is the sibling's row count.
	smallRows int

	// chunkRows rolls the chunk files. rows/chunkRows is the chunk count,
	// and restore's within-table axis only engages at >= 2 chunks.
	chunkRows int

	// tenants are the two shard-key values the generated rows alternate
	// between. On the live fixture these come from tenantsOnDistinctShards,
	// so the generated corpus is guaranteed to span both shards; on vanilla
	// PostgreSQL they are arbitrary and route nowhere.
	tenants [2]int64

	// secondaryIndex adds a non-key index to each generated table, so the
	// restore's INDEX phase has something to build.
	//
	// It is true for a live Neki and MUST be false on a forced-flavor vanilla
	// PostgreSQL, and the reason is the sharpest limit of flavor-forcing this
	// suite has found: a Neki target does not run `CREATE INDEX` at all. It
	// submits an ONLINE DDL workflow through the `__neki` schema
	// ([neki_online_ddl.go]), because the router caps DDL at 30 s (ADR-0184).
	// A server with no `__neki` schema answers SQLSTATE 3F000, measured
	// 2026-09-14:
	//
	//	restore: create indexes: postgres: neki online DDL: submit index
	//	"nk_restored_payload_idx" on "nk_restored": ERROR: schema "__neki"
	//	does not exist (SQLSTATE 3F000)
	//
	// So the index-build leg of a Neki restore is NOT COVERED by the per-PR
	// run, and saying so here is the point: it is the one phase of the five
	// whose per-PR proof does not exist, which makes it the phase most worth
	// reading carefully when the live arm reports.
	secondaryIndex bool
}

// defaultNekiBackupRestoreSpec is the shape both runs use.
func defaultNekiBackupRestoreSpec(tenantA, tenantB int64) nekiBackupRestoreSpec {
	return nekiBackupRestoreSpec{
		table:          "nk_restored",
		tableSmall:     "nk_restored_small",
		rows:           20_000,
		smallRows:      500,
		chunkRows:      4_000,
		tenants:        [2]int64{tenantA, tenantB},
		secondaryIndex: true,
	}
}

// nekiTriple is one generated/read row. The payload is nullable because a
// NULL that arrives as an empty string is exactly the kind of silent
// normalisation this suite exists to refuse, and a digest that cannot tell
// them apart would pass through it.
type nekiTriple struct {
	payload sql.NullString
	tenant  int64
	id      int64
}

// nekiDigestTriples folds an ordered row set into one digest.
//
// NULL and the literal string "NULL" must not collide, so a present value is
// prefixed `S:` and an absent one is the bare token — no value can render as
// both.
func nekiDigestTriples(rows []nekiTriple) string {
	h := sha256.New()
	for _, r := range rows {
		if r.payload.Valid {
			fmt.Fprintf(h, "%d|%d|S:%s\n", r.tenant, r.id, r.payload.String)
			continue
		}
		fmt.Fprintf(h, "%d|%d|NULL\n", r.tenant, r.id)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// nekiReadTriples reads an ordered (tenant, id, payload) projection from any
// engine that speaks `database/sql`. The query supplies the ordering; this
// never sorts, so a target returning rows out of order fails the digest
// rather than being quietly repaired.
func nekiReadTriples(ctx context.Context, db *sql.DB, query string) ([]nekiTriple, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []nekiTriple
	for rows.Next() {
		var r nekiTriple
		if err := rows.Scan(&r.tenant, &r.id, &r.payload); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// nekiGenerateRows builds the corpus for one table: alternating tenants so
// both shards are reached, ascending ids, and exactly one NULL payload.
func nekiGenerateRows(spec nekiBackupRestoreSpec, n int, prefix string) []nekiTriple {
	out := make([]nekiTriple, 0, n)
	for i := range n {
		r := nekiTriple{
			tenant: spec.tenants[i%2],
			id:     int64(i) + 1,
		}
		// One NULL, at a fixed position, in the middle of the corpus so it
		// lands inside a chunk rather than on a boundary.
		if i != n/2 {
			r.payload = sql.NullString{String: fmt.Sprintf("%s-%07d-%s", prefix, i, strings.Repeat("x", 16)), Valid: true}
		}
		out = append(out, r)
	}
	return out
}

// nekiSortTriples orders a generated corpus the way every read-back query
// orders it: by (tenant_id, id).
//
// Load-bearing, and it cost a run to learn: [nekiGenerateRows] ALTERNATES
// tenants so both shards are reached, so its natural order is
// (A,1),(B,2),(A,3),… while `ORDER BY tenant_id, id` returns all of A then
// all of B. Digesting the unsorted model against the sorted read reported an
// ordered-content MISMATCH on a target holding exactly the right rows — the
// worst kind of harness fault, because on a live cluster it reads as a
// routing defect.
func nekiSortTriples(rows []nekiTriple) []nekiTriple {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b nekiTriple) int {
		if a.tenant != b.tenant {
			return cmp.Compare(a.tenant, b.tenant)
		}
		return cmp.Compare(a.id, b.id)
	})
	return out
}

// nekiSourceTable renders the IR for one generated table: the shard key
// INSIDE the primary key (the shape a sharded target accepts), optionally
// plus one secondary index so the restore's index phase has something to
// build — which on Neki is the online-DDL workflow ADR-0184 describes. See
// [nekiBackupRestoreSpec.secondaryIndex] for why that is optional.
func nekiSourceTable(name string, secondaryIndex bool) *ir.Table {
	tbl := &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "tenant_id", Type: ir.Integer{Width: 64}},
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "payload", Type: ir.Text{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{
			Name:    name + "_pkey",
			Columns: []ir.IndexColumn{{Column: "tenant_id"}, {Column: "id"}},
		},
	}
	if secondaryIndex {
		tbl.Indexes = []*ir.Index{{
			Name:    name + "_payload_idx",
			Columns: []ir.IndexColumn{{Column: "payload"}},
		}}
	}
	return tbl
}

// nekiBuildSQLiteSource writes the generated corpus into a fresh SQLite file
// through sluice's own SQLite engine and returns its DSN.
//
// Through the engine rather than through `sql.Exec` text: the backup reads
// this file back with the same engine, so building it any other way would
// leave the arm free to pass while the IR round trip was broken.
func nekiBuildSQLiteSource(ctx context.Context, t *testing.T, spec nekiBackupRestoreSpec, wide, small []nekiTriple) string {
	t.Helper()

	dsn := filepath.Join(t.TempDir(), "neki-backup-source.db")
	schema := &ir.Schema{Tables: []*ir.Table{
		nekiSourceTable(spec.table, spec.secondaryIndex),
		nekiSourceTable(spec.tableSmall, spec.secondaryIndex),
	}}

	sw, err := sqlite.Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("neki backup source: open SQLite schema writer: %v", err)
	}
	defer migcore.CloseIf(sw)
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("neki backup source: create SQLite tables: %v", err)
	}

	rw, err := sqlite.Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("neki backup source: open SQLite row writer: %v", err)
	}
	defer migcore.CloseIf(rw)
	for i, tbl := range schema.Tables {
		corpus := wide
		if i == 1 {
			corpus = small
		}
		ch := make(chan ir.Row, 256)
		go func(corpus []nekiTriple) {
			defer close(ch)
			for _, r := range corpus {
				row := ir.Row{"tenant_id": r.tenant, "id": r.id, "payload": nil}
				if r.payload.Valid {
					row["payload"] = r.payload.String
				}
				ch <- row
			}
		}(corpus)
		if err := rw.WriteRows(ctx, tbl, ch); err != nil {
			t.Fatalf("neki backup source: write %s: %v", tbl.Name, err)
		}
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("neki backup source: create SQLite indexes: %v", err)
	}
	return dsn
}

// nekiPhaseRecorder records which restore/backup phase was in flight when a
// run failed.
//
// STRUCTURAL rather than inferred: the orchestrator's own [progress.Sink]
// brackets its phases, so "what was running" is read off the run instead of
// guessed from the error text. The fine-grained guess in
// [nekiClassifyFailurePhase] supplements this; it does not replace it.
type nekiPhaseRecorder struct {
	progress.Nop
	mu        sync.Mutex
	started   []string
	completed map[string]bool
}

func newNekiPhaseRecorder() *nekiPhaseRecorder {
	return &nekiPhaseRecorder{completed: map[string]bool{}}
}

func (r *nekiPhaseRecorder) PhaseStarted(p progress.Phase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, p.Key)
}

func (r *nekiPhaseRecorder) PhaseCompleted(p progress.Phase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed[p.Key] = true
}

// inFlight names the last phase that started and never completed, plus the
// whole ordered trace — a failure report that names only the last phase makes
// "it never got that far" indistinguishable from "it failed there".
func (r *nekiPhaseRecorder) inFlight() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := "(no phase started — the run failed before the orchestrator's first phase bracket)"
	for i := len(r.started) - 1; i >= 0; i-- {
		if !r.completed[r.started[i]] {
			last = r.started[i]
			break
		}
	}
	return fmt.Sprintf("%s (trace: %s)", last, strings.Join(r.started, " → "))
}

// nekiRestorePhaseMarkers maps a substring of the orchestrator's own error
// wrapping onto the phase name an operator would recognise. First match wins,
// so the list is ordered most-specific-first.
//
// The coarse [progress.Sink] phases are only three (schema/data/constraints);
// this is what distinguishes "create indexes" from "create constraints" inside
// the third, and "open the writer" from "create tables" inside the first.
var nekiRestorePhaseMarkers = []struct {
	contains string
	phase    string
}{
	{"open source schema reader", "CONNECT (source schema reader)"},
	{"open source row reader", "CONNECT (source row reader)"},
	{"open target schema writer", "CONNECT (target schema writer)"},
	{"open target row writer", "CONNECT (target row writer)"},
	{"open change applier", "CONNECT (change applier)"},
	{"ensure control table", "CONTROL-TABLE (the ADR-0187 placement door)"},
	{"create tables", "SCHEMA-APPLY (create tables)"},
	{"sync identity sequences", "SCHEMA-APPLY (sync identity sequences)"},
	{"create indexes", "INDEX (create indexes)"},
	{"create constraints", "CONSTRAINT (create constraints)"},
	{"create views", "VIEWS (create views)"},
	{"bulk-copy", "COPY (bulk-copy)"},
	{"bulk copy", "COPY (bulk-copy)"},
	{"restore table", "COPY (restore table)"},
	{"backup table", "COPY (backup table)"},
	{"read source schema", "SCHEMA-READ (read source schema)"},
	{"end position", "SNAPSHOT (end position)"},
	{"snapshot", "SNAPSHOT"},
}

func nekiClassifyFailurePhase(err error) string {
	msg := strings.ToLower(err.Error())
	for _, m := range nekiRestorePhaseMarkers {
		if strings.Contains(msg, m.contains) {
			return m.phase
		}
	}
	return "UNCLASSIFIED (no phase marker matched; the raw error below is the whole evidence)"
}

// nekiCataloguedHazards maps the SQLSTATEs this week's Neki work catalogued
// onto what an operator reading a weekly failure needs to know. A code that
// is NOT here is reported as unrecognised rather than guessed at — a wrong
// attribution in an unattended weekly is worse than none.
var nekiCataloguedHazards = map[string]string{
	"NK013": "the router refused the statement's SHAPE. Catalogued shapes: `COPY (SELECT …) TO` (which is why the " +
		"row reader declines the raw-copy lane), a shard key named in an UPDATE SET list, and a sequence read as a " +
		"relation. See docs/dev/neki-readiness.md.",
	"NK306": "shard-key column missing from the statement. On a CONTROL table this is the exact class ADR-0187 " +
		"closes by placing them in the authoritative shard group — so seeing it here means the placement did not " +
		"happen on this door. On a DATA table it means the table is routed by a shard index and the statement did " +
		"not carry the routing column.",
	"NK213": "the table is BLOCKED by a MoveTables/reshard workflow (sluice classifies this as " +
		"SLUICE-E-TARGET-TABLE-BLOCKED).",
	"NK205": "no healthy sidecar PRIMARY for a shard — the shard exists and cannot take writes. The fixture waits " +
		"this out at provisioning time (readShardUIDs/has_writable_primary); seeing it mid-run means a shard lost " +
		"its primary DURING the arm.",
	"53300": "too many connections. This is the concurrent-COPY ceiling: sluice paces a Neki target to " +
		"nekiConcurrentCopyLimit concurrent COPYs and the platform admits more than that (a floor of 12 was " +
		"measured, never a refusal) — so a 53300 HERE means either the pacing did not reach this door or the " +
		"platform's admission dropped.",
	"57014": "statement timeout. Neki caps DDL at 30 s, which is why ADR-0184 splits an index build into one ALTER " +
		"per index; a 57014 in the INDEX phase is that wall being met by an unsplit build.",
	"42501": "insufficient privilege — the role this run minted lacks something. See mintDSN: the fixture's role " +
		"inherits `postgres` precisely so it carries neki_viewer/neki_operator.",
	"42883": "function does not exist. Almost always overload resolution rather than absence (a PostgreSQL " +
		"function's identity is name + argument TYPES); use nekiFunctionSignature to print what the router registers.",
	"42704": "undefined object — on this platform most often a shard uid the ROUTER has not seen yet even though " +
		"the control plane reports it ready (see waitShardsVisibleToRouter).",
}

// nekiExplainFailure renders a backup/restore failure so an unattended weekly
// answers its own "why".
//
// The suite deletes its database at the end of every run, so anything not
// printed while the cluster existed cannot be recovered: a failure that says
// only "restore failed" costs a full dispatch to re-learn. Phase, SQLSTATE,
// every pgconn field the server populated, and the catalogued hazard all go
// in.
func nekiExplainFailure(op string, rec *nekiPhaseRecorder, err error) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s FAILED\n", op)
	fmt.Fprintf(&b, "  phase (from the orchestrator's own progress sink): %s\n", rec.inFlight())
	fmt.Fprintf(&b, "  phase (from the error's wrapping):                 %s\n", nekiClassifyFailurePhase(err))

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		fmt.Fprintf(&b, "  SQLSTATE: %s\n", pgErr.Code)
		fmt.Fprintf(&b, "  server:   severity=%q message=%q\n", pgErr.Severity, pgErr.Message)
		if pgErr.Detail != "" {
			fmt.Fprintf(&b, "            detail=%q\n", pgErr.Detail)
		}
		if pgErr.Hint != "" {
			fmt.Fprintf(&b, "            hint=%q\n", pgErr.Hint)
		}
		if pgErr.SchemaName != "" || pgErr.TableName != "" || pgErr.ColumnName != "" || pgErr.ConstraintName != "" {
			fmt.Fprintf(&b, "            schema=%q table=%q column=%q constraint=%q\n",
				pgErr.SchemaName, pgErr.TableName, pgErr.ColumnName, pgErr.ConstraintName)
		}
		if pgErr.Where != "" {
			fmt.Fprintf(&b, "            where=%q\n", pgErr.Where)
		}
		if note, ok := nekiCataloguedHazards[pgErr.Code]; ok {
			fmt.Fprintf(&b, "  CATALOGUED HAZARD %s: %s\n", pgErr.Code, note)
		} else {
			fmt.Fprintf(&b, "  SQLSTATE %s is NOT in this suite's catalogue of Neki hazards — treat it as a new "+
				"finding rather than a known wall, and add it to nekiCataloguedHazards once diagnosed.\n", pgErr.Code)
		}
	} else {
		b.WriteString("  (no *pgconn.PgError in the chain — the failure did not come back from the server, so it " +
			"is sluice-side or transport-side)\n")
	}
	fmt.Fprintf(&b, "  raw error: %v", err)
	return b.String()
}

// nekiStoreBytes sums the bytes a local store holds. The manifest records row
// counts and chunk files but not sizes, and "how big was it" is the first
// thing an operator asks of a backup.
func nekiStoreBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	if err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	}); err != nil {
		t.Logf("(could not size the backup store at %s: %v)", root, err)
		return -1
	}
	return total
}

// nekiRestoreCoreResult is what the restore core hands back so a caller can
// add target-specific assertions (a per-shard census, a topology read) without
// re-deriving any of this.
type nekiRestoreCoreResult struct {
	manifest   *irbackup.Manifest
	storeRoot  string
	wantDigest string
	gotDigest  string
	wantRows   int
	gotRows    int
	// tableParallelism is what restore's cross-table pool RESOLVED to,
	// captured through the package's own dispatch observer. On a Neki target
	// this is the number the copy ceiling is supposed to bound.
	tableParallelism int
	dispatchReason   string
}

// nekiRestoreCoreIntoTarget takes a full backup from a freshly-built SQLite
// source and restores it into targetDSN through the real `sluice restore`
// door, then grades the target's CONTENT against the rows this function
// generated.
//
// Concurrency is left at the DEFAULT (0 = auto) on purpose. The whole reason
// this arm exists is a comment in restore_table_pool.go recording that a
// literal 0 passed to the axis resolver dropped CopyConcurrencyCeiling and let
// a restore open table × chunk concurrent COPYs against a router that admits
// four. Pinning the axes here would route around the exact defect class.
func nekiRestoreCoreIntoTarget(
	ctx context.Context,
	t *testing.T,
	targetDSN string,
	spec nekiBackupRestoreSpec,
) *nekiRestoreCoreResult {
	t.Helper()

	wide := nekiGenerateRows(spec, spec.rows, "w")
	small := nekiGenerateRows(spec, spec.smallRows, "s")
	srcDSN := nekiBuildSQLiteSource(ctx, t, spec, wide, small)

	// The source is written in generation order and READ BACK in
	// (tenant_id, id) order, so the expected sets are sorted to match. See
	// [nekiSortTriples] — this is not a tidy-up.
	wide, small = nekiSortTriples(wide), nekiSortTriples(small)

	storeRoot := t.TempDir()
	store, err := blobcodec.NewLocalStore(storeRoot)
	if err != nil {
		t.Fatalf("neki restore core: NewLocalStore: %v", err)
	}

	backupRec := newNekiPhaseRecorder()
	if err := (&backup.Backup{
		Source:    sqlite.Engine{},
		SourceDSN: srcDSN,
		Store:     store,
		ChunkRows: spec.chunkRows,
		Progress:  backupRec,
	}).Run(ctx); err != nil {
		t.Fatalf("%s\n\nThis is the SQLITE-side backup, not the target — a failure here is a harness fault, not a "+
			"Neki finding.", nekiExplainFailure("neki restore core: backup FROM the local SQLite source", backupRec, err))
	}

	manifest, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("neki restore core: read the manifest the backup just wrote: %v", err)
	}
	// Anti-vacuity for the whole arm: the copy must be more than one chunk,
	// or the concurrency the router is being asked about never happens.
	for _, entry := range manifest.Tables {
		if entry.Name != spec.table {
			continue
		}
		if len(entry.Chunks) < 2 {
			t.Fatalf("neki restore core: %s was backed up as %d chunk(s) from %d rows at ChunkRows=%d — the "+
				"restore would be a single COPY and the concurrent-COPY behaviour this arm exists to exercise "+
				"would never occur", spec.table, len(entry.Chunks), spec.rows, spec.chunkRows)
		}
	}

	// The cross-table dispatch seam, so the arm reports the resolved axis
	// rather than inferring it from timing.
	var (
		dispatchMu     sync.Mutex
		dispatchN      int
		dispatchReason string
	)
	backup.RestoreDispatchObserver = func(n int, reason string) {
		dispatchMu.Lock()
		defer dispatchMu.Unlock()
		dispatchN, dispatchReason = n, reason
	}
	t.Cleanup(func() { backup.RestoreDispatchObserver = nil })

	restoreRec := newNekiPhaseRecorder()
	if err := (&backup.Restore{
		Target:    Engine{},
		TargetDSN: targetDSN,
		Store:     store,
		Progress:  restoreRec,
	}).Run(ctx); err != nil {
		t.Fatalf("%s", nekiExplainFailure("neki restore core: restore INTO the target", restoreRec, err))
	}

	dispatchMu.Lock()
	res := &nekiRestoreCoreResult{
		manifest:         manifest,
		storeRoot:        storeRoot,
		tableParallelism: dispatchN,
		dispatchReason:   dispatchReason,
	}
	dispatchMu.Unlock()

	res.wantDigest, res.wantRows = nekiDigestTriples(wide), len(wide)

	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("neki restore core: open the target for read-back: %v", err)
	}
	defer func() { _ = db.Close() }()

	got, err := nekiReadTriples(ctx, db,
		fmt.Sprintf(`SELECT tenant_id, id, payload FROM %s ORDER BY tenant_id, id`, spec.table))
	if err != nil {
		t.Fatalf("neki restore core: read %s back from the target: %v\n%s", spec.table, err,
			nekiExplainFailure("neki restore core: target read-back", restoreRec, err))
	}
	res.gotDigest, res.gotRows = nekiDigestTriples(got), len(got)

	if res.gotRows != res.wantRows {
		t.Errorf("neki restore core: %s holds %d rows, the generated corpus had %d — the restore did not land the "+
			"whole table", spec.table, res.gotRows, res.wantRows)
	}
	if res.gotDigest != res.wantDigest {
		t.Errorf("neki restore core: ORDERED-CONTENT MISMATCH on %s.\n  target:    %s (%d rows)\n"+
			"  generated: %s (%d rows)\n%s\nThe counts alone would not catch a NULL restored as an empty string, "+
			"a payload swapped between two rows, or a row duplicated onto two shards while another was dropped.",
			spec.table, res.gotDigest, res.gotRows, res.wantDigest, res.wantRows,
			nekiDiffTriples(wide, got))
	}

	// The sibling table too — a restore that landed the wide table and
	// silently skipped the small one would otherwise pass.
	gotSmall, err := nekiReadTriples(ctx, db,
		fmt.Sprintf(`SELECT tenant_id, id, payload FROM %s ORDER BY tenant_id, id`, spec.tableSmall))
	if err != nil {
		t.Errorf("neki restore core: read %s back from the target: %v", spec.tableSmall, err)
	} else if d := nekiDigestTriples(gotSmall); d != nekiDigestTriples(small) {
		t.Errorf("neki restore core: ORDERED-CONTENT MISMATCH on the sibling table %s: target %s (%d rows) vs "+
			"generated %s (%d rows)", spec.tableSmall, d, len(gotSmall), nekiDigestTriples(small), len(small))
	}

	// The secondary index must exist, or the index phase was a no-op and the
	// arm's claim to have exercised Neki's online-DDL index build is void.
	if spec.secondaryIndex {
		var idx int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_catalog.pg_indexes WHERE tablename = $1 AND indexname = $2`,
			spec.table, spec.table+"_payload_idx").Scan(&idx); err != nil {
			t.Errorf("neki restore core: read back the secondary index: %v", err)
		} else if idx != 1 {
			t.Errorf("neki restore core: the secondary index %s_payload_idx is MISSING after restore — the "+
				"index phase either skipped it or renamed it, and this arm's claim to exercise the online-DDL "+
				"index build is void", spec.table)
		}
	}

	t.Logf("neki restore core: restored %d + %d rows into %d chunk file(s); restore resolved table-parallelism=%d "+
		"(reason %q); backup store %d bytes at %s",
		res.wantRows, len(small), nekiChunkCount(manifest), res.tableParallelism, res.dispatchReason,
		nekiStoreBytes(t, storeRoot), storeRoot)
	return res
}

// nekiChunkCount totals the manifest's chunk files.
func nekiChunkCount(m *irbackup.Manifest) int {
	n := 0
	for _, entry := range m.Tables {
		n += len(entry.Chunks)
	}
	return n
}

// nekiDiffTriples renders the first few divergences between the expected and
// observed row sets. A bare digest mismatch on a 20,000-row table is nearly
// useless to whoever reads the weekly, and the cluster is gone by then.
func nekiDiffTriples(want, got []nekiTriple) string {
	render := func(r nekiTriple) string {
		if r.payload.Valid {
			return fmt.Sprintf("(%d,%d)=%q", r.tenant, r.id, r.payload.String)
		}
		return fmt.Sprintf("(%d,%d)=NULL", r.tenant, r.id)
	}
	var b strings.Builder
	b.WriteString("  first divergences (want → got):")
	shown := 0
	for i := 0; i < len(want) && i < len(got) && shown < 5; i++ {
		if want[i] == got[i] {
			continue
		}
		fmt.Fprintf(&b, "\n    [%d] %s → %s", i, render(want[i]), render(got[i]))
		shown++
	}
	if shown == 0 {
		fmt.Fprintf(&b, "\n    none in the overlapping prefix — the sets differ in LENGTH (want %d, got %d), so "+
			"rows were added or dropped rather than altered", len(want), len(got))
	}
	return b.String()
}

// nekiBackupCoreResult is what the backup core hands back.
//
// The digests are PER TABLE rather than one pair, because a single pair
// silently reports whichever table happened to be compared last — a log line
// that names one table's digest while claiming to describe the run is the
// small dishonest shape this suite spends its comments on.
type nekiBackupCoreResult struct {
	manifest     *irbackup.Manifest
	sourceDigest map[string]string
	readbackHash map[string]string
	sourceRows   map[string]int
	readbackRows map[string]int
	storeRoot    string
	rawCopyNote  string
	report       backup.VerifyReport
}

// nekiBackupCoreFromPG takes a full backup of the named tables FROM a
// PostgreSQL-family source and reads it back WITHOUT touching that source
// again, grading the artifact's content against an ordered SELECT of the
// source digested in Go.
//
// The read-back target is a local SQLite file. That is deliberate and it is
// the whole independence argument: restoring into the source would compare the
// backup against the thing it was taken from through the same engine, and
// `backup verify` — which is also run, below — streams each chunk through the
// same reader restore uses and so is internally consistent with the artifact
// by construction. Neither alone is evidence about content. The Go-side digest
// of the source is.
func nekiBackupCoreFromPG(
	ctx context.Context,
	t *testing.T,
	sourceDSN string,
	tables []string,
	chunkRows int,
) *nekiBackupCoreResult {
	t.Helper()

	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("neki backup core: open the source for the independent read: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The INDEPENDENT expected value, taken BEFORE the backup runs.
	perTableWant := map[string][]nekiTriple{}
	for _, tbl := range tables {
		rows, err := nekiReadTriples(ctx, db, nekiOrderedProjection(tbl))
		if err != nil {
			t.Fatalf("neki backup core: read %s from the source for the independent expected value: %v", tbl, err)
		}
		if len(rows) == 0 {
			t.Fatalf("neki backup core: %s is EMPTY on the source, so backing it up and reading it back would "+
				"compare nothing to nothing", tbl)
		}
		perTableWant[tbl] = rows
	}

	// The raw-copy lane's disposition, recorded rather than assumed: on Neki
	// the reader declines it (the router refuses `COPY (SELECT …) TO` with
	// NK013), so this backup is the IR copy path — which is precisely what
	// has never been measured against a sharded READ.
	rawCopyNote := "(the row reader does not expose ir.RawCopyDecliner)"
	if rr, err := (Engine{}).OpenRowReader(ctx, sourceDSN); err == nil {
		if decliner, ok := rr.(ir.RawCopyDecliner); ok {
			declined, reason := decliner.DeclinesRawCopy()
			rawCopyNote = fmt.Sprintf("raw-copy lane declined=%t reason=%q", declined, reason)
		}
		migcore.CloseIf(rr)
	} else {
		rawCopyNote = fmt.Sprintf("(could not open a row reader to ask: %v)", err)
	}

	storeRoot := t.TempDir()
	store, err := blobcodec.NewLocalStore(storeRoot)
	if err != nil {
		t.Fatalf("neki backup core: NewLocalStore: %v", err)
	}

	rec := newNekiPhaseRecorder()
	if err := (&backup.Backup{
		Source:    Engine{},
		SourceDSN: sourceDSN,
		Store:     store,
		Filter:    migcore.TableFilter{Include: tables},
		ChunkRows: chunkRows,
		Progress:  rec,
	}).Run(ctx); err != nil {
		t.Fatalf("%s", nekiExplainFailure("neki backup core: backup FROM the source", rec, err))
	}

	manifest, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("neki backup core: read the manifest the backup just wrote: %v", err)
	}
	if manifest.PartialState != irbackup.BackupStateComplete {
		t.Errorf("neki backup core: the manifest's PartialState is %q, want %q — the backup did not finish",
			manifest.PartialState, irbackup.BackupStateComplete)
	}

	// The manifest's recorded row count vs the independent count. These are
	// two different numbers from two different reads; a backup that dropped
	// rows and recorded the number it dropped to would satisfy `verify` and
	// fail here.
	recorded := map[string]int64{}
	for _, entry := range manifest.Tables {
		recorded[entry.Name] = entry.RowCount
	}
	for _, tbl := range tables {
		want := int64(len(perTableWant[tbl]))
		got, ok := recorded[tbl]
		if !ok {
			t.Errorf("neki backup core: the manifest carries NO entry for %s, although the filter named it — "+
				"the table was silently dropped from the backup", tbl)
			continue
		}
		if got != want {
			t.Errorf("neki backup core: the manifest records %d rows for %s; an independent ordered SELECT of the "+
				"source counted %d", got, tbl, want)
		}
	}

	// `backup verify --depth read`. Reported, not relied on for content —
	// see this function's doc.
	report, err := backup.VerifyBackupCodedReport(ctx, store, backup.VerifyOptions{Depth: backup.VerifyDepthRead})
	if err != nil {
		t.Errorf("neki backup core: backup verify --depth read FAILED on the artifact this run just wrote: %v", err)
	}
	if report.Failed != 0 {
		t.Errorf("neki backup core: backup verify reported %d failed chunk(s) of %d", report.Failed, report.Chunks)
	}

	// The read-back: restore into a LOCAL SQLite file, never back into the
	// source.
	dstDSN := filepath.Join(t.TempDir(), "neki-backup-readback.db")
	readRec := newNekiPhaseRecorder()
	if err := (&backup.Restore{
		Target:    sqlite.Engine{},
		TargetDSN: dstDSN,
		Store:     store,
		Progress:  readRec,
	}).Run(ctx); err != nil {
		t.Fatalf("%s\n\nThis is the read-back into a LOCAL SQLite file, so a failure here is about the ARTIFACT "+
			"(or the cross-engine retarget), not about the source.",
			nekiExplainFailure("neki backup core: read the backup back into local SQLite", readRec, err))
	}

	dst, err := sql.Open("sqlite", dstDSN)
	if err != nil {
		t.Fatalf("neki backup core: open the SQLite read-back target: %v", err)
	}
	defer func() { _ = dst.Close() }()

	res := &nekiBackupCoreResult{
		manifest:     manifest,
		sourceDigest: map[string]string{},
		readbackHash: map[string]string{},
		sourceRows:   map[string]int{},
		readbackRows: map[string]int{},
		storeRoot:    storeRoot,
		rawCopyNote:  rawCopyNote,
		report:       report,
	}
	for _, tbl := range tables {
		want := perTableWant[tbl]
		got, err := nekiReadTriples(ctx, dst, nekiOrderedProjection(tbl))
		if err != nil {
			t.Errorf("neki backup core: read %s back out of the restored artifact: %v", tbl, err)
			continue
		}
		wantDigest, gotDigest := nekiDigestTriples(want), nekiDigestTriples(got)
		res.sourceDigest[tbl], res.readbackHash[tbl] = wantDigest, gotDigest
		res.sourceRows[tbl], res.readbackRows[tbl] = len(want), len(got)
		if wantDigest != gotDigest {
			t.Errorf("neki backup core: ORDERED-CONTENT MISMATCH on %s between the SOURCE and the backup read "+
				"back independently.\n  source:  %s (%d rows)\n  backup:  %s (%d rows)\n%s",
				tbl, wantDigest, len(want), gotDigest, len(got), nekiDiffTriples(want, got))
		}
	}

	var digests strings.Builder
	for _, tbl := range tables {
		fmt.Fprintf(&digests, "\n  %s: source %s (%d rows) == backup %s (%d rows)",
			tbl, res.sourceDigest[tbl], res.sourceRows[tbl], res.readbackHash[tbl], res.readbackRows[tbl])
	}
	t.Logf("neki backup core: %d table(s), %d chunk file(s), %d bytes at %s; verify --depth read: %d chunk(s), "+
		"%d failed, %d authenticated; %s%s",
		len(manifest.Tables), nekiChunkCount(manifest), nekiStoreBytes(t, storeRoot), storeRoot,
		report.Chunks, report.Failed, report.Authenticated, rawCopyNote, digests.String())
	return res
}

// nekiOrderedProjection renders the ordered (tenant, id, payload) read for a
// table.
//
// The fixture's own `sk_good` names its payload column `v` while the restored
// tables name it `payload`; aliasing here keeps ONE scan shape, so the digest
// function cannot be fed a differently-ordered column list by accident.
func nekiOrderedProjection(table string) string {
	payload := "payload"
	if table == "sk_good" {
		payload = "v"
	}
	return fmt.Sprintf(`SELECT tenant_id, id, %s FROM %s ORDER BY tenant_id, id`, payload, table)
}
