//go:build integration && vstream

package mysql

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowReader_VStream_OLAPScopedToFullScan_Bug132 pins the Bug 132 fix:
// the PlanetScale (VStream) flavor's RowReader applies `workload=olap`
// ONLY to the unbounded [RowReader.ReadRows] full scan (the no-PK path
// vtgate's OLTP cap would silently truncate at ~100k rows), on a DEDICATED
// connection — NEVER as a session-wide setting. v0.99.14 set it session-
// wide via a DSN param, which also covered the LIMIT-paged ReadRowsBatch
// the parallel chunked copy uses, where olap streaming truncates each
// concurrent chunk's page → a silent partial copy of large PK tables.
//
// Two assertions:
//  1. The reader's POOLED session is NOT globally olap (@@workload == OLTP)
//     — the regression property: a session-wide olap would re-break the
//     chunked paged reads.
//  2. A no-PK ReadRows full scan still returns every row — proving the
//     scoped `SET workload='olap'` on the dedicated full-scan conn is
//     ACCEPTED by vtgate (a Bug-126-class rejection would surface) and the
//     full scan works. (The >100k cap-lift itself is the same SET as
//     before, just scoped; validated at scale on the PlanetScale rig.)
func TestRowReader_VStream_OLAPScopedToFullScan_Bug132(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// A NO-PK table: its bulk read is the single unbounded ReadRows full
	// scan that needs (scoped) olap.
	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE events_nopk (
			label VARCHAR(64) NOT NULL,
			n     BIGINT      NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	time.Sleep(3 * time.Second) // vtgate schema-tracker settle

	const seeded = 50
	var vals []string
	for i := 0; i < seeded; i++ {
		vals = append(vals, "('row"+strconv.Itoa(i)+"',"+strconv.Itoa(i)+")")
	}
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true",
		"INSERT INTO events_nopk (label,n) VALUES "+strings.Join(vals, ","))
	time.Sleep(2 * time.Second)

	sluiceDSN := mysqlDSN +
		"&vstream_endpoint=" + grpcEndpoint +
		"&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rr, err := (Engine{Flavor: FlavorPlanetScale}).OpenRowReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenRowReader (planetscale): %v", err)
	}
	defer closeIfCloser(rr)

	// (1) The pooled session must NOT be globally olap. A session-wide olap
	// is exactly what truncated the chunked paged reads (Bug 132).
	q := rr.(*RowReader).q
	var workload string
	rows, err := q.QueryContext(ctx, "SELECT @@workload")
	if err != nil {
		t.Fatalf("SELECT @@workload: %v", err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal("SELECT @@workload returned no rows")
	}
	if err := rows.Scan(&workload); err != nil {
		rows.Close()
		t.Fatalf("scan @@workload: %v", err)
	}
	rows.Close()
	if strings.EqualFold(workload, "olap") {
		t.Errorf("pooled session @@workload = %q; want OLTP — olap must NOT be session-wide "+
			"(it truncates concurrent chunked paged reads, Bug 132)", workload)
	}

	// (2) The no-PK ReadRows full scan returns every row (scoped olap SET
	// accepted + full scan works).
	tbl := &ir.Table{
		Name: "events_nopk",
		Columns: []*ir.Column{
			{Name: "label", Type: ir.Varchar{Length: 64}},
			{Name: "n", Type: ir.Integer{Width: 64}},
		},
	}
	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows (no-PK full scan): %v", err)
	}
	got := 0
	for range ch {
		got++
	}
	if err := rr.(*RowReader).Err(); err != nil {
		t.Fatalf("ReadRows stream error: %v", err)
	}
	if got != seeded {
		t.Errorf("no-PK ReadRows returned %d rows; want %d (scoped-olap full scan must read all)", got, seeded)
	}
}
