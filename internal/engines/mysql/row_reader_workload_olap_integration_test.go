//go:build integration && vstream

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRowReader_PlanetScaleFlavor_AppliesWorkloadOLAP verifies the
// PlanetScale (VStream) flavor's RowReader sets `workload=olap` on its
// session — the fix for vtgate's default OLTP ~100k-row result-set cap,
// which silently truncates a no-PK full-table copy (a no-PK table can't be
// PK-chunked, so its bulk read is one big SELECT). The session SET must
// (1) be ACCEPTED by vtgate — a Bug-126-class leak/rejection would surface
// at open — and (2) actually take effect (@@workload == OLAP). Vanilla
// MySQL never gets it (gated on the CDCVStream capability; see engine.go
// and the unit pin TestOpenRowReader_WorkloadGate).
func TestRowReader_PlanetScaleFlavor_AppliesWorkloadOLAP(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			label VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	// vttestserver's schema tracker is async; let vtgate pick up the table.
	time.Sleep(3 * time.Second)

	// The planetscale-shaped DSN a real `--source-driver=planetscale`
	// invocation produces (mirrors the Bug 126 test): embedded-MySQL DSN
	// plus sluice's vstream_* extensions.
	sluiceDSN := mysqlDSN +
		"&vstream_endpoint=" + grpcEndpoint +
		"&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rr, err := (Engine{Flavor: FlavorPlanetScale}).OpenRowReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenRowReader (planetscale): %v — a rejected `SET workload='olap'` would surface here", err)
	}
	defer closeIfCloser(rr)

	// Prove the reader's session actually carries OLAP. vtgate exposes the
	// vitess-aware `workload` variable; every pooled connection runs the
	// session SET, so whichever connection the querier hands out reports it.
	q := rr.(*RowReader).q
	rows, err := q.QueryContext(ctx, "SELECT @@workload")
	if err != nil {
		t.Fatalf("SELECT @@workload: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatalf("@@workload iteration: %v", err)
		}
		t.Fatal("SELECT @@workload returned no rows")
	}
	var workload string
	if err := rows.Scan(&workload); err != nil {
		t.Fatalf("scan @@workload: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("@@workload rows err: %v", err)
	}
	if !strings.EqualFold(workload, "olap") {
		t.Errorf("@@workload = %q; want OLAP (the no-PK 100k-cap lift)", workload)
	}
}
