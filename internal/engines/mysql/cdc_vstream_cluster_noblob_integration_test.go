//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item-74 / ADR-0172 VStream partial-row-image belt — DURABLE end-to-end
// coverage against a REAL binlog_row_image=NOBLOB Vitess cluster.
//
// The in-repo unit pins (cdc_vstream_partial_row_image_test.go) hand-build
// the RowChange.DataColumns bitmap and never boot a NOBLOB tablet, so they
// exercise the belt's dispatch wiring but NOT the real VStream
// AllowNoBlobBinlogRowImage wire path. This suite closes that gap: it boots
// the full cluster with binlog_row_image=NOBLOB written into each tablet's
// mysqld via EXTRA_MY_CNF (docker-compose.noblob.yml), so an unchanged
// BLOB/TEXT column genuinely drops out of an UPDATE after-image on the wire
// — the exact silent-loss trap the belt refuses.
//
// It covers BOTH dispatch paths that carry the belt:
//
//   - WarmResumePath (vstreamCDCReader.dispatchRow): the standalone CDC
//     tail, OpenCDCReader -> StreamChanges.
//   - ColdStartPath (vstreamSnapshotStream.dispatchCDCRow): the DEFAULT
//     first sync, OpenSnapshotStream -> COPY drain -> Changes.StreamChanges
//     catch-up. This is the v0.99.273 fix path — audit-2026-07-17 A1 found
//     the belt was wired into dispatchRow but MISSED on its hand-mirrored
//     twin dispatchCDCRow, so a NOBLOB cold start silently wrote NULL over
//     an unchanged BLOB/TEXT column.
//
// Each test asserts up front that @@global.binlog_row_image = NOBLOB on
// BOTH tablet mysqlds (primary uid 100 + replica uid 101) — the critical
// non-vacuity guard: a provisioning regression that silently reverts to
// FULL would make the belt never fire and the test pass vacuously green, so
// the guard fails LOUD before the behavioral assertions run.
//
// Run (heavy — own build tag, NOT in the per-PR gate):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessClusterNoBlob' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Distinct host ports from the full-cluster (15306/15991) and primary-only
// (15406/15891) harnesses so the NOBLOB stack (and a stale one from a
// crashed run) never collides with a sibling suite.
const (
	noblobMySQLPort = 15506
	noblobGRPCPort  = 15791
)

// noblobCluster holds the handle to a running NOBLOB Vitess stack. It keeps
// the compose invocation (dockerBin + the layered `-f` files + project +
// env) so the non-vacuity guard can `compose exec` into the tablets to read
// each mysqld's live @@global.binlog_row_image.
type noblobCluster struct {
	dockerBin    string
	composeFiles []string
	project      string
	baseEnv      []string

	mysqlDSN     string
	grpcEndpoint string
}

// startVitessClusterNoBlob boots the base cluster with the NOBLOB override
// layered on, so every tablet's mysqld runs binlog_row_image=NOBLOB from
// connect time. Mirrors startVitessCluster / startVitessClusterPrimaryOnly;
// the only differences are the layered override file and the published
// ports. Returns the handle and a teardown.
func startVitessClusterNoBlob(t *testing.T) (cluster *noblobCluster, cleanup func()) {
	t.Helper()

	// The NOBLOB override (docker-compose.noblob.yml) hardcodes the modern
	// hyphenated tablet flags that are canonical from Vitess v23; v21/v22
	// server binaries only accept the legacy underscore form and would fail
	// to boot. The full-cluster harness layers a legacy-flags override for
	// those majors, but this belt suite deliberately does not (it validates a
	// v24-era VStream posture), so SKIP on a legacy-flag image. This matters
	// because these tests match the vitess-version-matrix `cluster` leg's
	// `-run TestVitessCluster` filter and would otherwise red its v21/v22
	// legs; the dedicated extended-suites `noblob` leg runs the default v24.
	if major, ok := vitessClusterMajor(); ok && major < 23 {
		t.Skipf("noblob belt suite requires modern hyphenated tablet flags (Vitess v23+); VITESS_LITE_IMAGE major %d uses legacy underscore flags", major)
	}

	dockerBin := findDocker(t)
	baseCompose := composeFilePath(t)
	overrideCompose := noblobComposeFilePath(t)
	project := fmt.Sprintf("sluice-vitesscluster-noblob-%d", os.Getpid())

	c := &noblobCluster{
		dockerBin:    dockerBin,
		composeFiles: []string{"-f", baseCompose, "-f", overrideCompose},
		project:      project,
		baseEnv: append(
			os.Environ(),
			"COMPOSE_PROJECT="+project,
			fmt.Sprintf("VTGATE_MYSQL_PORT=%d", noblobMySQLPort),
			fmt.Sprintf("VTGATE_GRPC_PORT=%d", noblobGRPCPort),
		),
		mysqlDSN: fmt.Sprintf(
			"root@tcp(127.0.0.1:%d)/%s?parseTime=true&interpolateParams=true",
			noblobMySQLPort, clusterKeyspace,
		),
		grpcEndpoint: fmt.Sprintf("127.0.0.1:%d", noblobGRPCPort),
	}

	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if out, err := c.runCompose(ctx, "down", "-v", "--remove-orphans"); err != nil {
			t.Logf("noblob cluster teardown: %v\n%s", err, out)
		}
	}

	upCtx, upCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer upCancel()
	if out, err := c.runCompose(upCtx, "up", "-d"); err != nil {
		cleanup()
		t.Fatalf("docker compose up (noblob): %v\n%s", err, out)
	}

	if err := waitForWritablePrimary(t, c.mysqlDSN, 4*time.Minute); err != nil {
		out, _ := c.runCompose(context.Background(), "logs", "--tail", "40")
		cleanup()
		t.Fatalf("noblob cluster never reached writable PRIMARY: %v\nrecent logs:\n%s", err, out)
	}

	return c, cleanup
}

// runCompose runs a `docker compose` subcommand against the NOBLOB stack's
// layered compose files + project. Mirrors the closure the other cluster
// harnesses build inline; a method here so the non-vacuity guard can reach it.
func (c *noblobCluster) runCompose(ctx context.Context, args ...string) ([]byte, error) {
	full := append(append([]string{"compose"}, c.composeFiles...), "-p", c.project)
	full = append(full, args...)
	cmd := exec.CommandContext(ctx, c.dockerBin, full...)
	cmd.Env = c.baseEnv
	return cmd.CombinedOutput()
}

// tabletBinlogRowImage reads @@global.binlog_row_image directly from the
// named tablet service's mysqld (bypassing vtgate) via `compose exec`, so
// the value reflects that specific tablet's mysqld — not whatever vtgate
// routes to. The per-tablet mysqld listens on its own unix socket under the
// datadir (/vt/vtdataroot/vt_<uid>/mysql.sock); the glob matches the single
// socket in the container. Connects as the socket-local root vitess's
// init_db provisions with full privileges.
func (c *noblobCluster) tabletBinlogRowImage(t *testing.T, service string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const query = `mysql -u root -S "$(ls /vt/vtdataroot/vt_*/mysql.sock)" -N -B ` +
		`-e "SELECT @@global.binlog_row_image"`
	out, err := c.runCompose(ctx, "exec", "-T", service, "sh", "-c", query)
	if err != nil {
		t.Fatalf("read @@global.binlog_row_image from %s: %v\n%s", service, err, out)
	}
	return strings.TrimSpace(string(out))
}

// assertNoBlobProvisioned is the non-vacuity guard: both tablet mysqlds MUST
// report binlog_row_image=NOBLOB up front. If provisioning silently reverted
// to FULL the belt would never fire and every behavioral assertion below
// would pass vacuously green — so this fails LOUD first.
func (c *noblobCluster) assertNoBlobProvisioned(t *testing.T) {
	t.Helper()
	for _, svc := range []string{"vttablet", "vttablet-replica"} {
		if got := c.tabletBinlogRowImage(t, svc); !strings.EqualFold(got, "NOBLOB") {
			t.Fatalf("non-vacuity guard: %s @@global.binlog_row_image = %q; want NOBLOB "+
				"(provisioning regressed — the belt would never fire and this suite would pass vacuously)", svc, got)
		}
	}
	t.Log("non-vacuity guard PASS: both tablet mysqlds run binlog_row_image=NOBLOB")
}

// noblobComposeFilePath resolves the NOBLOB override next to the base
// compose file, reusing composeFilePath's anchor then swapping the filename.
func noblobComposeFilePath(t *testing.T) string {
	t.Helper()
	base := composeFilePath(t)
	return strings.Replace(base, "docker-compose.yml", "docker-compose.noblob.yml", 1)
}

// noblobUsersDDL seeds a table with a TEXT column — the BLOB/TEXT family
// NOBLOB drops from an UPDATE after-image when the column is unchanged.
const noblobUsersDDL = `
	CREATE TABLE users (
		id    BIGINT       NOT NULL AUTO_INCREMENT,
		email VARCHAR(255) NOT NULL,
		bio   TEXT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// noblobUsersTable is the ir.Table describing `users` for the cold-start
// COPY drain (Rows.ReadRows).
func noblobUsersTable() *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "bio", Type: ir.Text{Size: ir.TextRegular}},
		},
	}
}

// TestVitessClusterNoBlob_WarmResumePath drives the belt through the
// STANDALONE CDC tail (vstreamCDCReader.dispatchRow) against a real NOBLOB
// cluster. It asserts the two negatives don't over-fire — a FULL-image
// INSERT and a blob-CHANGING UPDATE both flow with the TEXT column intact —
// and that the positive fires: an unchanged-BLOB UPDATE (which NOBLOB
// renders as a partial after-image on the wire) stops the stream with the
// loud coded CodeCDCRowImagePartial naming the omitted `bio` column.
func TestVitessClusterNoBlob_WarmResumePath(t *testing.T) {
	c, cleanup := startVitessClusterNoBlob(t)
	defer cleanup()
	c.assertNoBlobProvisioned(t)

	applyClusterSQL(t, c.mysqlDSN, noblobUsersDDL)
	// Let the tablet's schema engine register the table before the VStream
	// FieldEvent (column-type metadata) is needed.
	time.Sleep(3 * time.Second)

	// FlavorPlanetScale, vstream_tablet_type=primary (the full cluster has a
	// replica, but primary keeps the tail deterministic and matches the
	// belt's target posture). Self-hosted transport/auth defaults set
	// explicitly.
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		c.mysqlDSN, c.grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc, ok := rdr.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *vstreamCDCReader", rdr)
	}
	defer func() { _ = cdc.Close() }()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle: vtgate's stream registers at "current" a moment after
	// StreamChanges returns; DML too early lands before the boundary.
	time.Sleep(3 * time.Second)

	const (
		originalBio = "original-bio-blobtext"
		changedBio  = "changed-bio-blobtext"
	)
	// (1) FULL-image INSERT — every column logged; bio present.
	applyClusterSQL(t, c.mysqlDSN,
		fmt.Sprintf("INSERT INTO users (id, email, bio) VALUES (1, 'a@x', '%s')", originalBio))
	// (2) blob-CHANGING UPDATE — bio IS in the after image (it changed), so
	// DataColumns is full and the belt must NOT fire.
	applyClusterSQL(t, c.mysqlDSN,
		fmt.Sprintf("UPDATE users SET bio = '%s' WHERE id = 1", changedBio))
	// (3) unchanged-BLOB UPDATE — bio omitted from the after image under
	// NOBLOB, so DataColumns has bio's bit UNSET: the belt fires here.
	applyClusterSQL(t, c.mysqlDSN,
		"UPDATE users SET email = 'b@y' WHERE id = 1")

	got := drainNoBlobUntilClosed(t, ctx, changes, 90*time.Second)

	// Negatives: the INSERT and the blob-CHANGING UPDATE flowed unrefused,
	// each carrying bio's real value (no over-fire dropped a present column).
	ins := firstInsert(t, got, "users")
	if bio := asStringVal(ins.Row["bio"]); bio != originalBio {
		t.Errorf("INSERT bio = %q; want %q (FULL image must carry the TEXT column)", bio, originalBio)
	}
	upd := firstUpdate(t, got, "users")
	if bio := asStringVal(upd.After["bio"]); bio != changedBio {
		t.Errorf("blob-CHANGING UPDATE after.bio = %q; want %q (a changed blob is present, belt must not over-fire)", bio, changedBio)
	}

	// Positive: the unchanged-BLOB UPDATE stopped the stream loudly, naming bio.
	assertBeltRefusal(t, cdc.Err(), "bio")
	t.Log("WarmResumePath PASS: FULL INSERT + blob-changing UPDATE flowed; unchanged-blob UPDATE refused loud (CodeCDCRowImagePartial, names bio)")
}

// TestVitessClusterNoBlob_ColdStartPath drives the belt through the
// COLD-START snapshot->CDC catch-up (vstreamSnapshotStream.dispatchCDCRow),
// the DEFAULT first sync and the v0.99.273 fix path. It asserts the COPY
// drain is full-image (all seeded rows land with their TEXT column intact —
// no over-fire), a post-COPY FULL-image INSERT flows, and an unchanged-BLOB
// UPDATE stops the catch-up stream with the loud coded CodeCDCRowImagePartial
// naming bio.
func TestVitessClusterNoBlob_ColdStartPath(t *testing.T) {
	c, cleanup := startVitessClusterNoBlob(t)
	defer cleanup()
	c.assertNoBlobProvisioned(t)

	applyClusterSQL(t, c.mysqlDSN, noblobUsersDDL)
	time.Sleep(3 * time.Second)

	// Seed rows BEFORE the snapshot so they land in the COPY phase.
	const seedRows = 3
	seedBio := func(i int) string { return fmt.Sprintf("seed-bio-%d", i) }
	var sb strings.Builder
	sb.WriteString("INSERT INTO users (id, email, bio) VALUES ")
	for i := 1; i <= seedRows; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "(%d,'seed%d@x','%s')", i, i, seedBio(i))
	}
	applyClusterSQL(t, c.mysqlDSN+"&multiStatements=true", sb.String())
	time.Sleep(2 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		c.mysqlDSN, c.grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// COPY drain: every seeded row must land with bio intact — the COPY
	// phase reads full rows, so a partial-image belt must NOT over-fire here.
	rowsCh, err := stream.Rows.ReadRows(ctx, noblobUsersTable())
	if err != nil {
		t.Fatalf("ReadRows(users): %v", err)
	}
	copied := map[int64]string{}
	for row := range rowsCh {
		id, ok := asInt64Val(row["id"])
		if !ok {
			t.Fatalf("COPY row has non-integer id: %#v", row["id"])
		}
		copied[id] = asStringVal(row["bio"])
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("snapshot COPY error after drain: %v", err)
	}
	if len(copied) != seedRows {
		t.Fatalf("COPY delivered %d rows; want %d", len(copied), seedRows)
	}
	for i := int64(1); i <= seedRows; i++ {
		if copied[i] != seedBio(int(i)) {
			t.Fatalf("COPY row id=%d bio = %q; want %q (COPY must carry the TEXT column full)", i, copied[i], seedBio(int(i)))
		}
	}

	// Catch-up: resume CDC from the COPY_COMPLETED position.
	catchup, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("Changes.StreamChanges: %v", err)
	}
	// Settle before post-COPY DML so it lands in the CDC window.
	time.Sleep(3 * time.Second)

	const newBio = "postcopy-bio-blobtext"
	// (1) FULL-image INSERT — flows through dispatchCDCRow.
	applyClusterSQL(t, c.mysqlDSN,
		fmt.Sprintf("INSERT INTO users (id, email, bio) VALUES (100, 'new@x', '%s')", newBio))
	// (2) unchanged-BLOB UPDATE — bio omitted; the belt fires on dispatchCDCRow.
	applyClusterSQL(t, c.mysqlDSN,
		"UPDATE users SET email = 'changed@x' WHERE id = 1")

	got := drainNoBlobUntilClosed(t, ctx, catchup, 90*time.Second)

	// Negative: the FULL-image INSERT flowed with bio intact.
	ins := firstInsert(t, got, "users")
	if bio := asStringVal(ins.Row["bio"]); bio != newBio {
		t.Errorf("catch-up INSERT bio = %q; want %q", bio, newBio)
	}

	// Positive: the unchanged-BLOB UPDATE stopped the catch-up stream loudly.
	// The error surfaces via the cold-start CDC half's optional Err() probe
	// (vstreamSnapshotChanges.Err() → the terminal pump error) — the exact
	// surface audit A1 found unguarded. Mirrors the pipeline's own probe.
	errer, ok := stream.Changes.(interface{ Err() error })
	if !ok {
		t.Fatalf("cold-start Changes (%T) exposes no Err(); cannot observe the belt refusal", stream.Changes)
	}
	assertBeltRefusal(t, errer.Err(), "bio")
	t.Log("ColdStartPath PASS: COPY full-image (bio intact) + FULL INSERT flowed; unchanged-blob UPDATE refused loud on dispatchCDCRow (CodeCDCRowImagePartial, names bio)")
}

// drainNoBlobUntilClosed collects changes until the channel closes (the belt
// terminates the stream) or the deadline / context fires. The belt refusal
// closes the channel, so a well-behaved run always reaches the close case.
func drainNoBlobUntilClosed(t *testing.T, ctx context.Context, changes <-chan ir.Change, timeout time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case ch, ok := <-changes:
			if !ok {
				return got
			}
			got = append(got, ch)
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for the stream to close (got %d changes so far); "+
				"the belt should have stopped the stream", timeout, len(got))
		case <-ctx.Done():
			t.Fatalf("context done draining changes (got %d): %v", len(got), ctx.Err())
		}
	}
}

// firstInsert returns the first ir.Insert for table in got, or fails.
func firstInsert(t *testing.T, got []ir.Change, table string) ir.Insert {
	t.Helper()
	for _, ch := range got {
		if ins, ok := ch.(ir.Insert); ok && ins.Table == table {
			return ins
		}
	}
	t.Fatalf("no ir.Insert for %q among %d changes (%s)", table, len(got), changeKinds(got))
	return ir.Insert{}
}

// firstUpdate returns the first ir.Update for table in got, or fails.
func firstUpdate(t *testing.T, got []ir.Change, table string) ir.Update {
	t.Helper()
	for _, ch := range got {
		if upd, ok := ch.(ir.Update); ok && upd.Table == table {
			return upd
		}
	}
	t.Fatalf("no ir.Update for %q among %d changes (%s)", table, len(got), changeKinds(got))
	return ir.Update{}
}

// changeKinds renders the concrete types of got for a failure message.
func changeKinds(got []ir.Change) string {
	kinds := make([]string, len(got))
	for i, ch := range got {
		kinds[i] = fmt.Sprintf("%T", ch)
	}
	return strings.Join(kinds, ",")
}

// assertBeltRefusal fails unless err is the coded partial-row-image refusal
// (CodeCDCRowImagePartial) naming the omitted column.
func assertBeltRefusal(t *testing.T, err error, wantCol string) {
	t.Helper()
	if err == nil {
		t.Fatal("unchanged-blob UPDATE: stream closed with Err()==nil — the belt did NOT fire (silent NULL-overwrite class)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
		t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCRowImagePartial, err, err)
	}
	if !strings.Contains(err.Error(), wantCol) {
		t.Errorf("belt refusal does not name the omitted column %q: %v", wantCol, err)
	}
}

// asStringVal coerces a decoded ir.Row cell (TEXT decodes to string or
// []byte per the value contract) to a string for comparison.
func asStringVal(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// asInt64Val coerces a decoded integer cell (driver int64 or []byte text) to
// int64.
func asInt64Val(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case []byte:
		var n int64
		if _, err := fmt.Sscanf(string(x), "%d", &n); err == nil {
			return n, true
		}
	case string:
		var n int64
		if _, err := fmt.Sscanf(x, "%d", &n); err == nil {
			return n, true
		}
	}
	return 0, false
}
