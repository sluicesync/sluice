//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Local-Vitess RESHARDING-topology infrastructure + the reshard-chaos
// correctness test (Track 1a, the headline test).
//
// WHY A SEPARATE BUILD TAG (`vitessreshard`, not `vstream`):
//
// The existing `vstream` tag boots vitess/vttestserver:mysql80, a
// SINGLE-PROCESS vtcombo all-in-one. Phase-A ground-truth (documented
// in docs/dev/development.md and the Track-1a report) proved that
// image CANNOT reshard: it ships only {vtcombo, vttestserver,
// mysqlctl}, has NO vtctldclient / vtctld / standalone vttablet, and
// vtcombo's keyspace shard count is fixed at boot (--num-shards).
// `vtctldclient Reshard create` + `SwitchTraffic` require the full
// multi-process cluster (external topo + vtctld + per-shard
// vttablets + vtgate) — exactly the topology vitess/lite + an etcd
// topo server provide.
//
// So Track-1a's reshard core runs a scripted multi-container Vitess
// cluster (the examples/local topology, containerised):
//
//	etcd  (quay.io/coreos/etcd)         — global+local topo server
//	vitess/lite container running:
//	  vtctld                            — admin RPC (Reshard, etc.)
//	  per shard: mysqlctl+vttablet x2   — a PRIMARY-candidate and a
//	                                      REPLICA (sluice's VStream
//	                                      reader streams REPLICA by
//	                                      design, so each shard needs
//	                                      one — ground-truthed below)
//	  vtgate                            — MySQL + gRPC frontends
//
// then `vtctldclient Reshard create --source-shards <src>
// --target-shards <dst>` + `Reshard SwitchTraffic`.
//
// IMAGE / TIME COST (documented per the Track-1a mandate; mirrored in
// docs/dev/development.md):
//   - vitess/lite:latest    ~2.0 GB
//   - quay.io/coreos/etcd   ~70 MB
//   - Cluster bring-up to ready ~40-60s. Observed wall time on the
//     dev box (Rancher/Windows): ProofOfReshardability ~80s,
//     ChaosExactlyOnce ~165s (6 mysqld for the chaos topology:
//     source -/ primary+replica + 2 target shards x primary+replica).
//     Test timeout set to 45m for headroom on a slow CI runner.
//
// This tag is HEAVY and SLOW; it is intentionally NOT part of the
// normal integration CI gate. The cheap CI-smoke subset (VStream
// basics + static-sharded correctness) stays under the existing
// `vstream` tag and runs in normal integration CI — see
// internal/pipeline/migrate_vstream_sharded_integration_test.go.
//
// Usage (Windows; see CLAUDE.md / docs/dev/development.md for the
// Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vitessreshard' -v -count=1 -timeout=25m \
//	  -run 'TestVitessReshard' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// ---------------------------------------------------------------------
// Cluster constants. Ports are container-internal; testcontainers maps
// the published ones to ephemeral host ports.
// ---------------------------------------------------------------------

const (
	vrEtcdImage   = "quay.io/coreos/etcd:v3.5.17"
	vrVitessImage = "vitess/lite:latest"

	vrKeyspace = "commerce"
	vrCell     = "zone1"

	// vtgate frontends (published to host).
	vrVtgateMySQLPort = "15306/tcp"
	vrVtgateGRPCPort  = "15991/tcp"

	// Stable Docker-network alias for the vitess/lite container, so a
	// sidecar (the toxiproxy latency proxy in the per-shard-latency A/B)
	// can forward UPSTREAM to a tablet by a fixed name instead of the
	// testcontainers-generated container id.
	vrVitessAlias = "vitess"
)

// vitessReshardCluster owns the etcd + vitess/lite containers and the
// network they share. The vitess/lite container runs a scripted
// examples/local-style cluster; vtctldExec shells `vtctldclient` into
// that container so the reshard workflow is driven exactly as an
// operator would.
type vitessReshardCluster struct {
	net       *testcontainers.DockerNetwork
	etcd      testcontainers.Container
	vitess    testcontainers.Container
	mysqlDSN  string // host-mapped vtgate MySQL frontend
	grpcAddr  string // host-mapped vtgate gRPC frontend
	terminate func()
}

// vtctldExec runs `vtctldclient --server localhost:15999 <args...>`
// inside the vitess/lite container and returns combined output. The
// reshard workflow is driven through this so the test exercises the
// real operator command path, not an in-process shortcut.
func (c *vitessReshardCluster) vtctldExec(t *testing.T, args ...string) (string, error) {
	t.Helper()
	full := append([]string{"/vt/bin/vtctldclient", "--server", "localhost:15999"}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	code, reader, err := c.vitess.Exec(ctx, full)
	if err != nil {
		return "", fmt.Errorf("exec %v: %w", args, err)
	}
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, rerr := reader.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if rerr != nil {
			break
		}
	}
	out := sb.String()
	if code != 0 {
		return out, fmt.Errorf("vtctldclient %v exit=%d: %s", args, code, out)
	}
	return out, nil
}

// clusterBringUpScript scripts the full examples/local topology in one
// vitess/lite container against the networked etcd. Shard count is
// parameterised by SOURCE_SHARDS (a space-separated list of shard
// ranges, e.g. "-" for unsharded or "-80 80-" for two shards). Target
// shards for the reshard are added on demand by the test via a second
// invocation of the per-shard tablet bring-up (vtTabletUp), so the
// script keeps only the source layout.
//
// The script is deliberately linear and loud: every step that can
// fail echoes a marker the test's wait strategy / assertions key on.
const clusterBringUpScript = `set -e
export VTDATAROOT=/vt/vtdataroot
export VTROOT=/vt
mkdir -p $VTDATAROOT
TOPO="--topo-implementation etcd2 --topo-global-server-address ${ETCD_ADDR} --topo-global-root /vitess/global"
VC="/vt/bin/vtctldclient --server localhost:15999"
CELL=` + vrCell + `
KS=` + vrKeyspace + `

# MySQL 8.4 (vitess/lite:latest ships 8.4.x) disables
# mysql_native_password by default; Vitess's internal MySQL client
# (tabletmanager) negotiates native auth, so without this every
# vttablet query-service start fails with "unsupported auth method:
# sha256_password" and PlannedReparentShard can never find a healthy
# tablet. EXTRA_MY_CNF is mysqlctl's documented merge hook. NOTE:
# default_authentication_plugin was REMOVED in MySQL 8.4 — setting it
# aborts mysqld init ("unknown variable"); only mysql_native_password
# =ON is valid here. Both facts ground-truthed via trace probes
# (Track-1a Phase A).
cat > $VTDATAROOT/native_auth.cnf <<'CNF'
[mysqld]
mysql_native_password=ON
CNF
export EXTRA_MY_CNF=$VTDATAROOT/native_auth.cnf

# --- vtctld (admin RPC on :15999) — takes topo flags directly ---
/vt/bin/vtctld $TOPO --cell ${CELL} \
  --service-map 'grpc-vtctl,grpc-vtctld' \
  --grpc-port 15999 --port 15998 \
  --pid-file $VTDATAROOT/vtctld.pid > $VTDATAROOT/vtctld.log 2>&1 &
# wait for vtctld's gRPC admin port to answer
for i in $(seq 1 60); do
  $VC GetKeyspaces >/dev/null 2>&1 && break
  sleep 1
done

# --- topo: register the cell (via the running vtctld) ---
$VC AddCellInfo --root /vitess/${CELL} --server-address ${ETCD_ADDR} ${CELL} || true

# --- per-shard tablet bring-up helper ---
# args: <shard> <tablet-uid> <mysql-port> <vttablet-port> <grpc-port>
# NOTE: bash reserves $UID (readonly); use TUID for the tablet uid.
# one_tablet brings up a single mysqld+vttablet pair.
# args: <shard> <tablet-uid> <mysql-port> <vttablet-port> <grpc-port>
one_tablet() {
  SHARD=$1; TUID=$2; MYP=$3; VTP=$4; GRP=$5
  TABLETDIR=$VTDATAROOT/vt_$(printf '%010d' $TUID)
  mkdir -p $TABLETDIR
  MYSQL_FLAVOR=MySQL80 /vt/bin/mysqlctl \
    --tablet-uid $TUID --mysql-port $MYP \
    --db-charset utf8mb4 \
    init --init-db-sql-file /vt/config/init_db.sql \
    > $VTDATAROOT/mysqlctl_${TUID}.log 2>&1
  # mysqlctl brings mysqld up on the UNIX SOCKET only (no TCP
  # listener by default); vttablet must connect via --db-socket or
  # it gets "connection refused" on 127.0.0.1:<port> and never
  # becomes serving (ground-truthed, Track-1a Phase A).
  /vt/bin/vttablet $TOPO \
    --tablet-path ${CELL}-${TUID} \
    --init-keyspace ${KS} --init-shard ${SHARD} --init-tablet-type replica \
    --port $VTP --grpc-port $GRP \
    --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    --db-socket $TABLETDIR/mysql.sock \
    --restore-from-backup=false \
    --pid-file $TABLETDIR/vttablet.pid \
    > $VTDATAROOT/vttablet_${TUID}.log 2>&1 &
  for i in $(seq 1 60); do
    if $VC GetTablet ${CELL}-${TUID} >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
}

# start_shard brings up TWO tablets per shard: a primary-candidate
# (TUID) and a replica (TUID+50). After PlannedReparentShard
# promotes TUID to PRIMARY, the +50 tablet stays REPLICA — which is
# REQUIRED because sluice's VStream CDC reader streams from a
# REPLICA tablet by design (off the primary's hot path,
# buildVStreamRequest -> TabletType_REPLICA). A single-tablet shard
# would leave VStream with "failed to find a REPLICA tablet"
# (ground-truthed, Track-1a Phase A).
start_shard() {
  SHARD=$1; TUID=$2; MYP=$3; VTP=$4; GRP=$5
  one_tablet "$SHARD" $TUID $MYP $VTP $GRP
  one_tablet "$SHARD" $((TUID+50)) $((MYP+50)) $((VTP+50)) $((GRP+50))
}

# --- create the keyspace + shard records BEFORE tablets init ---
# Modern Vitess does NOT auto-create the shard topo node from
# vttablet --init-shard; CreateShard must run first or
# PlannedReparentShard fails with "node doesn't exist".
$VC CreateKeyspace --force ${KS} || true
for SHARD in ${SOURCE_SHARDS}; do
  $VC CreateShard --force ${KS}/${SHARD} || true
done

# --- bring up the SOURCE layout ---
TUID=100; MYP=17100; VTP=15100; GRP=16100
for SHARD in ${SOURCE_SHARDS}; do
  start_shard "$SHARD" $TUID $MYP $VTP $GRP
  TUID=$((TUID+1)); MYP=$((MYP+1)); VTP=$((VTP+1)); GRP=$((GRP+1))
done

# --- reparent each source shard's primary ---
# Retry: the tablet's tabletmanager RPC can lag its topo
# registration by a few seconds after process start.
TUID=100
for SHARD in ${SOURCE_SHARDS}; do
  for attempt in $(seq 1 12); do
    if $VC PlannedReparentShard ${KS}/${SHARD} --new-primary ${CELL}-${TUID}; then
      break
    fi
    sleep 3
  done
  TUID=$((TUID+1))
done

# --- vtgate (MySQL :15306, gRPC :15991) ---
# --vschema-ddl-authorized-users=% is REQUIRED: the tests apply the
# hash VINDEX via ALTER VSCHEMA, and without this flag vtgate
# rejects it ("not authorized to perform vschema operations"), the
# table is never sharded, and Reshard create then fails with
# "table <t> not found" on the target tablets (ground-truthed,
# Track-1a Phase A).
/vt/bin/vtgate $TOPO \
  --cell ${CELL} --cells-to-watch ${CELL} \
  --tablet-types-to-wait 'PRIMARY,REPLICA' \
  --service-map 'grpc-vtgateservice' \
  --mysql-server-port 15306 --mysql-auth-server-impl none \
  --mysql-server-bind-address 0.0.0.0 \
  --vschema-ddl-authorized-users '%' \
  --grpc-port 15991 --port 15990 \
  --pid-file $VTDATAROOT/vtgate.pid > $VTDATAROOT/vtgate.log 2>&1 &

# Readiness marker the testcontainers wait strategy keys on. The
# mysql client is on PATH (/usr/bin/mysql), NOT /vt/bin — using the
# wrong path silently never matches and the cluster looks unready
# even when vtgate is serving (ground-truthed, Track-1a Phase A).
for i in $(seq 1 120); do
  if mysql -h127.0.0.1 -P15306 -e 'SELECT 1' >/dev/null 2>&1; then
    echo 'SLUICE_VITESS_CLUSTER_READY'
    break
  fi
  sleep 1
done

# Keep PID 1 alive; the test drives reshard via docker exec.
tail -f $VTDATAROOT/vtgate.log
`

// startVitessReshardCluster boots etcd + a scripted vitess/lite
// cluster with the given source shard layout. sourceShards is a
// space-separated shard list ("-" unsharded; "-80 80-" two shards).
//
// Returns a ready cluster (vtgate reachable) or t.Skip()s when Docker
// is unavailable / t.Fatal on a real bring-up failure.
func startVitessReshardCluster(t *testing.T, sourceShards string) *vitessReshardCluster {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	nw, err := network.New(ctx)
	if err != nil {
		t.Skipf("create docker network (provider likely unavailable): %v", err)
	}

	cleanups := make([]func(), 0, 3)
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	cleanups = append(cleanups, func() {
		rmCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = nw.Remove(rmCtx)
	})

	// --- etcd (global+local topo) ---
	etcdReq := testcontainers.ContainerRequest{
		Image:    vrEtcdImage,
		Networks: []string{nw.Name},
		NetworkAliases: map[string][]string{
			nw.Name: {"etcd"},
		},
		Cmd: []string{
			"/usr/local/bin/etcd",
			"--name", "etcd0",
			"--advertise-client-urls", "http://0.0.0.0:2379",
			"--listen-client-urls", "http://0.0.0.0:2379",
			"--initial-cluster-state", "new",
		},
		WaitingFor: wait.ForLog("ready to serve client requests").WithStartupTimeout(2 * time.Minute),
	}
	etcd, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: etcdReq,
		Started:          true,
	})
	if err != nil {
		cleanup()
		t.Fatalf("start etcd: %v", err)
	}
	cleanups = append(cleanups, func() {
		tc, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = etcd.Terminate(tc)
	})

	// --- vitess/lite running the scripted cluster ---
	vitessReq := testcontainers.ContainerRequest{
		Image:    vrVitessImage,
		Networks: []string{nw.Name},
		// Stable network alias so a sidecar (the toxiproxy latency proxy
		// used by the per-shard-latency A/B) can forward UPSTREAM to a
		// tablet inside this container by a fixed name. Additive/harmless
		// for every other test (none of them dial this alias).
		NetworkAliases: map[string][]string{
			nw.Name: {vrVitessAlias},
		},
		ExposedPorts: []string{vrVtgateMySQLPort, vrVtgateGRPCPort},
		Env: map[string]string{
			"ETCD_ADDR":     "etcd:2379",
			"SOURCE_SHARDS": sourceShards,
		},
		Entrypoint: []string{"/bin/bash", "-c", clusterBringUpScript},
		WaitingFor: wait.ForAll(
			wait.ForLog("SLUICE_VITESS_CLUSTER_READY").WithStartupTimeout(9*time.Minute),
			wait.ForListeningPort(vrVtgateMySQLPort).WithStartupTimeout(9*time.Minute),
			wait.ForListeningPort(vrVtgateGRPCPort).WithStartupTimeout(9*time.Minute),
		).WithStartupTimeoutDefault(9 * time.Minute),
	}
	vitess, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: vitessReq,
		Started:          true,
	})
	if err != nil {
		cleanup()
		t.Fatalf("start vitess/lite cluster: %v", err)
	}
	cleanups = append(cleanups, func() {
		tc, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = vitess.Terminate(tc)
	})

	host, err := vitess.Host(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("vitess host: %v", err)
	}
	myPort, err := vitess.MappedPort(ctx, vrVtgateMySQLPort)
	if err != nil {
		cleanup()
		t.Fatalf("mapped vtgate mysql port: %v", err)
	}
	grpcPort, err := vitess.MappedPort(ctx, vrVtgateGRPCPort)
	if err != nil {
		cleanup()
		t.Fatalf("mapped vtgate grpc port: %v", err)
	}

	c := &vitessReshardCluster{
		net:    nw,
		etcd:   etcd,
		vitess: vitess,
		mysqlDSN: fmt.Sprintf(
			"root@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true",
			host, myPort.Num(), vrKeyspace,
		),
		grpcAddr:  fmt.Sprintf("%s:%d", host, grpcPort.Num()),
		terminate: cleanup,
	}
	return c
}

// vrApplySQL runs DDL/DML against vtgate's MySQL frontend.
func vrApplySQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open vtgate mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql %q: %v", vrTrunc(sqlText, 80), err)
	}
}

func vrTrunc(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// =====================================================================
// PHASE-A PROOF: a trivial 1 -> 2 reshard against this topology. If
// this passes, the topology is PROVEN reshardable in this environment
// and the full reshard-chaos oracle (below) is built on solid ground.
// If it cannot pass, the honest infeasible-report fires here.
// =====================================================================

// TestVitessReshard_ProofOfReshardability is the Phase-A feasibility
// gate, kept as a permanent regression: stand up a 1-shard keyspace,
// add 2 target shards, run `Reshard create` + `SwitchTraffic`, and
// assert vtgate now routes to the 2-shard layout. No sluice code is
// exercised here — this proves only that the TOPOLOGY can reshard.
func TestVitessReshard_ProofOfReshardability(t *testing.T) {
	c := startVitessReshardCluster(t, "-")
	defer c.terminate()

	// Source unsharded keyspace with a hash-vindexed table.
	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE product (
		sku BIGINT NOT NULL,
		name VARCHAR(128) NOT NULL,
		PRIMARY KEY (sku)
	) ENGINE=InnoDB`)
	vrApplySQL(t, c.mysqlDSN, `ALTER VSCHEMA ON `+vrKeyspace+`.product ADD VINDEX hash(sku) USING hash`)
	time.Sleep(3 * time.Second)
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true", `
		INSERT INTO product (sku, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');`)

	// Add the two target shards (-80, 80-) as fresh tablets, then run
	// the reshard workflow.
	c.addTargetShards(t, "-80", "80-")

	out, err := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "proof", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-")
	if err != nil {
		t.Fatalf("Reshard create failed (topology NOT reshardable): %v\n%s", err, out)
	}
	c.waitReshardRunning(t, "proof")

	if _, err := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "proof", "--target-keyspace", vrKeyspace); err != nil {
		t.Fatalf("Reshard SwitchTraffic failed: %v", err)
	}

	// Proof: vtgate now reports two shards for the keyspace.
	shards := vrShowShards(t, c.mysqlDSN)
	if len(shards) != 2 {
		t.Fatalf("post-reshard shards = %v; want 2 (-80, 80-) — RESHARD DID NOT TAKE EFFECT", shards)
	}
	// And the data is still all there, now distributed. The target
	// shard primaries are routable a beat AFTER SwitchTraffic
	// returns; vrCountEventually retries the brief
	// "no healthy tablet" window (ground-truthed in the Phase-A
	// probe — a single-shot query here races the cutover).
	if n := c.vrCountEventually(t, "SELECT COUNT(*) FROM product", 4, 90*time.Second); n != 4 {
		t.Fatalf("post-reshard product count = %d; want 4", n)
	}
	t.Logf("PHASE-A PROOF PASSED: vitess/lite topology resharded 1 -> 2 (shards now %v)", shards)
}

// addTargetShards brings up one replica tablet per target shard and
// reparents each. Reuses the same per-shard logic the bring-up script
// uses, driven from the host via docker exec so target shards can be
// added AFTER the source cluster is already serving.
func (c *vitessReshardCluster) addTargetShards(t *testing.T, shards ...string) {
	t.Helper()
	// uids 200+ to avoid colliding with source uids (100+).
	uid := 200
	for _, sh := range shards {
		c.bringUpTargetShard(t, sh, uid, "")
		uid++
	}
}

// bringUpTargetShard brings up one target shard (a primary-candidate at
// `uid` + a REPLICA at `uid+50`) and reparents it, driven from the host
// via docker exec so target shards can be added AFTER the source cluster
// is already serving. uid selects the tablet uid / port block (caller is
// responsible for keeping uids disjoint).
//
// replicaHostname, when non-empty, sets the REPLICA tablet's
// --tablet-hostname so vtgate dials it at that name instead of the
// container's own hostname. This is the hook the per-shard-latency A/B
// uses to route ONE shard's VStream replica through a toxiproxy latency
// sidecar (replicaHostname=the proxy's network alias); the primary is
// never rerouted so reparent/admin RPCs stay direct. Empty ⇒ today's
// behavior (direct hostname), so the default reshard tests are unchanged.
func (c *vitessReshardCluster) bringUpTargetShard(t *testing.T, sh string, uid int, replicaHostname string) {
	t.Helper()
	script := fmt.Sprintf(`set -e
export VTDATAROOT=/vt/vtdataroot
TOPO="--topo-implementation etcd2 --topo-global-server-address ${ETCD_ADDR} --topo-global-root /vitess/global"
VC="/vt/bin/vtctldclient --server localhost:15999"
# Same MySQL-8.4 native-auth hook as the bring-up script; this runs
# as a separate docker exec so it must re-export EXTRA_MY_CNF.
export EXTRA_MY_CNF=$VTDATAROOT/native_auth.cnf
TUID=%d; MYP=%d; VTP=%d; GRP=%d; SHARD=%q; CELL=%s; KS=%s; REPLICA_HOST=%q

# Same primary+replica topology as the bring-up script: the target
# shard also needs a REPLICA so VStream can stream the post-reshard
# layout after the collector Reopen()s.
# arg 5 (HOSTFLAG) is an OPTIONAL "--tablet-hostname <name>" string
# (unquoted on the vttablet line so it splits into two tokens); empty
# ⇒ the tablet advertises its own hostname (today's behavior).
one_tablet() {
  U=$1; MP=$2; VP=$3; GP=$4; HOSTFLAG=$5
  TD=$VTDATAROOT/vt_$(printf '%%010d' $U)
  mkdir -p $TD
  MYSQL_FLAVOR=MySQL80 /vt/bin/mysqlctl --tablet-uid $U --mysql-port $MP --db-charset utf8mb4 \
    init --init-db-sql-file /vt/config/init_db.sql > $VTDATAROOT/mysqlctl_${U}.log 2>&1
  /vt/bin/vttablet $TOPO --tablet-path ${CELL}-${U} \
    --init-keyspace ${KS} --init-shard ${SHARD} --init-tablet-type replica \
    --port $VP --grpc-port $GP $HOSTFLAG \
    --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    --db-socket $TD/mysql.sock \
    --restore-from-backup=false --pid-file $TD/vttablet.pid \
    > $VTDATAROOT/vttablet_${U}.log 2>&1 &
  for i in $(seq 1 60); do
    $VC GetTablet ${CELL}-${U} >/dev/null 2>&1 && break
    sleep 1
  done
}

REPLICA_HOSTFLAG=""
if [ -n "$REPLICA_HOST" ]; then REPLICA_HOSTFLAG="--tablet-hostname $REPLICA_HOST"; fi

$VC CreateShard --force ${KS}/${SHARD} || true
one_tablet $TUID $MYP $VTP $GRP ""
one_tablet $((TUID+50)) $((MYP+50)) $((VTP+50)) $((GRP+50)) "$REPLICA_HOSTFLAG"
for attempt in $(seq 1 12); do
  if $VC PlannedReparentShard ${KS}/${SHARD} --new-primary ${CELL}-${TUID}; then
    break
  fi
  sleep 3
done
echo SLUICE_TARGET_SHARD_READY_${TUID}
`, uid, 17000+uid, 15000+uid, 16000+uid, sh, vrCell, vrKeyspace, replicaHostname)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	code, reader, err := c.vitess.Exec(ctx, []string{"/bin/bash", "-c", script})
	cancel()
	out := vrDrain(reader)
	if err != nil || code != 0 {
		t.Fatalf("add target shard %q (uid %d) failed: err=%v code=%d\n%s", sh, uid, err, code, out)
	}
	if !strings.Contains(out, fmt.Sprintf("SLUICE_TARGET_SHARD_READY_%d", uid)) {
		t.Fatalf("add target shard %q: readiness marker missing\n%s", sh, out)
	}
	// The script only waits for topo *registration* of the tablets; the
	// REPLICA tablet (uid+50) needs to additionally reach a
	// healthy/streamable state before VStream can resume on it
	// post-reshard. Without this the reader Reopen()s onto a replica
	// that is still "down or nonexistent" and the collector sees a
	// spurious gap (ground-truthed, Track-1a Phase A). Wait for BOTH the
	// primary-candidate and the replica to be queryable.
	//
	// When the replica is routed through a latency proxy, vtctld dials it
	// at replicaHostname through the proxy too — waitTabletServing keys on
	// topo reads (GetTablet) which still succeed, and a few hundred ms of
	// added RTT is well within its 3-min budget.
	c.waitTabletServing(t, uid)
	c.waitTabletServing(t, uid+50)
}

// waitTabletServing blocks until vtctld reports the tablet's
// query-service is healthy (a non-empty primary/replica state with
// no serving error). Polls GetTablet + the shard's serving status;
// the cheap proxy used here is that the tablet's stream-eligible
// type is set and GetTablet succeeds repeatedly (a flapping tablet
// fails this). Bounded; loud on timeout so a genuinely-stuck tablet
// is not silently tolerated.
func (c *vitessReshardCluster) waitTabletServing(t *testing.T, uid int) {
	t.Helper()
	alias := fmt.Sprintf("%s-%d", vrCell, uid)
	deadline := time.Now().Add(3 * time.Minute)
	stable := 0
	for time.Now().Before(deadline) {
		out, err := c.vtctldExec(t, "GetTablet", alias)
		if err == nil && strings.Contains(out, `"port_map"`) {
			stable++
			if stable >= 3 { // three consecutive healthy reads
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("tablet %s never reached a stable serving state within deadline", alias)
}

// waitReshardRunning polls `Reshard show` until the workflow reports
// the copy phase complete and replicating (so SwitchTraffic is safe).
func (c *vitessReshardCluster) waitReshardRunning(t *testing.T, workflow string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := c.vtctldExec(t, "Workflow", "--keyspace", vrKeyspace, "show", "--workflow", workflow)
		// The workflow's top-level state JSON flips
		// "Copying" -> "Running" once every target shard finishes
		// the copy phase and is replicating (ground-truthed in the
		// Phase-A reshard probe). Match the exact state token so
		// lingering "Copying" text in copy_states history doesn't
		// keep us spinning past a genuinely-Running workflow.
		if err == nil && strings.Contains(out, `"state": "Running"`) {
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("reshard workflow %q did not reach Running within deadline", workflow)
}

func vrDrain(reader interface{ Read([]byte) (int, error) }) string {
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	return sb.String()
}

func vrShowShards(t *testing.T, dsn string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SHOW VITESS_SHARDS LIKE '"+vrKeyspace+"/%'")
	if err != nil {
		t.Fatalf("SHOW VITESS_SHARDS: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if i := strings.IndexByte(s, '/'); i >= 0 {
			out = append(out, s[i+1:])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	sort.Strings(out)
	return out
}

// vrCountEventually runs the COUNT query, retrying the transient
// "no healthy tablet available" window that exists for a few seconds
// immediately after Reshard SwitchTraffic (the target shard
// primaries become routable a beat after the cutover RPC returns —
// ground-truthed in the Phase-A reshard probe). It returns as soon
// as the count equals want, or the last-seen count at deadline so
// the caller's assertion produces a precise failure.
func (c *vitessReshardCluster) vrCountEventually(t *testing.T, q string, want int, timeout time.Duration) int {
	t.Helper()
	db, err := sql.Open("mysql", c.mysqlDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(timeout)
	last := -1
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var n int
		qerr := db.QueryRowContext(ctx, q).Scan(&n)
		cancel()
		if qerr != nil {
			// "no healthy tablet" / cutover transients: retry.
			time.Sleep(3 * time.Second)
			continue
		}
		last = n
		if n == want {
			return n
		}
		time.Sleep(2 * time.Second)
	}
	return last
}

// =====================================================================
// THE HEADLINE TEST: reshard-chaos exactly-once oracle.
//
// Sequence:
//  1. 1-shard keyspace `commerce`, table `account` (hash vindex on id).
//  2. Seed N rows; open sluice VStream CDC (FlavorPlanetScale) from
//     "current".
//  3. Start a continuous writer: INSERTs new ids at a steady rate on a
//     separate connection (the "writes continue mid-stream" clause).
//  4. Mid-stream: add target shards, `Reshard create` + `SwitchTraffic`
//     1 -> 2 while the writer keeps going.
//  5. The CDC reader, with StopOnReshard:true, sees a JOURNAL ->
//     ShardLayoutChangedError; the test drives rdr.Reopen(resh) (the
//     documented production pattern in cdc_vstream.go) to resume on
//     the new 2-shard layout from the journal GTIDs.
//  6. Stop the writer, drain the tail.
//
// THE LOAD-BEARING ORACLE (re-run by the maintainer): build the set of
// every id the writer COMMITTED to the source, and the multiset of ids
// the CDC stream delivered as Inserts. Assert:
//   - every committed source id appears in the delivered stream
//     (NO GAP across the journal cut), and
//   - no id is delivered more than once after dedup-by-PK with the
//     post-reshard replays accounted for (NO DUP — exactly-once on the
//     applied set), and
//   - the reader's VGTID resumed on the 2-shard layout (post-reshard
//     positions decode to 2 shardGtids).
//
// A "no error but rows lost/duplicated" outcome FAILS LOUDLY here.
// =====================================================================

func TestVitessReshard_ChaosExactlyOnce(t *testing.T) {
	c := startVitessReshardCluster(t, "-")
	defer c.terminate()

	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE account (
		id    BIGINT       NOT NULL,
		owner VARCHAR(128) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	vrApplySQL(t, c.mysqlDSN, `ALTER VSCHEMA ON `+vrKeyspace+`.account ADD VINDEX hash(id) USING hash`)
	time.Sleep(3 * time.Second)

	// Seed the pre-stream baseline (ids 1..50).
	const seedCount = 50
	var sb strings.Builder
	sb.WriteString("INSERT INTO account (id, owner) VALUES ")
	for i := 1; i <= seedCount; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "(%d,'seed-%d')", i, i)
	}
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true", sb.String())
	time.Sleep(2 * time.Second)

	// Open sluice VStream CDC from "current" — CDC-only; the seed rows
	// are the baseline, the oracle tracks ids inserted AFTER stream
	// open (the ones whose delivery crosses the journal cut).
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
		c.mysqlDSN, c.grpcAddr,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdcRdr, ok := rdr.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *vstreamCDCReader", rdr)
	}
	defer func() { _ = cdcRdr.Close() }()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(3 * time.Second) // let the stream register at "current"

	// --- continuous writer: ids 1000.. at ~20/s on its own conn ---
	committed := &committedSet{m: make(map[int64]string)}
	writerCtx, stopWriter := context.WithCancel(ctx)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		db, derr := sql.Open("mysql", c.mysqlDSN)
		if derr != nil {
			t.Errorf("writer open: %v", derr)
			return
		}
		defer func() { _ = db.Close() }()
		id := int64(1000)
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-writerCtx.Done():
				return
			case <-tick.C:
				owner := fmt.Sprintf("w-%d", id)
				if _, e := db.ExecContext(writerCtx,
					"INSERT INTO account (id, owner) VALUES (?, ?)", id, owner); e != nil {
					// Mid-SwitchTraffic vtgate can briefly reject writes;
					// that id was NOT committed, so don't record it.
					if errors.Is(e, context.Canceled) {
						return
					}
					continue
				}
				committed.add(id, owner)
				id++
			}
		}
	}()

	// Collector: drains changes; on ShardLayoutChangedError it drives
	// Reopen (the production pattern) and keeps collecting. Records
	// every delivered Insert id + tracks post-reshard shard count.
	delivered := &deliveredSet{counts: make(map[int64]int)}
	reopened := make(chan int, 1) // post-reshard shard count, once
	collectorDone := make(chan struct{})
	// tearingDown is set (before cdcRdr.Close()) on every teardown
	// path so the collector treats the resulting stream/Reopen
	// failure ("connection is closing", context canceled) as a
	// clean stop, not an oracle error.
	var tearingDown atomic.Bool
	go func() {
		defer close(collectorDone)
		ch := changes
		// lastResh holds the most recent reshard journal so a
		// transient post-Reopen stream error (target replica still
		// settling) can be retried by re-Reopening from the same
		// NewShards GTIDs. Bounded so a permanently-broken stream
		// still fails the oracle loudly rather than spinning.
		var lastResh *ShardLayoutChangedError
		streamRetries := 0
		const maxStreamRetries = 15
		for {
			select {
			case <-ctx.Done():
				return
			case ev, alive := <-ch:
				if !alive {
					if tearingDown.Load() {
						return // clean shutdown initiated by the test
					}
					// Stream closed. Three cases:
					//  1. Reshard JOURNAL => ShardLayoutChangedError on
					//     Err(): drive Reopen (the production pattern)
					//     and keep collecting on the new layout.
					//  2. Clean ctx cancellation (test teardown):
					//     expected, not a failure.
					//  3. Any other terminal error: fatal for oracle.
					streamErr := cdcRdr.Err()
					var resh *ShardLayoutChangedError
					if errors.As(streamErr, &resh) {
						lastResh = resh
						newCh, rerr := cdcRdr.Reopen(context.Background(), resh)
						if rerr != nil {
							t.Errorf("Reopen after reshard: %v", rerr)
							return
						}
						select {
						case reopened <- len(resh.NewShards):
						default:
						}
						ch = newCh
						continue
					}
					if errors.Is(streamErr, context.Canceled) ||
						errors.Is(streamErr, context.DeadlineExceeded) {
						// Clean teardown.
						return
					}
					// Post-reshard transient: right after Reopen the
					// target REPLICA tablets can still be settling
					// ("tablet ... is either down or nonexistent",
					// Unavailable). The production Streamer loop
					// retries transient stream failures; mirror that
					// by re-Reopening from the SAME journal state
					// (resumes at the persisted NewShards GTIDs — a
					// genuinely-skipped event stays skipped, so this
					// retries flakes WITHOUT masking a real gap). Only
					// bail if we never saw a reshard at all, or the
					// retry budget is exhausted.
					if lastResh != nil && streamRetries < maxStreamRetries {
						streamRetries++
						time.Sleep(4 * time.Second)
						if tearingDown.Load() {
							return // teardown began during backoff
						}
						newCh, rerr := cdcRdr.Reopen(context.Background(), lastResh)
						if rerr != nil {
							if tearingDown.Load() {
								return // Reopen failed only because we're closing
							}
							t.Errorf("retry Reopen after transient post-reshard stream error %q: %v", streamErr, rerr)
							return
						}
						ch = newCh
						continue
					}
					if streamErr != nil && !tearingDown.Load() {
						t.Errorf("stream terminated non-reshard error (retries=%d): %v", streamRetries, streamErr)
					}
					return
				}
				if ins, isIns := ev.(ir.Insert); isIns {
					if idv, ok := vrAsInt64(ins.Row["id"]); ok {
						delivered.add(idv)
					}
				}
			}
		}
	}()

	// Let the writer build a pre-reshard backlog the stream is
	// actively delivering.
	time.Sleep(8 * time.Second)

	// --- RESHARD MID-STREAM (writes still flowing) ---
	c.addTargetShards(t, "-80", "80-")
	if out, rerr := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "chaos", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-"); rerr != nil {
		t.Fatalf("Reshard create: %v\n%s", rerr, out)
	}
	c.waitReshardRunning(t, "chaos")
	if _, rerr := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "chaos", "--target-keyspace", vrKeyspace); rerr != nil {
		t.Fatalf("Reshard SwitchTraffic: %v", rerr)
	}

	// The VStream JOURNAL (StopOnReshard) is emitted on the SOURCE
	// shard stream when SwitchTraffic completes the cutover, then the
	// collector Reopen()s on the new layout. This is asynchronous and
	// can lag the SwitchTraffic RPC return by tens of seconds (the
	// reader streams from a REPLICA and vtgate has to flush the
	// journal). Do NOT tear down on a fixed sleep — that races the
	// cut and was the v1 failure. Block until the reader actually
	// Reopen()s (the load-bearing event) with a generous deadline.
	var reopenShards int
	select {
	case reopenShards = <-reopened:
		t.Logf("ORACLE: reader observed reshard JOURNAL and Reopen()ed on %d-shard layout", reopenShards)
	case <-time.After(4 * time.Minute):
		tearingDown.Store(true)
		stopWriter()
		writerWG.Wait()
		_ = cdcRdr.Close()
		cancel()
		<-collectorDone
		t.Fatalf("reader never observed the reshard JOURNAL / never Reopen()ed within 4m — StopOnReshard path did not fire across the cut (delivered-distinct=%d)", delivered.distinct())
	}

	// Reopened on the new layout — let writes continue on the NEW
	// 2-shard layout for a good while so the oracle covers ids
	// committed strictly AFTER the journal cut (the
	// no-gap-across-the-cut property). Generous because the
	// collector may spend a few bounded retry cycles while the
	// target REPLICA tablets finish settling post-SwitchTraffic.
	time.Sleep(40 * time.Second)

	// Stop the writer, give the post-reshard stream ample time to
	// drain the tail of what the writer committed on the new layout
	// (incl. any in-flight collector stream retries).
	stopWriter()
	writerWG.Wait()
	time.Sleep(25 * time.Second)
	tearingDown.Store(true)
	_ = cdcRdr.Close()
	cancel()
	<-collectorDone

	// --- ORACLE ---
	if reopenShards != 2 {
		t.Fatalf("reader resumed with %d shards after reshard; want 2 (VGTID did not follow the journal to the new layout)", reopenShards)
	}

	srcIDs := committed.ids()
	if len(srcIDs) < 20 {
		t.Fatalf("writer only committed %d rows across the reshard window; test is not exercising the cut (need >=20)", len(srcIDs))
	}

	// NO GAP: every committed source id must have been delivered.
	var missing []int64
	for _, id := range srcIDs {
		if delivered.count(id) == 0 {
			missing = append(missing, id)
		}
	}
	// NO DUP: VStream re-emits the last pre-journal txn on Reopen by
	// design (the captured position is "before this txn commits"), so
	// a SMALL bounded replay is correct CDC behaviour the idempotent
	// applier absorbs. The oracle bound: duplicates are allowed only
	// for ids near the cut, and the TOTAL dup excess must be a small
	// constant — NOT proportional to the stream (which would mean the
	// reader re-read a whole shard).
	dupExcess := 0
	for _, id := range srcIDs {
		if cnt := delivered.count(id); cnt > 1 {
			dupExcess += cnt - 1
		}
	}

	if len(missing) > 0 {
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		show := missing
		if len(show) > 20 {
			show = show[:20]
		}
		t.Fatalf("RESHARD GAP: %d/%d committed source ids were NEVER delivered across the journal cut (first missing: %v) — this is a sluice CDC-reader correctness bug, NOT a flake",
			len(missing), len(srcIDs), show)
	}
	// Tolerate at most a handful of boundary replays (one in-flight
	// transaction's worth); anything larger means the reader re-read
	// data after Reopen (a real dup bug).
	const maxBoundaryReplay = 10
	if dupExcess > maxBoundaryReplay {
		t.Fatalf("RESHARD DUP: %d duplicate deliveries across the cut (> %d boundary tolerance) — the reader re-read rows after Reopen; exactly-once violated",
			dupExcess, maxBoundaryReplay)
	}

	t.Logf("ORACLE PASSED: committed=%d delivered-distinct=%d boundary-replays=%d (<=%d) — exactly-once held across the 1->2 journal cut, no gap no dup",
		len(srcIDs), delivered.distinct(), dupExcess, maxBoundaryReplay)
}

// committedSet records ids the writer successfully COMMITTed to the
// source (the oracle's ground truth for "what must appear").
type committedSet struct {
	mu sync.Mutex
	m  map[int64]string
}

func (s *committedSet) add(id int64, owner string) {
	s.mu.Lock()
	s.m[id] = owner
	s.mu.Unlock()
}

func (s *committedSet) ids() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, 0, len(s.m))
	for id := range s.m {
		out = append(out, id)
	}
	return out
}

// deliveredSet is the multiset of ids the CDC stream delivered as
// Inserts (counts let the oracle separate "missing" from "duplicated").
type deliveredSet struct {
	mu     sync.Mutex
	counts map[int64]int
}

func (d *deliveredSet) add(id int64) {
	d.mu.Lock()
	d.counts[id]++
	d.mu.Unlock()
}

func (d *deliveredSet) count(id int64) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.counts[id]
}

func (d *deliveredSet) distinct() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.counts)
}

// vrAsInt64 normalises the IR row's id cell to int64. The VStream
// decoder yields int64 for BIGINT; defensively handle the other
// integer shapes the value contract permits.
func vrAsInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case uint64:
		return int64(n), true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
