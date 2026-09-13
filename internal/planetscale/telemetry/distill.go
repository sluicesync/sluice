// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// metricNames is the per-engine PlanetScale metric-name table. The CPU/mem
// percentage metrics are engine-shared; storage, lag, and connection metric
// names differ between the Vitess/MySQL surface and the Postgres surface
// (ADR-0107 Phase 3). An empty field means "this engine exposes no metric we
// can read for this signal" — the corresponding snapshot *Known flag then
// stays false (the honest "unobserved" degrade), never a wrong reading.
type metricNames struct {
	cpuUtilPct       string
	memUtilPct       string
	volAvailableByte string
	volCapacityByte  string
	replicaLagSec    string
	activeConns      string
	maxConns         string

	// primaryContainer, when non-empty, is the `planetscale_container` label
	// value that identifies the write-target's primary series among MULTIPLE
	// container series of a per-pod metric (cpu/mem). Postgres emits cpu/mem
	// once per container (`postgres`, `pgbouncer`, `walg-daemon`) under
	// `planetscale_component="hzinstance"` with NO `tablet_type` — so the
	// Vitess vttablet/primary cascade can't pick one and (with >1 series) the
	// single-series fallback refuses, silently dropping CPU/mem on PG targets
	// (live-confirmed 2026-06-23). This selects the `postgres` container.
	// Empty (MySQL/Vitess) ⇒ the vttablet/primary cascade as before.
	primaryContainer string

	// The FRONT-DOOR names, for a platform whose front door is a POOLER
	// SIDECAR rather than a fleet of router pods. PlanetScale Postgres runs
	// PgBouncer as a container on each database pod; PlanetScale Neki runs
	// separate router pods identified by [labelRouter].
	//
	// Both shapes are described by ONE metricNames table, because Neki IS
	// Postgres by engine and so reads [postgresMetricNames]. That is safe
	// because the two surfaces are DISJOINT, verified live 2026-09-13 on two
	// real branches of the same org: the Neki exposition contains the string
	// "pgbouncer" zero times, and the PlanetScale Postgres exposition carries
	// `planetscale_router` on zero series. So the selectors below can be
	// tried in either order and at most one can answer. If that ever stops
	// holding, [TestFrontDoorSurfacesAreDisjoint] fails.
	//
	//   - poolerCPUPct is the pooler's OWN cpu metric, not the per-container
	//     slice of the pod metric. PgBouncer is single-threaded per peer
	//     process, so "this peer is at 100%" is saturation even while the pod
	//     it lives on is nearly idle; the pod-level container reading would
	//     understate exactly the condition worth alerting on.
	//   - poolerContainer picks the pooler's MEMORY out of the per-container
	//     pods metric, because the platform publishes no pgbouncer-specific
	//     memory series.
	//   - frontDoorWaitSec is the queue wait — clients admitted but not yet
	//     handed a backend connection.
	poolerCPUPct     string
	poolerContainer  string
	frontDoorWaitSec string
}

// mysqlMetricNames is the MySQL/Vitess table, CONFIRMED against the live
// sluicesync endpoint 2026-06-21 (see ADR-0107 impl-plan §2a).
var mysqlMetricNames = metricNames{
	cpuUtilPct:       "planetscale_pods_cpu_util_percentages",
	memUtilPct:       "planetscale_pods_mem_util_percentages",
	volAvailableByte: "planetscale_vttablet_volume_available_bytes",
	volCapacityByte:  "planetscale_vttablet_volume_capacity_bytes",
	replicaLagSec:    "planetscale_mysql_replica_lag_seconds",
	activeConns:      "planetscale_edge_active_connections",
	maxConns:         "planetscale_mysql_max_connections",
}

// postgresMetricNames is the Postgres table (ADR-0107 Phase 3). CPU/mem are
// the engine-shared pod metrics; storage drops the vttablet-specific prefix
// (`planetscale_volume_*`); lag is the postgres-specific series. Connection
// metrics are left UNSET on purpose: the documented PG connection surface
// (`planetscale_postgres_connection_state`) is a per-state breakdown that
// does not fit the single-value-per-pod selection [selectPrimaryValue] uses,
// and — unlike the MySQL names — it has not been verified against the live
// endpoint, so reading it could silently mis-sum. ConnKnown therefore stays
// false for PG targets until the shape is confirmed live (a tracked
// follow-up). Connections are a SECONDARY signal (operator priority is
// CPU/mem/storage), so this is a clean, honest gap rather than a blocker.
var postgresMetricNames = metricNames{
	cpuUtilPct:       "planetscale_pods_cpu_util_percentages",
	memUtilPct:       "planetscale_pods_mem_util_percentages",
	volAvailableByte: "planetscale_volume_available_bytes",
	volCapacityByte:  "planetscale_volume_capacity_bytes",
	// All three CONFIRMED live 2026-07-24. Each was previously left unset on
	// the strength of a 2026-06-23 probe that is no longer accurate — the
	// endpoint has since grown them:
	//
	//   - replicaLagSec: `planetscale_postgres_replica_lag_seconds` DOES now
	//     exist ("Replica lag in fine-grained seconds from Postgres"), one
	//     gauge per replica pod. The old comment asserted it did not.
	//   - activeConns: `planetscale_edge_postgres_active_connections`, a
	//     single unlabelled series (so the single-series tier resolves it).
	//   - maxConns: `planetscale_postgres_settings_max_connections`, one
	//     series per pod carrying planetscale_role — resolvable since the
	//     role tier was added. This is NOT the per-state
	//     `planetscale_postgres_connection_state` breakdown the old comment
	//     worried about mis-summing; it is a flat setting value.
	replicaLagSec:    "planetscale_postgres_replica_lag_seconds",
	activeConns:      "planetscale_edge_postgres_active_connections",
	maxConns:         "planetscale_postgres_settings_max_connections",
	primaryContainer: "postgres",
	// The PgBouncer front door, CONFIRMED live 2026-09-13 against a real
	// PlanetScale Postgres branch (all three present, one series per pod,
	// tagged planetscale_container="pgbouncer" and planetscale_role).
	//
	// UNVERIFIED PREMISE, stated rather than assumed: the branch measured ran
	// `planetscale_pgbouncer_total_peers = 1` on every pod, so whether
	// `_per_peer_percentages` averages or maxes across MULTIPLE peers could
	// not be determined from it. With one peer the two agree. If a branch
	// ever runs several peers and the metric turns out to average, a single
	// saturated peer would read low here — the reading would be conservative,
	// never alarmist, which is why this is recorded as a caveat rather than
	// treated as a blocker.
	poolerCPUPct:     "planetscale_pgbouncer_cpu_util_per_peer_percentages",
	poolerContainer:  "pgbouncer",
	frontDoorWaitSec: "planetscale_pgbouncer_pools_client_maxwait_seconds",
}

// metricNamesFor selects the metric-name table for a target engine registry
// name. The Postgres family is "postgres" and "postgres-trigger" (the two
// PG-storage engines sluice registers); everything else — "mysql",
// "planetscale", "vitess", and any empty/unknown name — falls back to the
// MySQL/Vitess table (the confirmed, default surface), so this Phase-3 split
// is byte-for-byte the old behaviour for every non-PG target.
func metricNamesFor(engine string) metricNames {
	switch engine {
	case "postgres", "postgres-trigger":
		return postgresMetricNames
	default:
		return mysqlMetricNames
	}
}

// Marker metrics that identify which engine surface an exposition came from.
// Ground-truthed 2026-07-24 against the live `sluicesync` org (6 branches, 4
// Vitess/MySQL + 2 Postgres): the two storage families are DISJOINT — a
// Vitess branch carries `planetscale_vttablet_volume_*` and never
// `planetscale_volume_*`; a Postgres branch carries `planetscale_volume_*`
// and never the vttablet spelling. That disjointness is what makes
// [metricNamesForExposition] safe.
const (
	markerVitessVolume   = "planetscale_vttablet_volume_capacity_bytes"
	markerPostgresVolume = "planetscale_volume_capacity_bytes"
)

// metricNamesForExposition picks the per-engine metric-name table for ONE
// scraped exposition by MARKER-METRIC presence, falling back to the declared
// (--engine) table when neither marker is present.
//
// It exists for the org-wide fleet watch (roadmap item 75b): the per-org
// service-discovery document carries `planetscale_database_name` /
// `planetscale_branch_name` / `planetscale_organization_name` and NOTHING
// that names the engine (probed live 2026-07-24), yet a real org mixes
// Vitess/MySQL and Postgres databases — the `sluicesync` org itself does. A
// single declared --engine would therefore read the wrong name table for half
// the fleet.
//
// Why this is safe rather than a guess: the two tables' names are disjoint
// (see the marker constants), so picking WRONG can only fail to find a series
// — the corresponding *Known flag stays false, the honest "unobserved"
// degrade — it can never attribute one engine's number to the other's signal.
// The scan is ORDER-INDEPENDENT and an AMBIGUOUS exposition (both markers, or
// neither) falls back to the declared table rather than letting whichever
// series happened to be listed first decide. The single-database path does
// NOT use this at all: it keeps the declared table byte-for-byte, so nothing
// about existing behaviour moves.
func metricNamesForExposition(samples []promSample, declared metricNames) metricNames {
	var vitess, postgres bool
	for _, s := range samples {
		switch s.name {
		case markerVitessVolume:
			vitess = true
		case markerPostgresVolume:
			postgres = true
		}
	}
	switch {
	case vitess && !postgres:
		return mysqlMetricNames
	case postgres && !vitess:
		return postgresMetricNames
	default:
		return declared
	}
}

// Label keys + the write-target selection values. The write-target health
// is the PRIMARY vttablet (the pod that takes apply writes); we select on
// these labels and fall back gracefully when a label is absent.
const (
	labelComponent  = "planetscale_component"
	labelTabletType = "planetscale_tablet_type"
	labelContainer  = "planetscale_container"
	// labelRole is the Postgres surface's primary/replica discriminator.
	// PG emits its PER-POD metrics (the volume pair) under
	// `planetscale_component="hzinstance"` with NO `planetscale_tablet_type`
	// and NO `planetscale_container` — the container label exists only on the
	// PER-CONTAINER metrics (cpu/mem, fanned across postgres / pgbouncer /
	// walg-daemon). So neither the vttablet cascade nor the primaryContainer
	// escape hatch could identify the primary pod, and a 3-pod PG branch fell
	// all the way through to the refuse-to-guess arm: StorageKnown stayed
	// false and `--notify-storage-util` could never fire on a Postgres target
	// (live-confirmed 2026-07-24 on two real PS-PG branches).
	labelRole = "planetscale_role"

	// labelRouter marks a FRONT-DOOR pod — the routing layer client
	// connections arrive on. On a PlanetScale Neki branch it is carried by,
	// and only by, `planetscale_component="nkrouter"` series (verified
	// against a live branch 2026-09-13: every series carrying the label is
	// an nkrouter one, and the database tablets carry none). Its PRESENCE is
	// the test rather than its value, because the value is the operator's
	// router name ("default" on a single-router branch) and a branch with
	// several named routers must still be graded as one front door.
	//
	// The Vitess/MySQL surface emits no series carrying it, so the router
	// fields simply stay unobserved there — no special-casing, and no risk
	// of attributing a vttablet's number to a front door that does not
	// exist.
	labelRouter = "planetscale_router"

	componentVTTablet = "vttablet"
	tabletTypePrimary = "primary"
	rolePrimary       = "primary"
)

// distill collapses a poll's parsed exposition samples into the
// engine-neutral [ir.TargetHealthSnapshot]. It performs the three
// PlanetScale-specific transforms ADR-0107 specifies:
//
//   - PERCENTAGE → FRACTION: CPU/mem arrive as 0–100 percentages; we divide
//     by 100 into the snapshot's [0,1] fraction.
//   - PRIMARY-VTTABLET SELECTION: a metric is emitted per pod; we pick the
//     write-target's series (component=vttablet, tablet_type=primary) and
//     fall back to a best-effort series when the primary label is absent.
//   - MISSING METRIC ⇒ *Known=false: a metric absent from the scrape leaves
//     its value 0 AND its companion flag false, so a consumer NEVER mistakes
//     "unobserved" for "idle" (the loud-failure / honesty tenet).
//
// now stamps SampledAt so freshness is measured from the poll, not the
// exposition's own (untrusted) timestamp. names is the per-engine metric-name
// table ([metricNamesFor]); an empty name in the table means the engine
// exposes no series for that signal, so the lookup finds nothing and the
// corresponding *Known flag stays false (honest "unobserved").
func distill(samples []promSample, names metricNames, now time.Time) ir.TargetHealthSnapshot {
	snap := ir.TargetHealthSnapshot{SampledAt: now}

	if v, ok := selectPrimaryValue(samples, names.cpuUtilPct, names.primaryContainer); ok {
		snap.CPUUtil = clampFraction(v / 100.0)
		snap.CPUKnown = true
	}
	if v, ok := selectPrimaryValue(samples, names.memUtilPct, names.primaryContainer); ok {
		snap.MemUtil = clampFraction(v / 100.0)
		snap.MemKnown = true
	}

	// The front door, as the BUSIEST instance. See the snapshot fields' doc
	// for why this is a separate signal from the primary's CPU/mem and not
	// folded into it, and the metricNames doc for why one table can carry
	// both the router-pod and pooler-sidecar shapes.
	if v, ok := selectFrontDoorCPU(samples, names); ok {
		snap.FrontDoorCPUUtil = clampFraction(v / 100.0)
		snap.FrontDoorCPUKnown = true
	}
	if v, ok := selectFrontDoorMem(samples, names); ok {
		snap.FrontDoorMemUtil = clampFraction(v / 100.0)
		snap.FrontDoorMemKnown = true
	}
	// Not clamped and not a fraction: this is a duration, and a long one is
	// the point. The worst waiter across instances, for the same reason the
	// utilisation figures take the busiest.
	if v, ok := selectWorstOf(samples, names.frontDoorWaitSec); ok {
		snap.FrontDoorWaitSeconds = v
		snap.FrontDoorWaitKnown = true
	}

	avail, availOK := selectPrimaryValue(samples, names.volAvailableByte, names.primaryContainer)
	capac, capacOK := selectPrimaryValue(samples, names.volCapacityByte, names.primaryContainer)
	if availOK && capacOK && capac > 0 {
		snap.StorageAvailableBytes = int64(avail)
		snap.StorageCapacityBytes = int64(capac)
		snap.StorageUtil = clampFraction(1.0 - avail/capac)
		snap.StorageKnown = true
	}

	// The FULLEST pod's volume, across every pod of the branch — a SEPARATE
	// signal from the primary's, never folded into it. A storage alert exists
	// to fire before a volume fills, and the fullest pod is not reliably the
	// primary: on a live PS-PG branch the two replicas were consistently
	// fuller than the primary (637.2 MB vs 603.7 MB used), so a primary-only
	// alert is blind to the pod that reaches the ceiling first. Adaptivity
	// (the AIMD damp / headroom refusal) deliberately stays on the PRIMARY —
	// those throttle sluice's own writes, and the primary is the volume those
	// writes land on.
	if wAvail, wCapac, ok := selectWorstVolume(samples, names.volAvailableByte, names.volCapacityByte); ok {
		snap.StorageAvailableWorstBytes = int64(wAvail)
		snap.StorageCapacityWorstBytes = int64(wCapac)
		snap.StorageUtilWorst = clampFraction(1.0 - wAvail/wCapac)
		snap.StorageWorstKnown = true
	}

	// Replica lag is the WORST (max) lag across replicas, not a
	// primary-selected value — on BOTH engines. Lag is inherently a replica
	// property: neither surface emits a primary series for it (PG tags every
	// lag series planetscale_role="replica"; Vitess tags every one
	// planetscale_tablet_type="replica"), so [selectPrimaryValue] found no
	// primary, fell past the single-series tier on any branch with ≥2
	// replicas, and refused — leaving LagKnown false. That silently disabled
	// --notify-lag-seconds on multi-replica branches of BOTH engines
	// (live-confirmed 2026-07-24). Max is also the semantically right
	// reduction: a lag alert cares about the furthest-behind replica, and on a
	// single-replica branch max degenerates to that one value, so this is a
	// strict repair with no behaviour change where lag already resolved.
	if v, ok := selectWorstOf(samples, names.replicaLagSec); ok {
		snap.ReplicaLagSeconds = v
		snap.LagKnown = true
	}

	active, activeOK := selectPrimaryValue(samples, names.activeConns, names.primaryContainer)
	maxc, maxOK := selectPrimaryValue(samples, names.maxConns, names.primaryContainer)
	// Each half gates its OWN value: the two counts come from independent
	// series families and either can resolve while the other does not, so a
	// shared flag published a fabricated 0 as observed (audit SL-6).
	if activeOK {
		snap.ActiveConnections = int(active)
		snap.ActiveConnKnown = true
	}
	if maxOK {
		snap.MaxConnections = int(maxc)
		snap.MaxConnKnown = true
	}

	return snap
}

// selectPrimaryValue finds the value for the named metric on the
// write-target's PRIMARY series. Selection is a graceful cascade:
//
//  0. (Postgres) if primaryContainer is set, a series whose
//     `planetscale_container` matches it — the PG DB container, picked out of
//     the per-container fan (postgres / pgbouncer / walg-daemon) that has no
//     vttablet/tablet_type labels;
//  1. an exact match (component=vttablet AND tablet_type=primary) — the
//     Vitess write target;
//  2. else any series tagged tablet_type=primary (a primary pod whose
//     component label is absent/different);
//  3. else, if exactly one series exists for the metric, use it (single-pod
//     exposition with no distinguishing labels — the common small-DB case,
//     e.g. the single-series PG volume metric);
//  4. else no confident pick → ok=false (the metric stays *Known=false
//     rather than guessing the wrong pod).
//
// Returns ok=false when the metric is absent entirely (including the
// engine-table-unset case where name is "").
func selectPrimaryValue(samples []promSample, name, primaryContainer string) (float64, bool) {
	if name == "" {
		return 0, false
	}
	var (
		matches     []promSample
		primaryOnly []promSample
	)
	for _, s := range samples {
		if s.name != name {
			continue
		}
		matches = append(matches, s)
		// (0) Postgres container match — the write-target DB container among a
		// multi-container fan (the pod also runs walg-daemon and friends).
		//
		// THE ROLE CHECK IS NOT OPTIONAL, and its absence was a live defect.
		// On a single-writer Postgres the container label alone identifies the
		// write target, which is why this started as a container-only match.
		// On a PlanetScale NEKI branch it does not: the primary AND every
		// replica run `planetscale_container="postgres"`, so a container-only
		// match returns whichever pod the exposition happens to list first.
		// Measured 2026-09-13 on a real Neki exposition — the first
		// postgres-container CPU series was a REPLICA, and `metrics-watch`
		// reported the replica's utilisation as the target's. It went
		// unnoticed because that branch was thrashing and every pod sat near
		// 100%, so the wrong answer equalled the right one.
		//
		// A series that carries no role label keeps the old behaviour (a
		// single-writer PG exposition tags no roles), so this narrows the
		// match only where a role is actually declared.
		if primaryContainer != "" && s.label(labelContainer) == primaryContainer {
			if role := s.label(labelRole); role == "" || role == rolePrimary {
				return s.value, true
			}
		}
		isPrimary := s.label(labelTabletType) == tabletTypePrimary
		if isPrimary && s.label(labelComponent) == componentVTTablet {
			// (1) exact write-target match — take it immediately.
			return s.value, true
		}
		// (2a) Postgres per-pod primary: `planetscale_role="primary"`. Held
		// to the same tier as a tablet_type primary (NOT taken immediately)
		// so the Vitess exact match at (1) always wins if both are somehow
		// present — the two label vocabularies never co-occur on a real
		// exposition, but tier order is the thing that makes that safe
		// rather than incidental.
		if isPrimary || s.label(labelRole) == rolePrimary {
			primaryOnly = append(primaryOnly, s)
		}
	}
	switch {
	case len(matches) == 0:
		return 0, false
	case len(primaryOnly) > 0:
		// (2) primary-tagged but no vttablet component label.
		return primaryOnly[0].value, true
	case len(matches) == 1:
		// (3) single unambiguous series.
		return matches[0].value, true
	default:
		// (4) multiple pods, none identifiable as the primary write target —
		// refuse to guess.
		return 0, false
	}
}

// selectWorstOf returns the MAXIMUM value across every series of the named
// metric — the "worst pod wins" reduction, for signals where higher is worse
// and no series is privileged (replica lag being the motivating case: every
// series is a replica, so there is no primary to select).
//
// Unlike [selectPrimaryValue] this never refuses on ambiguity, because
// ambiguity is not a problem here: with no privileged series, the max IS the
// answer regardless of how many pods report. It returns ok=false only when
// the metric is absent entirely (including the engine-table-unset case where
// name is ""), preserving the honest-unobserved contract.
func selectWorstOf(samples []promSample, name string) (float64, bool) {
	if name == "" {
		return 0, false
	}
	worst, found := 0.0, false
	for _, s := range samples {
		if s.name != name {
			continue
		}
		if !found || s.value > worst {
			worst, found = s.value, true
		}
	}
	return worst, found
}

// selectWorstRouterValue returns the highest value of the named metric across
// the branch's FRONT-DOOR pods — those carrying [labelRouter] — and ok=false
// when the exposition has none, which is every non-Neki surface.
//
// It is deliberately NOT [selectWorstOf] with a filter argument bolted on:
// that function's contract is "the worst across every series", and a router
// series and a database-tablet series of `planetscale_pods_cpu_util_percentages`
// are the same metric on different machines. Reducing them together answers
// "is anything busy", which is the question nobody is asking — the whole point
// of the router fields is to tell a saturated front door apart from a
// saturated database.
//
// The filter is label PRESENCE, not a value match: a branch may run several
// named routers and all of them are the front door (see [labelRouter]).
func selectWorstRouterValue(samples []promSample, name string) (float64, bool) {
	if name == "" {
		return 0, false
	}
	worst, found := 0.0, false
	for _, s := range samples {
		if s.name != name || s.label(labelRouter) == "" {
			continue
		}
		if !found || s.value > worst {
			worst, found = s.value, true
		}
	}
	return worst, found
}

// selectFrontDoorCPU resolves the busiest front door's CPU across the two
// shapes a PlanetScale branch can take, and reports ok=false when the branch
// has neither.
//
// Router pods first, pooler sidecar second — an order that is presentational
// rather than load-bearing, because the two surfaces are disjoint (see
// [metricNames] and [TestFrontDoorSurfacesAreDisjoint]). Written as a cascade
// rather than a single merged query so each arm keeps its own metric name: the
// router arm reads the shared per-pod CPU metric filtered to router series,
// while the pooler arm reads PgBouncer's own per-peer metric, which is a
// DIFFERENT measurement and not interchangeable with the pod-level one.
func selectFrontDoorCPU(samples []promSample, names metricNames) (float64, bool) {
	if v, ok := selectWorstRouterValue(samples, names.cpuUtilPct); ok {
		return v, true
	}
	return selectWorstOf(samples, names.poolerCPUPct)
}

// selectFrontDoorMem is the memory half of [selectFrontDoorCPU]. Both arms
// read the same per-pod memory metric — there is no pgbouncer-specific memory
// series — and differ only in which series they claim: router pods by their
// [labelRouter], the pooler by its container label.
func selectFrontDoorMem(samples []promSample, names metricNames) (float64, bool) {
	if v, ok := selectWorstRouterValue(samples, names.memUtilPct); ok {
		return v, true
	}
	return selectWorstContainerValue(samples, names.memUtilPct, names.poolerContainer)
}

// selectWorstContainerValue returns the highest value of the named metric
// across series whose `planetscale_container` matches container, and false
// when container is empty or no series carries it.
//
// Distinct from [selectPrimaryValue]'s container tier, which picks the ONE
// primary series and is about identifying the write target. This reduces
// across every pod running that container, which is what "the busiest front
// door" means when the front door is a sidecar riding on all of them.
func selectWorstContainerValue(samples []promSample, name, container string) (float64, bool) {
	if name == "" || container == "" {
		return 0, false
	}
	worst, found := 0.0, false
	for _, s := range samples {
		if s.name != name || s.label(labelContainer) != container {
			continue
		}
		if !found || s.value > worst {
			worst, found = s.value, true
		}
	}
	return worst, found
}

// selectWorstVolume returns the (available, capacity) pair of the FULLEST
// pod across every pod exposing the volume metrics — the pod that will hit
// its ceiling first, which is the signal a storage alert wants.
//
// The pairing is the load-bearing part: min(available) and max(capacity)
// taken INDEPENDENTLY across series would mix two different pods and
// manufacture a utilisation neither pod has (a small pod's available over a
// big pod's capacity). So the two metrics are joined per pod on their FULL
// label set — available and capacity for one pod carry identical labels,
// which makes the serialized label set an exact pod key without this
// function needing to know any engine's pod-identity label. Utilisation is
// then compared as a ratio, not as raw free bytes, so heterogeneous volume
// sizes rank correctly.
//
// Returns ok=false when either metric is absent, when no pod exposes both,
// or when every candidate has a non-positive capacity — the same
// honest-unobserved contract as [selectPrimaryValue], never a guess.
func selectWorstVolume(samples []promSample, availName, capacName string) (avail, capac float64, ok bool) {
	if availName == "" || capacName == "" {
		return 0, 0, false
	}
	type pod struct {
		avail, capac float64
		haveA, haveC bool
	}
	pods := map[string]*pod{}
	get := func(s promSample) *pod {
		k := labelKey(s.labels)
		p := pods[k]
		if p == nil {
			p = &pod{}
			pods[k] = p
		}
		return p
	}
	for _, s := range samples {
		switch s.name {
		case availName:
			p := get(s)
			p.avail, p.haveA = s.value, true
		case capacName:
			p := get(s)
			p.capac, p.haveC = s.value, true
		}
	}
	worst := -1.0
	for _, p := range pods {
		if !p.haveA || !p.haveC || p.capac <= 0 {
			continue
		}
		used := 1.0 - p.avail/p.capac
		if used > worst {
			worst, avail, capac, ok = used, p.avail, p.capac, true
		}
	}
	return avail, capac, ok
}

// labelKey serializes a label set into a deterministic, collision-free pod
// key. Sorted so map iteration order can't produce two keys for one pod, and
// length-prefixed so a label value containing the separator cannot forge a
// different pod's key.
func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%d:%s=%d:%s|", len(k), k, len(labels[k]), labels[k])
	}
	return b.String()
}

// clampFraction bounds a derived utilisation into [0,1]; a provider quirk
// (a percentage slightly over 100, or available > capacity mid-resize)
// must never produce an out-of-range fraction the consumers treat as a
// nonsense saturation level.
func clampFraction(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}
