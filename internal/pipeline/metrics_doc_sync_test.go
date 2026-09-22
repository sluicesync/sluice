// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// TestMetricsDocSync_RunningAsAService pins the operator doc's metrics
// reference table against the REAL scrape output, the same Tier-1 ratchet
// shape as sluicecode's TestRegistryDocSync and docsync's CDC-guide gate:
//
//  1. every series the handler can emit must appear in
//     docs/operator/running-as-a-service.md (a new metric can't ship
//     undocumented), and
//  2. every series the doc's reference table names must come out of the
//     scrape (the table can't rot into claiming metrics that don't exist).
//
// The scrape is assembled with EVERY optional family attached (AIMD serial +
// lanes, spill, target telemetry with all *Known flags set, sync lag known),
// so the name inventory is the full emit surface — not the default-off
// subset. Names are parsed from the `# TYPE` exposition headers, which every
// emitter writes exactly once per family.
func TestMetricsDocSync_RunningAsAService(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.SetBuildInfo("v-docsync", "deadbeef")
	ms.AttachAIMDController(newAIMDControllerForTest(t, "docsync", 100))
	ms.AttachLaneAIMDControllers([]*appliercontrol.Controller{
		newAIMDControllerForTest(t, "docsync", 100),
	})
	ms.AttachSpillReporter(func(context.Context) (SpillSnapshot, bool, error) {
		return SpillSnapshot{StreamID: "docsync", SlotName: "sluice_slot", SpillTxns: 1, SpillBytes: 2}, true, nil
	})
	ms.AttachTargetTelemetry(docSyncTelemetry{})
	ms.AttachSyncLagSource(docSyncLagSource{})

	body := scrapeMetrics(t, ms)

	typeRe := regexp.MustCompile(`(?m)^# TYPE (sluice_[a-z0-9_]+) `)
	scraped := make(map[string]bool)
	for _, m := range typeRe.FindAllStringSubmatch(body, -1) {
		scraped[m[1]] = true
	}

	// Belt against a vacuous pass: the fully-attached scrape must surface
	// the whole emit surface. Adding a metric to metrics.go moves this
	// number — update it together with the doc table, which direction (1)
	// below forces anyway.
	const wantSeries = 35
	if len(scraped) != wantSeries {
		t.Fatalf("fully-attached scrape emitted %d distinct sluice_* series, want %d — if a metric was added/removed, update docs/operator/running-as-a-service.md's reference table and this count together. scraped: %v", len(scraped), wantSeries, sortedKeys(scraped))
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "running-as-a-service.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read operator doc: %v", err)
	}
	doc := string(raw)

	// Direction 1: every emitted series is documented.
	for name := range scraped {
		if !regexp.MustCompile(regexp.QuoteMeta(name) + `\b`).MatchString(doc) {
			t.Errorf("series %q is emitted by /metrics but never mentioned in %s — add it to the metrics reference table", name, docPath)
		}
	}

	// Direction 2: every series the reference table CLAIMS exists. Table
	// rows carry the series name backticked in the first column.
	rowRe := regexp.MustCompile("(?m)^\\| `(sluice_[a-z0-9_]+)` \\|")
	rows := rowRe.FindAllStringSubmatch(doc, -1)
	if len(rows) < 20 {
		t.Fatalf("found only %d metric rows in %s — the reference table appears missing or reformatted; keep rows in the '| `sluice_...` |' shape this gate parses", len(rows), docPath)
	}
	for _, m := range rows {
		if !scraped[m[1]] {
			t.Errorf("doc table row names %q but the fully-attached scrape never emits it — stale doc row (or a conditional family this test forgot to attach)", m[1])
		}
	}
}

// sortedKeys renders a set's keys for a stable failure message.
func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	// Order doesn't need to be sorted for correctness of the message, but
	// stability makes two failure outputs diffable.
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

// docSyncTelemetry is an [ir.TargetTelemetry] stub whose every *Known flag
// is true, so the scrape emits the complete target-health family.
type docSyncTelemetry struct{}

func (docSyncTelemetry) Sample(context.Context) (ir.TargetHealthSnapshot, bool) {
	return ir.TargetHealthSnapshot{
		SampledAt: time.Now(),
		CPUUtil:   0.5,
		CPUKnown:  true,
		MemUtil:   0.5,
		MemKnown:  true,
		// The routing layer and the connection pooler, enabled for the same
		// reason the worst-pod family below is: the exporter gates each series
		// on its own flag. Every value here is DISTINCT from every other so a
		// copy-paste that emitted the wrong field is visible in a failure diff
		// — which matters more since these five fields sit in two groups of
		// near-identical shape.
		RouterCPUUtil:              0.9,
		RouterCPUKnown:             true,
		RouterMemUtil:              0.3,
		RouterMemKnown:             true,
		PgBouncerCPUUtil:           0.55,
		PgBouncerCPUKnown:          true,
		PgBouncerMemUtil:           0.45,
		PgBouncerMemKnown:          true,
		PgBouncerClientWaitSeconds: 1.25,
		PgBouncerWaitKnown:         true,
		StorageUtil:                0.5,
		StorageAvailableBytes:      1 << 30,
		StorageCapacityBytes:       1 << 31,
		StorageKnown:               true,
		// The worst-pod family must be ENABLED here or the doc-sync gate
		// cannot see it: the exporter emits it only under StorageWorstKnown,
		// so a fixture that leaves the flag false lets a new metric ship both
		// undocumented and unpinned — the drift class this test exists to
		// catch. Deliberately fuller than the primary above, mirroring the
		// live PS-PG shape that motivated the signal.
		StorageUtilWorst:           0.75,
		StorageAvailableWorstBytes: 1 << 29,
		StorageCapacityWorstBytes:  1 << 31,
		StorageWorstKnown:          true,
		ReplicaLagSeconds:          1.5,
		LagKnown:                   true,
		ActiveConnections:          3,
		MaxConnections:             100,
		ActiveConnKnown:            true,
		MaxConnKnown:               true,
	}, true
}

// docSyncLagSource is a syncLagSource stub with a known reading, so the
// scrape emits sluice_sync_lag_seconds.
type docSyncLagSource struct{}

func (docSyncLagSource) SyncLagSeconds(time.Time) (float64, bool) { return 2.5, true }

// TestDocSyncFixturesEnableEveryKnownFlag is the Tier-1 gate for the
// fixture-blindness class.
//
// Both /metrics exporters gate each metric family on a *Known flag from
// [ir.TargetHealthSnapshot]. A doc-sync fixture that leaves one false makes the
// exporter emit nothing for that family — so the gate sees nothing, compares
// nothing, and passes while a brand-new metric ships undocumented AND unpinned.
// That is not hypothetical: it happened twice in one day, in the
// single-database exporter and then again in the fleet exporter, for the same
// three worst-pod series.
//
// Reflecting over the struct rather than listing flags by hand is the point: a
// future signal added to the snapshot arrives here automatically, and the
// person adding it is told to enable it rather than discovering months later
// that their metric was never covered.
func TestDocSyncFixturesEnableEveryKnownFlag(t *testing.T) {
	snap, ok := docSyncTelemetry{}.Sample(context.Background())
	if !ok {
		t.Fatal("docSyncTelemetry.Sample returned ok=false; the doc-sync fixture must produce a usable snapshot")
	}

	v := reflect.ValueOf(snap)
	typ := v.Type()
	var unset []string
	seen := 0
	for i := range typ.NumField() {
		f := typ.Field(i)
		if f.Type.Kind() != reflect.Bool || !strings.HasSuffix(f.Name, "Known") {
			continue
		}
		seen++
		if !v.Field(i).Bool() {
			unset = append(unset, f.Name)
		}
	}

	// Anti-vacuity: a rename away from the *Known convention would silently
	// empty this gate.
	if seen < 4 {
		t.Fatalf("found only %d *Known flags on ir.TargetHealthSnapshot; the naming convention this gate keys on has probably changed — re-point it", seen)
	}
	if len(unset) > 0 {
		t.Errorf("doc-sync fixture leaves %d *Known flag(s) false: %s\n\n"+
			"Every exporter family is gated on one of these, so a false flag makes the family unemittable "+
			"and this gate structurally blind to it — a new metric would ship undocumented and unpinned. "+
			"Set them all true in docSyncTelemetry and give the fields a representative value.",
			len(unset), strings.Join(unset, ", "))
	}
}

// TestMetricsDocSync_EveryOperatorDocNamesOnlyLiveSeries widens the two
// table gates above from ONE doc's reference table to every operator-facing
// doc.
//
// Those gates hold docs/operator/running-as-a-service.md's table to the live
// scrape in both directions, and they came back clean under an independent
// sweep — while three series that have never existed (`sluice_lag_seconds`,
// `sluice_seconds_since_last_event`, `sluice_streamer_state`) sat in
// docs/vitess-vstream-troubleshooting.md's "what to watch" section for ~140
// releases, telling an operator mid-incident to alert on series that never
// appear on /metrics (gap census 2026-09-22, GC-16). The names were copied
// from a DESIGN doc. Nothing graded them, because the existing gates'
// universe was one file's table rows.
//
// # What is graded
//
// A `sluice_[a-z0-9_]+` token in an operator-facing doc is graded when it
// LOOKS LIKE a series name, by any of three arms:
//
//   - it carries a Prometheus unit/type suffix (`_total`, `_seconds`, …);
//   - its namespace — the segment after `sluice_` — is one the live registry
//     emits (`sync`, `apply`, `target`, …), so `sluice_apply_foo` is graded
//     even with no suffix;
//   - it sits in a section (heading to heading) that talks about metrics,
//     gauges, counters, Prometheus, scrapes or alerting — the arm that
//     catches a state gauge with no unit suffix and a namespace nothing
//     emits, which is exactly what `sluice_streamer_state` was.
//
// A graded token passes when it is a live series, a control-table name
// ([appliershared.ControlTableNames] — `sluice_target_metrics_history`
// shares the `target` namespace with fourteen real gauges), or carries a
// per-file exemption naming it and saying why:
//
//	<!-- metric-name-exempt: sluice_vstream_reshards_total - named as planned future work -->
//
// Tokens outside the three arms (a slot named `sluice_wave1`, the
// `sluice_capture` trigger) are not graded. The tell that the arms are too
// narrow is a phantom this gate did not see; the fix is a fourth arm, not a
// wider exemption list.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
// The roots are cmd/sluice's TestDocsNameOnlyRealCommands roots, for its
// reasons: docs/*.md, docs/operator, docs/cookbook, skills/, README.md and
// AGENTS.md — the trees an operator ACTS on. docs/dev (where the design doc
// that seeded the phantoms legitimately proposes series that do not exist),
// docs/adr, docs/releases and docs/research are OUT.
//
// The live set is the union of the fully-attached single-database scrape and
// the fleet exporter — the same fixtures the gates above use — so a family
// they cannot see (a *Known flag left false) is invisible here too;
// TestDocSyncFixturesEnableEveryKnownFlag is what keeps that honest.
func TestMetricsDocSync_EveryOperatorDocNamesOnlyLiveSeries(t *testing.T) {
	live := liveMetricSeries(t)

	// Anti-vacuity on the truth side: the single-database scrape alone
	// carries 35 series (pinned above), so a smaller union means the scrape
	// helper stopped seeing the exporter.
	if len(live) < 35 {
		t.Fatalf("only %d live series derived from the two exporters (floor 35) — the scrape side of this gate is broken", len(live))
	}
	liveNamespace := map[string]bool{}
	for name := range live {
		liveNamespace[metricNamespace(name)] = true
	}
	controlTable := map[string]bool{}
	for _, name := range appliershared.ControlTableNames() {
		controlTable[name] = true
	}

	// The optional `="` prefix marks a token that is a LABEL VALUE in a
	// quoted scrape sample (`slot="sluice_slot"`), which is a slot name, not
	// a series; the loop below drops those.
	tokenRe := regexp.MustCompile(`(=")?\bsluice_[a-z0-9_]+`)
	headingRe := regexp.MustCompile(`(?m)^#{1,6} `)
	contextRe := regexp.MustCompile(`(?i)\b(metrics?|gauges?|counters?|histograms?|prometheus|promql|scrap(e|es|ed|ing)|alert(s|ing)?)\b`)
	// The reason text is required by the pattern: an exemption with no
	// stated reason does not parse, and therefore does not exempt.
	exemptRe := regexp.MustCompile(`<!--\s*metric-name-exempt:\s*(sluice_[a-z0-9_]+)\s*[-\x{2014}]+\s*[^\s>][^>]*-->`)

	repo := filepath.Join("..", "..")
	phantom := map[string][]string{} // token -> files naming it
	graded := map[string]bool{}
	files := 0
	for _, path := range operatorDocFiles(t, repo) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		files++
		rel, _ := filepath.Rel(repo, path)
		rel = filepath.ToSlash(rel)
		doc := string(raw)

		exempt := map[string]bool{}
		for _, m := range exemptRe.FindAllStringSubmatch(doc, -1) {
			exempt[m[1]] = true
		}

		for _, section := range headingRe.Split(doc, -1) {
			inMetricContext := contextRe.MatchString(section)
			for _, tok := range tokenRe.FindAllString(section, -1) {
				// A trailing underscore is a family-prefix mention
				// (`sluice_target_…`), not a name; a label value is a
				// slot/stream name the exporter carries, not a series.
				if strings.HasSuffix(tok, "_") || strings.HasPrefix(tok, `="`) {
					continue
				}
				if !hasMetricUnitSuffix(tok) && !liveNamespace[metricNamespace(tok)] && !inMetricContext {
					continue
				}
				graded[tok] = true
				if live[tok] || controlTable[tok] || exempt[tok] {
					continue
				}
				if !slices.Contains(phantom[tok], rel) {
					phantom[tok] = append(phantom[tok], rel)
				}
			}
		}
	}

	// Anti-vacuity floors on the walk side. The metrics reference table
	// alone grades ~40 distinct names; a walker that lost its roots or a
	// tokenizer that stopped matching would pass on an empty set.
	if files < 25 {
		t.Fatalf("only %d operator doc files scanned (floor 25); a root moved and this gate is measuring nothing", files)
	}
	if len(graded) < 30 {
		t.Fatalf("only %d distinct metric-shaped tokens graded (floor 30); the tokenizer or the section split stopped matching", len(graded))
	}

	if len(phantom) == 0 {
		return
	}
	names := make([]string, 0, len(phantom))
	for tok := range phantom {
		names = append(names, tok)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("operator-facing docs name Prometheus series that /metrics never emits:\n")
	for _, tok := range names {
		b.WriteString("  " + tok + "  (" + strings.Join(phantom[tok], ", ") + ")\n")
	}
	b.WriteString("\nThe live set is derived from the real exporters, not a list. Either the doc names a series that was\n")
	b.WriteString("never built (rewrite it around one that exists — docs/operator/running-as-a-service.md's table is the\n")
	b.WriteString("reference), or the series was renamed and the doc was not. A deliberate mention of a series that does\n")
	b.WriteString("not exist (planned work, or a sentence saying no such gauge exists) carries a marker in that file:\n")
	b.WriteString("  <!-- metric-name-exempt: sluice_<name> - <why> -->")
	t.Fatal(b.String())
}

// liveMetricSeries is the union of every series the single-database exporter
// (fully attached, as TestMetricsDocSync_RunningAsAService builds it) and the
// fleet exporter can emit, keyed by name.
func liveMetricSeries(t *testing.T) map[string]bool {
	t.Helper()
	typeRe := regexp.MustCompile(`(?m)^# TYPE (sluice_[a-z0-9_]+) `)
	live := map[string]bool{}

	ms := newTestMetricsServer(t)
	ms.SetBuildInfo("v-docsync", "deadbeef")
	ms.AttachAIMDController(newAIMDControllerForTest(t, "docsync", 100))
	ms.AttachLaneAIMDControllers([]*appliercontrol.Controller{
		newAIMDControllerForTest(t, "docsync", 100),
	})
	ms.AttachSpillReporter(func(context.Context) (SpillSnapshot, bool, error) {
		return SpillSnapshot{StreamID: "docsync", SlotName: "sluice_slot", SpillTxns: 1, SpillBytes: 2}, true, nil
	})
	ms.AttachTargetTelemetry(docSyncTelemetry{})
	ms.AttachSyncLagSource(docSyncLagSource{})
	for _, m := range typeRe.FindAllStringSubmatch(scrapeMetrics(t, ms), -1) {
		live[m[1]] = true
	}

	// The fleet exporter, fed the same every-flag-true snapshot so the
	// fixture-blindness gate above covers this half too.
	snap, _ := docSyncTelemetry{}.Sample(context.Background())
	var buf bytes.Buffer
	emitFleetTelemetryMetrics(&buf, []ir.FleetHealthSample{
		{Target: ir.FleetTarget{Database: "app", Branch: "main"}, Snapshot: snap, OK: true},
	})
	for _, m := range typeRe.FindAllStringSubmatch(buf.String(), -1) {
		live[m[1]] = true
	}
	return live
}

// metricNamespace is the segment after `sluice_`: `sluice_apply_batch_size_current`
// → `apply`.
func metricNamespace(name string) string {
	rest := strings.TrimPrefix(name, "sluice_")
	if i := strings.IndexByte(rest, '_'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// hasMetricUnitSuffix reports whether a token ends in one of the Prometheus
// unit/type suffixes the live registry uses. A control table can share one
// (`sluice_cdc_state` does not; `_state` is deliberately absent so it does
// not have to be exempted everywhere it appears) — a token that both matches
// here and is a control table passes on the roster, not on the suffix.
func hasMetricUnitSuffix(tok string) bool {
	for _, s := range []string{
		"_total", "_seconds", "_bytes", "_info", "_util", "_percent", "_ratio",
		"_count", "_objects", "_connections", "_goroutines", "_gomaxprocs",
	} {
		if strings.HasSuffix(tok, s) {
			return true
		}
	}
	return false
}

// operatorDocFiles is the in-scope surface: a plain file is read directly, a
// directory contributes the markdown directly inside it, and "/..." recurses
// (skills/ holds one SKILL.md per subdirectory). The list mirrors
// cmd/sluice's docCommandRoots; see the gate's doc comment for what is OUT.
func operatorDocFiles(t *testing.T, repo string) []string {
	t.Helper()
	roots := []string{"docs", "docs/operator", "docs/cookbook", "skills/...", "README.md", "AGENTS.md"}
	seen := map[string]bool{}
	var out []string
	add := func(p string) {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	for _, root := range roots {
		recurse := strings.HasSuffix(root, "/...")
		root = filepath.Join(repo, strings.TrimSuffix(root, "/..."))
		info, err := os.Stat(root)
		if err != nil {
			continue // a missing root is caught by the file-count floor
		}
		if !info.IsDir() {
			add(root)
			continue
		}
		if recurse {
			err := filepath.Walk(root, func(p string, fi os.FileInfo, werr error) error {
				if werr != nil || fi.IsDir() || !strings.HasSuffix(p, ".md") {
					return werr
				}
				add(p)
				return nil
			})
			if err != nil {
				t.Fatalf("walk %s: %v", root, err)
			}
			continue
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read dir %s: %v", root, err)
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
				add(filepath.Join(root, e.Name()))
			}
		}
	}
	sort.Strings(out)
	return out
}
