// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
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
	const wantSeries = 30
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
		SampledAt:             time.Now(),
		CPUUtil:               0.5,
		CPUKnown:              true,
		MemUtil:               0.5,
		MemKnown:              true,
		StorageUtil:           0.5,
		StorageAvailableBytes: 1 << 30,
		StorageCapacityBytes:  1 << 31,
		StorageKnown:          true,
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
