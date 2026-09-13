// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRouterCPUNotifyRule_ReadsTheRouterNotTheDatabase pins the threshold rule
// behind `--notify-router-cpu-util`.
//
// The rule exists because the routing layer and the database saturate
// independently and are resized by different controls — a larger router tier
// versus a larger instance — so an operator who armed `--notify-cpu-util` has
// NOT armed this one, and an alert that fired for either would not say which
// machine to touch.
//
// Two ways to get it wrong, both of which look fine at a glance: reading
// CPUUtil (so the "router" alert is a second database alert, firing on the
// wrong condition and never on the right one), and ignoring the *Known flag
// (so a target with no routing layer reports 0.0 as observed and the rule sits
// permanently un-fired while looking armed).
func TestRouterCPUNotifyRule_ReadsTheRouterNotTheDatabase(t *testing.T) {
	t.Parallel()

	rules := buildMetricsNotifyRulesFrom(metricsNotifyThresholds{RouterCPUUtil: 0.8})
	if len(rules) != 1 {
		t.Fatalf("arming only --notify-router-cpu-util built %d rules, want exactly 1 — every other "+
			"threshold is 0 and must stay inert", len(rules))
	}
	rule := rules[0]
	if rule.metric != notifyRouterCPUUtil {
		t.Fatalf("rule metric = %q, want %q", rule.metric, notifyRouterCPUUtil)
	}
	if rule.read == nil {
		t.Fatal("the router rule has a nil reader; only the derived storage-growth rule may have one")
	}

	// The discriminating snapshot: the two CPU readings DISAGREE, so a reader
	// pointed at the wrong field returns the wrong number rather than the
	// same one by luck.
	busyRouterIdleDB := ir.TargetHealthSnapshot{
		CPUUtil: 0.10, CPUKnown: true,
		RouterCPUUtil: 0.95, RouterCPUKnown: true,
	}
	got, known := rule.read(busyRouterIdleDB)
	if !known {
		t.Fatal("the router reading is marked unobserved on a snapshot that observed it")
	}
	if got != 0.95 {
		if got == 0.10 {
			t.Fatalf("the router rule read %v — that is the DATABASE's CPU. Armed this way, the alert "+
				"fires when the database is busy and stays silent when the routing layer is pegged, "+
				"which is the exact inversion of what it is for", got)
		}
		t.Fatalf("the router rule read %v, want 0.95", got)
	}

	// The inverse shape: the database is pegged and the router is fine. The
	// router rule must NOT fire here — this is the case that would make the
	// alert redundant with --notify-cpu-util rather than complementary.
	idleRouterBusyDB := ir.TargetHealthSnapshot{
		CPUUtil: 0.99, CPUKnown: true,
		RouterCPUUtil: 0.05, RouterCPUKnown: true,
	}
	if v, _ := rule.read(idleRouterBusyDB); v >= rule.threshold {
		t.Fatalf("the router rule read %v (>= the %v threshold) on a snapshot whose ROUTER is at 0.05 "+
			"and whose database is at 0.99 — it would fire for a condition the database's own alert "+
			"already covers", v, rule.threshold)
	}

	// A target with no routing layer: unobserved, so the rule neither fires
	// nor re-arms. A fabricated 0.0-as-observed would leave an operator with
	// an alert that looks armed and can never fire.
	noRouter := ir.TargetHealthSnapshot{CPUUtil: 0.99, CPUKnown: true}
	if _, known := rule.read(noRouter); known {
		t.Error("the router reading is marked OBSERVED on a target with no routing layer — the rule " +
			"would treat a structural absence as a healthy 0%")
	}
}

// TestRouterCPUNotifyRule_IsIndependentOfTheDatabaseRule pins the half that a
// single-rule test cannot: arming one threshold must not arm the other.
//
// This is the sibling-miss shape in its cheapest form. Both rules read a CPU
// fraction from the same snapshot, so a copy-paste that wired the new rule to
// the old threshold field would pass every value-reading assertion above and
// still be wrong in the way that matters — the operator's `--notify-cpu-util`
// would silently start alerting on the router too, or vice versa.
func TestRouterCPUNotifyRule_IsIndependentOfTheDatabaseRule(t *testing.T) {
	t.Parallel()

	onlyDB := buildMetricsNotifyRulesFrom(metricsNotifyThresholds{CPUUtil: 0.9})
	if len(onlyDB) != 1 || onlyDB[0].metric != notifyCPUUtil {
		t.Fatalf("arming only --notify-cpu-util produced %d rule(s) %v, want exactly the %q rule — "+
			"the routing-layer threshold must not be armed by the database flag",
			len(onlyDB), ruleMetrics(onlyDB), notifyCPUUtil)
	}

	onlyRouter := buildMetricsNotifyRulesFrom(metricsNotifyThresholds{RouterCPUUtil: 0.9})
	if len(onlyRouter) != 1 || onlyRouter[0].metric != notifyRouterCPUUtil {
		t.Fatalf("arming only --notify-router-cpu-util produced %d rule(s) %v, want exactly the %q rule",
			len(onlyRouter), ruleMetrics(onlyRouter), notifyRouterCPUUtil)
	}

	both := buildMetricsNotifyRulesFrom(metricsNotifyThresholds{CPUUtil: 0.9, RouterCPUUtil: 0.7})
	if len(both) != 2 {
		t.Fatalf("arming both produced %d rule(s) %v, want 2", len(both), ruleMetrics(both))
	}
	for _, r := range both {
		switch r.metric {
		case notifyCPUUtil:
			if r.threshold != 0.9 {
				t.Errorf("the database CPU rule carries threshold %v, want 0.9 — the two thresholds "+
					"have been crossed", r.threshold)
			}
		case notifyRouterCPUUtil:
			if r.threshold != 0.7 {
				t.Errorf("the routing-layer rule carries threshold %v, want 0.7 — the two thresholds "+
					"have been crossed", r.threshold)
			}
		}
	}
}

// ruleMetrics renders a rule set's metric names for a readable failure.
func ruleMetrics(rules []metricsNotifyRule) []notifyMetric {
	out := make([]notifyMetric, 0, len(rules))
	for _, r := range rules {
		out = append(out, r.metric)
	}
	return out
}
