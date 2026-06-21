// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"strings"
	"testing"
)

func TestParsePromText_ShapeVariants(t *testing.T) {
	const text = `
# HELP planetscale_pods_cpu_util_percentages CPU util
# TYPE planetscale_pods_cpu_util_percentages gauge
planetscale_pods_cpu_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="primary"} 42.5 1718900000000
planetscale_pods_cpu_util_percentages{planetscale_component="vtgate"} 5
bare_metric 17
malformed_no_value{a="b"}
not_a_number{a="b"} notafloat
`
	samples := parsePromText(strings.NewReader(text))
	// Expect: primary cpu, vtgate cpu, bare_metric. The malformed-no-value
	// and not-a-number lines are dropped.
	if len(samples) != 3 {
		t.Fatalf("want 3 parsed samples, got %d: %+v", len(samples), samples)
	}

	got := samples[0]
	if got.name != "planetscale_pods_cpu_util_percentages" {
		t.Errorf("name = %q", got.name)
	}
	if got.value != 42.5 {
		t.Errorf("value = %v, want 42.5", got.value)
	}
	if got.label("planetscale_component") != "vttablet" {
		t.Errorf("component label = %q", got.label("planetscale_component"))
	}
	if got.label("planetscale_tablet_type") != "primary" {
		t.Errorf("tablet_type label = %q", got.label("planetscale_tablet_type"))
	}
	if got.label("absent") != "" {
		t.Errorf("absent label should be empty, got %q", got.label("absent"))
	}

	if samples[2].name != "bare_metric" || samples[2].value != 17 {
		t.Errorf("bare metric mis-parsed: %+v", samples[2])
	}
}

func TestParsePromText_EscapedLabelValues(t *testing.T) {
	const text = `m{k="a\"b\\c",j="x\ny"} 1`
	samples := parsePromText(strings.NewReader(text))
	if len(samples) != 1 {
		t.Fatalf("want 1 sample, got %d", len(samples))
	}
	if got := samples[0].label("k"); got != `a"b\c` {
		t.Errorf("escaped value k = %q, want %q", got, `a"b\c`)
	}
	if got := samples[0].label("j"); got != "x\ny" {
		t.Errorf("escaped value j = %q, want %q", got, "x\ny")
	}
}

func TestParsePromFloat_RejectsNonFinite(t *testing.T) {
	for _, s := range []string{"+Inf", "-Inf", "Inf", "NaN"} {
		if _, err := parsePromFloat(s); err == nil {
			t.Errorf("parsePromFloat(%q) should error so it never poisons a fraction", s)
		}
	}
	if v, err := parsePromFloat("0.85"); err != nil || v != 0.85 {
		t.Errorf("parsePromFloat(0.85) = %v, %v", v, err)
	}
}
