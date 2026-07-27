// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for audit 2026-07-26 SL-14 — a 200 response that is not a Prometheus
// exposition must be a FAILED poll, not a successful one full of unknowns.
//
// The parser's contract ("silently drops any line it cannot parse") is right
// for a malformed line and wrong for a body that is not an exposition at all.
// An intermediary serving HTML or a JSON error with HTTP 200 parsed to zero
// samples with no error, and the provider then overwrote its good cached
// snapshot with an all-unknown one and recorded the poll as successful —
// strictly worse than an HTTP error, which correctly leaves the last reading
// in place. Downstream: `n/a` everywhere, every --notify-* threshold silently
// stops firing, and the sink persists fresh=true rows with every metric null.
//
// This is the "no skip-branch without proof" rule from the project's own
// new-surface checklist, and the same shape as the mydumper `case "":` that
// made a severed fragment vanish.
package telemetry

import (
	"strings"
	"testing"
)

func TestParsePromTextChecked_RejectsNonExpositionBodies(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"html error page", "<html>\n<head><title>502 Bad Gateway</title></head>\n<body>502</body>\n</html>"},
		{"json error", `{"error":"unauthorized","code":401}`},
		{"plain text", "Service Temporarily Unavailable"},
		{
			// The realistic format-drift case: OpenMetrics allows a quoted
			// metric name, which this parser does not understand. If
			// PlanetScale adopts it, every metric silently becomes unknown.
			name: "openmetrics quoted metric name",
			body: `{"planetscale_pods_cpu_util_percentages",planetscale_role="primary"} 42`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePromTextChecked(strings.NewReader(tc.body))
			if err == nil {
				t.Errorf("parsed %d samples and reported NO error for a body that is not an exposition. The "+
					"provider would treat this as a successful poll and overwrite its good cached snapshot with "+
					"an all-unknown one, silencing every threshold (audit SL-14).", len(got))
			}
		})
	}
}

// TestParsePromTextChecked_AcceptsRealExpositions is the other half: the fix
// must not turn a legitimate scrape into an error. A body with SOME
// unparseable lines is still an exposition as long as something parsed.
func TestParsePromTextChecked_AcceptsRealExpositions(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"empty body", "", 0},
		{"comments only", "# HELP x A metric.\n# TYPE x gauge\n", 0},
		{
			name: "well-formed",
			body: "# TYPE a gauge\na{k=\"v\"} 1\nb 2\n",
			want: 2,
		},
		{
			name: "mixed: one junk line among good ones stays a successful parse",
			body: "a{k=\"v\"} 1\n!!! not a metric !!!\nb 2\n",
			want: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePromTextChecked(strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("legitimate exposition rejected: %v", err)
			}
			if len(got) != tc.want {
				t.Errorf("parsed %d samples, want %d", len(got), tc.want)
			}
		})
	}
}

// TestParsePromLabels_PartialBlockRejectsTheLine pins the related wart: the
// label parser documented "malformed pairs are skipped" while actually
// abandoning the whole block at the first bad pair, silently dropping every
// VALID label after it. Labels are how a series is SELECTED, so a partial
// block can make a replica look unlabelled and be chosen as the primary.
func TestParsePromLabels_PartialBlockRejectsTheLine(t *testing.T) {
	// planetscale_role would be LOST by the old break-and-return-partial
	// behaviour, because it follows the malformed pair.
	line := `m{bad,planetscale_role="primary"} 1`
	if _, ok := parsePromLine(line); ok {
		t.Error("a line whose label block is only partially parseable was accepted. The surviving labels drive " +
			"series selection, so a dropped planetscale_role can make a REPLICA series be selected as the " +
			"primary (audit SL-14).")
	}

	// A well-formed block still parses, with every label present.
	s, ok := parsePromLine(`m{planetscale_role="primary",planetscale_container="postgres"} 1`)
	if !ok {
		t.Fatal("a well-formed label block was rejected")
	}
	if s.label("planetscale_role") != "primary" || s.label("planetscale_container") != "postgres" {
		t.Errorf("labels lost on a well-formed block: %+v", s.labels)
	}
}
