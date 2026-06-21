// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package telemetry is the OPTIONAL PlanetScale control-plane health
// provider (ADR-0107 Phase 2). It implements [ir.TargetTelemetry] against
// the PlanetScale per-org Prometheus-metrics endpoint so sluice's apply
// path can see the target's resource state (CPU / memory / storage, plus
// secondary lag / connections) and react proactively.
//
// CONTAIN-PS-COMPLEXITY: this is the ONLY package outside cmd/sluice that
// knows PlanetScale exists. internal/ir, internal/pipeline, and the engine
// packages import only the engine-neutral [ir.TargetTelemetry] seam; this
// package imports ir (to satisfy the interface) but is imported BY nothing
// in core — it is constructed at the cmd/sluice composition root and handed
// to the streamer as an ir.TargetTelemetry. No planetscale-go SDK: a thin
// stdlib net/http client plus the minimal Prometheus-text parser below is
// sufficient for the documented metric subset and keeps the dependency
// surface small (ADR-0107 "Alternatives considered").
package telemetry

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// promSample is one parsed Prometheus exposition line: a metric name, its
// label set (name → value), and the float value. The optional trailing
// timestamp is ignored — Sample stamps SampledAt with the poll wall-clock,
// so the exposition's own timestamp is not load-bearing.
type promSample struct {
	name   string
	labels map[string]string
	value  float64
}

// label returns the value of the named label, or "" when absent.
func (s promSample) label(k string) string { return s.labels[k] }

// parsePromText parses Prometheus text-exposition (version 0.0.4) into a
// flat slice of samples. It is deliberately minimal — it understands
// `name{l="v",...} value [timestamp]` and bare `name value`, skips `#`
// comment/HELP/TYPE lines and blank lines, and silently drops any line it
// cannot parse (a malformed line must never crash the poll loop). It does
// NOT decode histogram/summary quantile structure beyond treating each
// emitted series as an independent sample — sufficient for the gauge subset
// ADR-0107 consumes.
func parsePromText(r io.Reader) []promSample {
	var out []promSample
	sc := bufio.NewScanner(r)
	// Metric values are short; the buffer only needs to hold one (long,
	// many-labelled) line. 1 MiB is generous headroom over PlanetScale's
	// per-pod label sets.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s, ok := parsePromLine(line)
		if !ok {
			continue
		}
		out = append(out, s)
	}
	return out
}

// parsePromLine parses a single non-comment exposition line. Returns
// ok=false for any line it cannot confidently interpret.
func parsePromLine(line string) (promSample, bool) {
	name, rest, labels := splitNameAndLabels(line)
	if name == "" {
		return promSample{}, false
	}
	// rest is now "<value> [timestamp]"; take the first field as the value.
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return promSample{}, false
	}
	fields := strings.Fields(rest)
	v, err := parsePromFloat(fields[0])
	if err != nil {
		return promSample{}, false
	}
	return promSample{name: name, labels: labels, value: v}, true
}

// splitNameAndLabels separates "name{labels}" (or bare "name") from the
// trailing "value [ts]" portion. Returns the metric name, the remainder
// after the label block, and the parsed label map (nil when there are no
// labels).
func splitNameAndLabels(line string) (name, rest string, labels map[string]string) {
	brace := strings.IndexByte(line, '{')
	if brace < 0 {
		// Bare "name value [ts]".
		sp := strings.IndexAny(line, " \t")
		if sp < 0 {
			return "", "", nil
		}
		return strings.TrimSpace(line[:sp]), line[sp:], nil
	}
	name = strings.TrimSpace(line[:brace])
	end := strings.IndexByte(line[brace:], '}')
	if end < 0 {
		return "", "", nil
	}
	end += brace
	labels = parsePromLabels(line[brace+1 : end])
	return name, line[end+1:], labels
}

// parsePromLabels parses the inside of a `{...}` label block:
// `k1="v1",k2="v2"`. Values are double-quoted with `\"` and `\\` escapes
// per the exposition spec. Malformed pairs are skipped.
func parsePromLabels(s string) map[string]string {
	labels := make(map[string]string)
	i := 0
	for i < len(s) {
		// Skip separators / whitespace.
		for i < len(s) && (s[i] == ',' || s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i >= len(s) {
			break
		}
		// Read key up to '='.
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			break
		}
		key := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		if i >= len(s) || s[i] != '"' {
			break
		}
		i++ // consume opening quote
		val, next, ok := readQuotedValue(s, i)
		if !ok {
			break
		}
		i = next
		if key != "" {
			labels[key] = val
		}
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}

// readQuotedValue reads a double-quoted, backslash-escaped value starting
// at index i (just past the opening quote). Returns the unescaped value,
// the index just past the closing quote, and ok.
func readQuotedValue(s string, i int) (val string, next int, ok bool) {
	var b strings.Builder
	for i < len(s) {
		c := s[i]
		switch c {
		case '\\':
			if i+1 >= len(s) {
				return "", 0, false
			}
			esc := s[i+1]
			if esc == 'n' {
				b.WriteByte('\n')
			} else {
				// `\"` → `"`, `\\` → `\`, and any other escape passes the
				// escaped byte through verbatim (lenient).
				b.WriteByte(esc)
			}
			i += 2
		case '"':
			return b.String(), i + 1, true
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", 0, false
}

// parsePromFloat parses a Prometheus float value, tolerating the
// exposition spec's +Inf / -Inf / NaN spellings (which we treat as
// unparseable so they never poison a utilisation fraction).
func parsePromFloat(s string) (float64, error) {
	switch s {
	case "+Inf", "-Inf", "Inf", "NaN":
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseFloat(s, 64)
}
