// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// Standalone-sequence comparison for `sluice schema diff` (audit
// 2026-08-04 B-10). [Schemas] walked Tables and Views only, so a schema
// object the IR carries specifically to close a named silent-loss item
// was never compared on the one surface that exists to check it.

import (
	"sort"
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
)

// SequenceDiff captures one standalone sequence's expected-vs-actual
// mismatch, for a sequence present on both sides under the same name.
// Only the fields that actually differ are populated; equal ones are
// left zero, matching [ColumnDiff] and [IndexDiff].
//
// # What is compared, and why exactly these
//
// The attributes below are the ones that decide WHAT VALUES THE SEQUENCE
// PRODUCES — the property the IR field was added for (a re-optioned
// source sequence that collapsed to a plain identity column made the
// migrated target hand out different numbers than the source would):
//
//   - DataType, MinValue, MaxValue — the value RANGE. A target capped
//     lower than its source stops issuing values the source still can.
//   - Start, Increment — the value SEQUENCE itself.
//   - Cycle — what happens at the boundary: wrap and re-issue numbers
//     already consumed, or refuse. That is the difference between a
//     duplicate-key error and silently reusing an identifier.
//   - Cache — how many values a session claims at once, which decides
//     which numbers a given backend hands out and how many are lost on
//     a crash. It changes the produced values, so it is compared.
//   - OwnedBy — lifecycle: an owned sequence is dropped with its column,
//     an unowned one outlives it.
//
// Deliberately NOT compared: LastValue / LastValueIsCalled /
// LastValueValid, the sequence's POSITION. It is a moving runtime value,
// not a structural attribute — a source under write load advances between
// the two reads this diff makes, so comparing it would report drift on
// every run of a HEALTHY pair. A check that fires constantly gets
// suppressed, and a suppressed check hides the structural drift above.
// The position is instead the writer's business (it primes the target via
// setval at migrate time; see [ir.Sequence]). This is a real coverage
// gap, named rather than implied: `schema diff` will not tell an operator
// that a target sequence is behind its source.
type SequenceDiff struct {
	Name string

	// Each pair is populated only when that attribute differs, with BOTH
	// sides rendered. Integer attributes render through strconv, which
	// never produces the empty string — so "both empty" unambiguously
	// means "equal" even for a legitimate zero value.
	ExpectedDataType string `json:"expected_data_type,omitempty"`
	ActualDataType   string `json:"actual_data_type,omitempty"`

	ExpectedStart string `json:"expected_start,omitempty"`
	ActualStart   string `json:"actual_start,omitempty"`

	ExpectedIncrement string `json:"expected_increment,omitempty"`
	ActualIncrement   string `json:"actual_increment,omitempty"`

	ExpectedMinValue string `json:"expected_min_value,omitempty"`
	ActualMinValue   string `json:"actual_min_value,omitempty"`

	ExpectedMaxValue string `json:"expected_max_value,omitempty"`
	ActualMaxValue   string `json:"actual_max_value,omitempty"`

	ExpectedCache string `json:"expected_cache,omitempty"`
	ActualCache   string `json:"actual_cache,omitempty"`

	// CycleMismatched gates the bool pair for the reason
	// [IndexDiff.UniqueMismatched] does: false is both a real value and
	// the zero value.
	CycleMismatched bool `json:"cycle_mismatched,omitempty"`
	ExpectedCycle   bool `json:"expected_cycle,omitempty"`
	ActualCycle     bool `json:"actual_cycle,omitempty"`

	ExpectedOwnedBy string `json:"expected_owned_by,omitempty"`
	ActualOwnedBy   string `json:"actual_owned_by,omitempty"`
}

// diffSequences populates the sequence-related slices on d. Sequences are
// matched by NAME (set semantics, mirroring tables and views) rather than
// by schema-qualified name: the diff's target-side reader is pinned to a
// single namespace (`--target-schema`), so the schema part carries no
// discriminating information here and including it would make every
// cross-namespace diff report the whole set as missing+extra.
func diffSequences(d *SchemaDiff, expected, actual *ir.Schema, opts Options) {
	expByName := sequencesByName(expected)
	actByName := sequencesByName(actual)

	for name := range expByName {
		if _, ok := actByName[name]; !ok {
			d.SequencesMissing = append(d.SequencesMissing, name)
		}
	}
	sort.Strings(d.SequencesMissing)

	if !opts.IgnoreExtras {
		for name := range actByName {
			if _, ok := expByName[name]; !ok {
				d.SequencesExtra = append(d.SequencesExtra, name)
			}
		}
		sort.Strings(d.SequencesExtra)
	}

	common := make([]string, 0, len(expByName))
	for name := range expByName {
		if _, ok := actByName[name]; ok {
			common = append(common, name)
		}
	}
	sort.Strings(common)

	for _, name := range common {
		if sd, mismatched := diffSequence(name, expByName[name], actByName[name]); mismatched {
			d.SequencesMismatched = append(d.SequencesMismatched, sd)
		}
	}
}

// diffSequence compares one sequence pair. See [SequenceDiff] for which
// attributes and why.
func diffSequence(name string, expected, actual *ir.Sequence) (SequenceDiff, bool) {
	sd := SequenceDiff{Name: name}
	mismatched := false

	// Empty DataType means bigint (the PG default; see [ir.Sequence]).
	// Normalizing before comparison keeps a reader that spells the
	// default out from reporting drift against one that leaves it blank.
	if e, a := normalizeSeqDataType(expected.DataType), normalizeSeqDataType(actual.DataType); e != a {
		sd.ExpectedDataType, sd.ActualDataType = e, a
		mismatched = true
	}
	for _, pair := range []struct {
		exp, act       int64
		expOut, actOut *string
	}{
		{expected.Start, actual.Start, &sd.ExpectedStart, &sd.ActualStart},
		{expected.Increment, actual.Increment, &sd.ExpectedIncrement, &sd.ActualIncrement},
		{expected.MinValue, actual.MinValue, &sd.ExpectedMinValue, &sd.ActualMinValue},
		{expected.MaxValue, actual.MaxValue, &sd.ExpectedMaxValue, &sd.ActualMaxValue},
		{expected.Cache, actual.Cache, &sd.ExpectedCache, &sd.ActualCache},
	} {
		if pair.exp == pair.act {
			continue
		}
		*pair.expOut = strconv.FormatInt(pair.exp, 10)
		*pair.actOut = strconv.FormatInt(pair.act, 10)
		mismatched = true
	}
	if expected.Cycle != actual.Cycle {
		sd.CycleMismatched = true
		sd.ExpectedCycle, sd.ActualCycle = expected.Cycle, actual.Cycle
		mismatched = true
	}
	if e, a := renderSeqOwnedBy(expected), renderSeqOwnedBy(actual); e != a {
		sd.ExpectedOwnedBy, sd.ActualOwnedBy = e, a
		mismatched = true
	}
	return sd, mismatched
}

// normalizeSeqDataType maps the "" spelling of the PG default onto the
// explicit one so the two compare equal.
func normalizeSeqDataType(t string) string {
	if t == "" {
		return "bigint"
	}
	return t
}

// renderSeqOwnedBy renders a sequence's OWNED BY target for comparison
// and display. "<none>" for an unowned sequence, mirroring the "<none>"
// sentinel [renderDefault] uses, so "unowned" is distinguishable from a
// malformed half-populated pair.
func renderSeqOwnedBy(s *ir.Sequence) string {
	if s.OwnedByTable == "" && s.OwnedByColumn == "" {
		return "<none>"
	}
	return s.OwnedByTable + "." + s.OwnedByColumn
}

// sequencesByName indexes a schema's Sequences by name. Returns an empty
// map for a nil schema or one with no sequences. Unnamed entries are
// skipped on the same rationale as [checksByName] — a sequence with no
// name has no identity to match on, and no reader produces one.
func sequencesByName(s *ir.Schema) map[string]*ir.Sequence {
	if s == nil {
		return nil
	}
	out := make(map[string]*ir.Sequence, len(s.Sequences))
	for _, seq := range s.Sequences {
		if seq == nil || seq.Name == "" {
			continue
		}
		out[seq.Name] = seq
	}
	return out
}
