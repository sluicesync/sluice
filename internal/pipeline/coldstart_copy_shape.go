// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The copy-shape fingerprint: what a cold start's flags made the copy
// PUT ON THE TARGET, recorded so a resume can refuse when the re-run's
// flags no longer describe it.
//
// # Why this is not the drift check we already had
//
// The pipeline already refuses a `--where` that changed between runs:
// [rowFilterHashDriftAny], compared against the row_filter_hash on the
// stream's `sluice_cdc_state` row. That door cannot fire for a
// stopped-cold-start resume, and the reason is structural rather than
// an oversight: its first condition is `rowExists`, the cdc-state row —
// and the resume path is reached from the dispatch's `default:` case,
// which is precisely the case where that row has never been written.
// The ADR-0031 source/target fingerprint check is bypassed for the same
// reason. Every identity-and-drift door in runOnce keys on a row this
// state does not have.
//
// So the same question is asked against a row this path DOES write —
// the migrate-state header that already carries the snapshot anchor.
// The hash function for `--where` is literally the existing one, so a
// stream that later grows a cdc-state row is graded by both against the
// same value.
//
// # What a mismatch would cost, which is why it is a refusal
//
// The resume SKIPS the bulk copy. Run 1 copied the rows ITS flags
// selected and shaped; run 2 then owns the target and streams CDC into
// it under run 2's flags. Every disagreement is silent divergence at
// exit 0:
//
//   - a widened or removed `--where`: every row run 1's predicate
//     excluded is permanently absent, and CDC will never backfill it;
//   - a changed `--redact`: half the target is redacted and half is
//     not, by copy-vs-CDC provenance;
//   - a changed `--type-override` / `--inject-shard-column`: the target
//     was CREATED with run 1's shapes and the resume does not re-create
//     tables, so run 2's shaping applies only to what CDC writes;
//   - a changed `--target-schema`: run 1's rows are in one namespace and
//     run 2 addresses another.
//
// Before this branch every one of those re-runs hit the "slot already
// exists" refusal, so the resume must not be the thing that lets them
// through quietly.

package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// coldStartCopyShape renders the fingerprint of the shaping inputs for
// this run, as `key=hash` pairs joined by ';' in key order.
//
// # The enumeration — which operator inputs are graded, and which are not
//
// COVERED (a change refuses the resume):
//
//	where         — RowFilters:            which ROWS were copied
//	tables        — the in-scope table set: which TABLES were copied
//	types         — Mappings + ExpressionMappings: the target COLUMN types
//	redact        — Redactor:              the VALUES written
//	shard         — InjectShardColumn:     an extra column + its value
//	target_schema — TargetSchema:          WHERE the rows were written
//
// ADVISORY — recorded and compared, but a difference WARNS instead of
// refusing (see [copyShapeAdvisory]):
//
//	skip_fks — --skip-foreign-keys
//	views    — the effective view set (ViewFilter / --skip-views)
//
// The reason these are not refusals, and the precise limit of that
// reason, which was worth tracing rather than asserting: both shape the
// CONSTRAINTS and VIEWS phases, and the resume RE-RUNS those phases
// under run 2's settings — so in the window this resume was built for
// (a stop during the copy+index phase, which is where a stop actually
// lands) run 2's intent is applied in full and nothing diverges. That
// argument holds ONLY for that window. The phase gate also admits
// `constraints`, `views` and `complete`, and there run 1's constraint
// or view phase has already run in part or whole:
//
//   - run 1 without --skip-foreign-keys stopped at/after `constraints`
//     has already CREATED some foreign keys. A run 2 that adds the flag
//     creates no more — and cannot remove the ones already there. The
//     operator asked for no FKs and the target holds some.
//   - the mirror: run 1 WITH the flag synthesised backing indexes for
//     the skipped FKs; a run 2 without it leaves those indexes in
//     place, and creates the FKs on top.
//   - a stop inside the VIEWS phase can leave a view run 2's narrower
//     filter would not create.
//
// None of that is a lost or altered ROW — no copied row depends on
// either flag — so refusing would cost a full re-copy to fix a
// difference in DDL the operator can see and change with one
// statement, and would do it in the sympathetic case where someone
// Ctrl-Cs a slow FK validation and re-runs with --skip-foreign-keys.
// So the resume proceeds and SAYS what the target now holds. That is a
// deliberate trade, not an oversight, and it is why these keys are
// recorded at all: an uncompared flag could not even be reported.
//
// NOT RECORDED, each with its reason:
//
//   - --upfront-indexes, --analyze-after, and every parallelism /
//     buffer / fan-out knob: performance only. The resume builds the
//     indexes itself, under run 2's settings.
//   - --schema-already-applied: it suppresses DDL phases and promises a
//     prepared target. The resume creates no tables either way.
//   - source and target DSN identity: NOT a copy shape, and covered
//     elsewhere — the source by [ir.SnapshotAnchorVerifier] (the slot
//     must be the one the anchor names, at the anchor's LSN) and the
//     target by the resume's own row floor, which asks the target
//     whether the rows are there rather than trusting a fingerprint.
//
// TestColdStartCopyShape_EveryAspectDrifts holds that list to the code:
// it changes each recorded input and requires exactly its key to drift.
//
// Every aspect emits a value even when the flag is unset, because
// "unset" and "set" must hash differently in BOTH directions — the
// expensive drift is a `--where` that was REMOVED, which a scheme that
// omitted absent aspects would render as no change at all (the D0-2
// lesson, at a different door).
//
// schema is the FINALIZED schema (post-[Streamer.coldStartPrepareSchema]),
// so the table set is the effective one rather than the filter's
// spelling: two different `--tables` spellings that select the same
// tables agree, and that is the property worth comparing.
func coldStartCopyShape(s *Streamer, schema *ir.Schema) string {
	pairs := [][2]string{
		{"where", rowFilterFullHash(s.RowFilters)},
		{"tables", copyShapeTableSetHash(schema)},
		{"types", copyShapeTypesHash(s)},
		{"redact", copyShapeRedactHash(s)},
		{"shard", copyShapeShardHash(s.InjectShardColumn)},
		{"target_schema", copyShapeTokenHash(s.TargetSchema)},
		{"skip_fks", copyShapeTokenHash(fmt.Sprint(s.SkipForeignKeys))},
		{"views", copyShapeViewSetHash(schema)},
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		parts = append(parts, p[0]+"="+p[1])
	}
	return strings.Join(parts, ";")
}

// copyShapeDrift reports the aspect keys whose recorded fingerprint
// differs from the current one.
//
// Two asymmetries, both deliberate and both in the refusing direction:
//
//   - a key present NOW but absent from the recorded value is DRIFT.
//     The anchor and the fingerprint are written together by the same
//     binary, so a recorded value missing a key can only come from an
//     older sluice that did not grade that aspect — which means it is
//     unproven, not unchanged. The cost is that adding an aspect
//     refuses in-flight stopped cold starts from the previous version;
//     that is a loud re-copy, which is what those operators had before
//     the resume existed.
//   - a key present in the RECORDED value but not now is ignored: a
//     future aspect this binary does not know about cannot be compared,
//     and pretending otherwise would refuse every resume after a
//     downgrade.
//
// An empty recorded value is reported by the caller as "no evidence",
// not as drift — the two read differently to an operator.
func copyShapeDrift(recorded, current string) []string {
	rec := parseCopyShape(recorded)
	var drifted []string
	for _, kv := range splitCopyShape(current) {
		got, ok := rec[kv[0]]
		if !ok || got != kv[1] {
			drifted = append(drifted, kv[0])
		}
	}
	return drifted
}

// copyShapeAdvisory splits drifted keys into the ones that REFUSE the
// resume and the ones that only WARN.
//
// The split is a policy, so it is a value rather than a condition
// scattered through the caller: a key refuses when a difference means
// the target holds ROWS the current flags do not describe, and warns
// when it means the target holds DDL the current flags do not describe.
// The second is visible, fixable with one statement, and cannot be
// backfilled-or-not by CDC either way; refusing it would cost a full
// re-copy to correct a constraint.
//
// A key that is neither — which today cannot happen, since the split
// covers every key coldStartCopyShape emits — is treated as REFUSING.
// That is the safe direction for a key someone adds without deciding.
func copyShapeAdvisory(drifted []string) (refusing, advisory []string) {
	for _, k := range drifted {
		switch k {
		case "skip_fks", "views":
			advisory = append(advisory, k)
		default:
			refusing = append(refusing, k)
		}
	}
	return refusing, advisory
}

// copyShapeViewSetHash hashes the effective view set, the same way the
// table set is hashed: the schema's views are already filtered by the
// time the fingerprint is taken, so this compares what would actually
// be created rather than the filter's spelling.
func copyShapeViewSetHash(schema *ir.Schema) string {
	if schema == nil {
		return copyShapeTokenHash("")
	}
	names := make([]string, 0, len(schema.Views))
	for _, v := range schema.Views {
		names = append(names, v.Schema+"."+v.Name)
	}
	sort.Strings(names)
	return copyShapeTokenHash(names...)
}

// copyShapePlural renders "input" / "inputs" for the drift refusal, so
// the message reads as a sentence in both the one-flag and
// several-flags cases.
func copyShapePlural(drifted []string) string {
	if len(drifted) == 1 {
		return "input"
	}
	return "inputs"
}

func parseCopyShape(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range splitCopyShape(s) {
		out[kv[0]] = kv[1]
	}
	return out
}

func splitCopyShape(s string) [][2]string {
	if s == "" {
		return nil
	}
	var out [][2]string
	for _, field := range strings.Split(s, ";") {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		out = append(out, [2]string{k, v})
	}
	return out
}

// copyShapeTableSetHash hashes the effective in-scope table set,
// schema-qualified and ordered, so a widened or narrowed `--tables`
// refuses. Length-prefixed like the redaction fingerprint: a table
// named with the separator cannot forge a boundary.
func copyShapeTableSetHash(schema *ir.Schema) string {
	if schema == nil {
		return copyShapeTokenHash("")
	}
	names := make([]string, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		names = append(names, t.Schema+"."+t.Name)
	}
	sort.Strings(names)
	return copyShapeTokenHash(names...)
}

// copyShapeTypesHash hashes the per-column type and expression
// overrides. Both lists shape the TARGET columns the copy wrote into,
// and the resume does not re-create tables, so a change means run 2's
// declared types describe a table run 1 built differently.
func copyShapeTypesHash(s *Streamer) string {
	tokens := make([]string, 0, len(s.Mappings)*4+len(s.ExpressionMappings)*3)
	for _, m := range s.Mappings {
		tokens = append(tokens, "m", m.Table, m.Column, m.TargetType, fmt.Sprint(m.TargetTypeOptions))
	}
	for _, m := range s.ExpressionMappings {
		tokens = append(tokens, "e", m.Table, m.Column, m.Expression)
	}
	return copyShapeTokenHash(tokens...)
}

// copyShapeRedactHash reuses the redaction registry's own fingerprint —
// the same value the backup manifest records — so the two surfaces
// cannot disagree about what "the same redaction policy" means.
func copyShapeRedactHash(s *Streamer) string {
	if s.Redactor == nil {
		return copyShapeTokenHash("")
	}
	return copyShapeTokenHash("r", s.Redactor.Fingerprint())
}

func copyShapeShardHash(spec ShardColumnSpec) string {
	if !spec.Engaged() {
		return copyShapeTokenHash("")
	}
	return copyShapeTokenHash("s", spec.Name, fmt.Sprint(spec.Value))
}

// copyShapeTokenHash hashes a token list injectively (length-prefixed,
// the ADR-0181 shape) and returns 16 hex characters. The consumer is an
// equality check between two artifacts one operator produced, never an
// adversarial search, so 64 bits is deliberate — the same call the
// redaction fingerprint makes.
func copyShapeTokenHash(tokens ...string) string {
	h := sha256.New()
	for _, t := range tokens {
		fmt.Fprintf(h, "%d:%s\n", len(t), t)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
