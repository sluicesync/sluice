// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
	"sluicesync.dev/sluice/internal/translate"
)

// The Bug 243 gate: a chain recorded by a pre-v0.120.0 MySQL-family
// reader can carry a MANGLED expression — a string literal that never
// closes — and emitting that recorded DDL failed `restore` mid-run with
// the target's raw parse error while `backup verify` passed the chain.
// The detector is [ir.TableExpressionLexProblems] (structural, never a
// repair) — joined by the residue arm, [ir.TableExpressionBackslashLiterals]
// keyed on [recordedByPreEscapeFixMySQLReader]: the same era recorded
// literal backslashes in MySQL's doubled spelling, structurally valid
// but silently WRONG on every current target. Both arms report through
// the same (code, renderer) pair. This file is the wiring into the
// doors:
//
//   - `restore` (and therefore every chain segment's full, which runs
//     through the Restore path): a pre-DDL refusal, AFTER the table
//     filter — deliberately unlike the shape preflights, which run
//     pre-filter — because the remedy for a source that no longer
//     exists is `--exclude-table=<affected>` to salvage every other
//     table, and a filter-blind gate would make its own remedy
//     impossible.
//   - `chain restore`'s schema deltas: filter-aware BY CONSTRUCTION
//     since Bug 244 — the deltas are filtered before the door runs, so
//     `--exclude-table=<affected>` releases it exactly like the restore
//     door. (Pre-Bug-244 this read "unconditional because deltas apply
//     unfiltered"; the premise changed with the fix.)
//   - `backup verify`: chain-level, unfiltered — verify predicts the
//     UNFILTERED restore (the Bug 217/218 doctrine), so it refuses any
//     chain a plain `restore` would refuse.
//
// `backup incremental` WARNs instead (see incremental.go), and `backup
// compact` / `backup prune` WARN through
// [warnIfChainRecordedSchemaMalformed] below: their own work is valid,
// and refusing would stop an operation an operator may still want — but
// silently extending or maintaining an un-restorable chain is how
// Bug 243 presented, so the warning names the code.
//
// The broker's `--reset-target-data` cold start is the FIFTH refusal
// door, exported as [RefuseChainRecordedSchemaMalformed] (audit
// 2026-08-11 BRK-1): it DROPS the target's tables off the cached
// manifest BEFORE running ChainRestore, whose own doors would then
// refuse the chain — converting a refusable chain into a destroyed
// target. The broker runs the exported door over the whole chain's
// manifests before its drop, on the same detector and renderer as
// every other door.

// recordedSchemaMalformedHint is the operator remedy, shared by every
// door so one shape reports one (code, hint) pair.
const recordedSchemaMalformedHint = "take a fresh `backup full` of the live source with sluice v0.120.0 or newer " +
	"(the current reader records these expressions correctly); if the source is gone, " +
	"`restore --exclude-table=<the named table>` recovers every other table — the data chunks are intact"

// recordedByPreEscapeFixMySQLReader reports whether m was recorded by a
// MySQL-family reader older than v0.120.0 — the era whose recorded
// expression text spells a literal backslash in MySQL's DOUBLED form
// (`'a\\d'` meaning one backslash). That spelling emits WRONG on every
// current target (the Bug 243 residue, corrected during closure: the
// filing assumed a MySQL-target restore stays correct, but the
// post-v0.120.0 MySQL emit boundary assumes the bare contract and
// re-doubles — escapeExprLiteralBackslashes — so the same-engine round
// trip silently gains a backslash too; PostgreSQL under
// standard_conforming_strings and SQLite read the doubled form as two
// characters directly).
//
// Coverage boundary, stated because a gate must not read broader than
// it is: the version key requires a PARSEABLE "X.Y.Z" SluiceVersion.
// Every released binary has stamped one since backup shipped (v0.15.0,
// 5550c230 — the CLI passes it unconditionally), so every
// released-binary chain is covered. A chain written by a from-source
// build stamps "dev" and is NOT gated — treating unparseable as old
// would refuse chains that dev builds write TODAY, and the printed
// remedy (a fresh backup, which would also stamp "dev") could never
// release it: an unrunnable remedy, the Bug 245/246/247 class.
//
// Second stated boundary: within the era the arm cannot distinguish a
// doubled spelling from a genuinely-bare (already-correct) backslash —
// e.g. a NO_BACKSLASH_ESCAPES-shaped source rendering — so a correct
// old recording with a backslash literal refuses too. That is the loud
// direction on a rare shape, with the same working remedies (fresh
// backup, or --exclude-table), and preferable to guessing which
// spelling the recording intended.
func recordedByPreEscapeFixMySQLReader(m *irbackup.Manifest) bool {
	if m == nil || !translate.IsMySQLFamily(m.SourceEngine) {
		return false
	}
	major, minor, ok := parseSluiceVersionMajorMinor(m.SluiceVersion)
	if !ok {
		return false
	}
	return major == 0 && minor < 120
}

// parseSluiceVersionMajorMinor parses the leading "X.Y" of a manifest's
// recorded SluiceVersion ("0.119.3", "v0.99.292"). ok=false for
// anything that does not start with two dot-separated integers ("dev",
// "").
func parseSluiceVersionMajorMinor(v string) (major, minor int, ok bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	maj, err1 := strconv.Atoi(parts[0])
	mnr, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || maj < 0 || mnr < 0 {
		return 0, 0, false
	}
	return maj, mnr, true
}

// describeDoubledBackslashProblem prefixes an IR backslash-literal site
// with the era diagnosis, so every door's refusal explains WHY a
// structurally valid expression is refused.
func describeDoubledBackslashProblem(m *irbackup.Manifest, site string) string {
	v := strings.TrimSpace(m.SluiceVersion)
	return site + " — recorded by sluice " + v + ", whose MySQL-family reader kept MySQL's doubled backslash spelling; " +
		"this binary's writers assume the bare spelling, so the restored expression would silently mean a different predicate"
}

// ManifestRecordedSchemaProblems collects the problems of everything a
// manifest can ask a restore to emit — its full schema (when present)
// and every schema-delta table it adds or alters — through BOTH arms:
// the structural lex check (an unterminated literal, the apostrophe
// mangle), and, for manifests a pre-v0.120.0 MySQL-family reader
// recorded, the doubled-backslash-literal arm
// ([ir.SchemaExpressionBackslashLiterals]; see
// [recordedByPreEscapeFixMySQLReader] for why the era makes a
// structurally valid literal wrong).
func ManifestRecordedSchemaProblems(m *irbackup.Manifest) []string {
	if m == nil {
		return nil
	}
	problems := ir.SchemaExpressionLexProblems(m.Schema)
	for _, d := range m.SchemaDelta {
		problems = append(problems, ir.TableExpressionLexProblems(d.After)...)
	}
	if recordedByPreEscapeFixMySQLReader(m) {
		for _, site := range ir.SchemaExpressionBackslashLiterals(m.Schema) {
			problems = append(problems, describeDoubledBackslashProblem(m, site))
		}
		for _, d := range m.SchemaDelta {
			for _, site := range ir.TableExpressionBackslashLiterals(d.After) {
				problems = append(problems, describeDoubledBackslashProblem(m, site))
			}
		}
	}
	return problems
}

// warnIfChainRecordedSchemaMalformed WARNs — once, on the first affected
// manifest — when the chain's restorable window carries a recorded
// schema that cannot be emitted as valid SQL (Bug 243's mangle class).
// The maintenance-door sibling of `backup incremental`'s
// warnIfParentChainUnrestorable (internal/pipeline/incremental.go):
// compact and prune rewrite/retire chain state at exit 0, and silently
// maintaining a chain whose restore is already broken is the exact shape
// Bug 243 presented. A WARN, deliberately not a refusal — the
// maintenance operation's own work is valid, and restore/verify hold the
// refusal.
//
// Coverage, stated per the gate rule: every RESTORABLE segment's full
// and incremental manifests (Schema + SchemaDelta.After via
// [ManifestRecordedSchemaProblems]); retired segments sit outside the
// restore path and are unscanned. A manifest this scan cannot read is
// skipped without failing the maintenance op. The skip suppresses at
// most this ADVISORY, never a refusal: prune's own read of a restorable
// incremental hard-fails on the same file, and an unreadable manifest
// is refused loudly by restore/verify — but NOT every scanned manifest
// is re-read by every op (prune does not read kept segment fulls), so
// the skip is justified by the advisory's own stakes, not by a
// guaranteed re-read.
func warnIfChainRecordedSchemaMalformed(ctx context.Context, op string, store irbackup.Store, cat *lineage.Catalog) {
	if cat == nil {
		return
	}
	for si := max(cat.RestorableFromSegment, 0); si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		ss := seg.Store(store)
		paths := make([]string, 0, 1+len(seg.Incrementals))
		paths = append(paths, seg.FullManifestPath)
		paths = append(paths, seg.Incrementals...)
		for _, p := range paths {
			m, err := lineage.ReadManifestAt(ctx, ss, p)
			if err != nil {
				continue
			}
			problems := ManifestRecordedSchemaProblems(m)
			if len(problems) == 0 {
				continue
			}
			slog.WarnContext(
				ctx,
				op+": this chain's recorded schema carries expressions that cannot be emitted as valid SQL — "+
					"the chain will NOT restore until a fresh `backup full` re-records the schema "+
					"(recorded by a sluice older than v0.120.0; restore/verify refuse it as SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED)",
				slog.String("manifest", p),
				slog.Int("segment", si),
				slog.Int("affected_expressions", len(problems)),
				slog.String("first", problems[0]),
			)
			return
		}
	}
}

// RefuseChainRecordedSchemaMalformed is the pre-DDL door for callers
// that take a DESTRUCTIVE step before [ChainRestore]'s own doors can
// fire (audit 2026-08-11 BRK-1 — the Bug 243 sweep's missed fifth
// door): the broker's --reset-target-data cold start drops the
// target's tables off the cached manifest before ChainRestore.Run's
// refusals run, so a chain the restore will refuse must refuse HERE
// first, while the target still holds its data. It scans everything
// the given manifests can ask a restore to emit — each full's schema
// and each incremental's schema-delta tables — through the same
// detector and renderer as every other door, so the doors cannot
// disagree about what counts as malformed. Deliberately unfiltered:
// this caller restores whole chains (its ChainRestore carries no
// table filter), so there is no salvage subset to honour.
func RefuseChainRecordedSchemaMalformed(mode string, manifests []*irbackup.Manifest) error {
	problems := make([]string, 0, len(manifests))
	for _, m := range manifests {
		problems = append(problems, ManifestRecordedSchemaProblems(m)...)
	}
	return refuseMalformedRecordedSchema(mode, problems)
}

// refuseMalformedRecordedSchema renders the coded refusal for a
// non-empty problem list. mode names the door ("restore", "chain
// restore", "verify") so the message reads in context.
func refuseMalformedRecordedSchema(mode string, problems []string) error {
	if len(problems) == 0 {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRecordedSchemaMalformed,
		recordedSchemaMalformedHint,
		fmt.Errorf("%s: the chain's recorded schema cannot be emitted faithfully "+
			"(recorded by a sluice older than v0.120.0, whose MySQL-family reader mangled apostrophe-carrying "+
			"expressions and kept literal backslashes in MySQL's doubled spelling):\n  %s",
			mode, strings.Join(problems, "\n  ")))
}

// filteredSchemaLexProblems is the restore-door variant: only tables the
// run will actually emit are checked, so `--exclude-table` on an
// affected table is a working remedy rather than a re-refusal. m is the
// manifest the schema came from — it keys the doubled-backslash arm
// (see [ManifestRecordedSchemaProblems]); the two doors scan the same
// arms so restore and verify cannot disagree about what refuses.
func filteredSchemaLexProblems(m *irbackup.Manifest, s *ir.Schema, filter migcore.TableFilter) []string {
	if s == nil {
		return nil
	}
	backslashEra := recordedByPreEscapeFixMySQLReader(m)
	var out []string
	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		if !filter.IsEmpty() && !filter.Allows(t.Name) {
			continue
		}
		out = append(out, ir.TableExpressionLexProblems(t)...)
		if backslashEra {
			for _, site := range ir.TableExpressionBackslashLiterals(t) {
				out = append(out, describeDoubledBackslashProblem(m, site))
			}
		}
	}
	return out
}
