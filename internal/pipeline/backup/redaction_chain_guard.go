// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"strings"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The redaction-posture guards.
//
// `backup full --redact` applies redaction at CHUNK-WRITE time, so the
// archive it writes is PII-clean. `backup incremental` and `backup
// stream` apply NONE — their change events come off the CDC pump
// verbatim, and neither command exposes a `--redact` flag. So a
// redacted full followed by an incremental restored PLAINTEXT for every
// row touched after the full, at exit 0, with nothing refusing and
// nothing warning — while `backup full --redact`'s own help text told
// the operator "restore from a redacted chain produces the same
// redacted shape".
//
// These guards are the LOUD-FAILURE half, not the feature: sluice still
// cannot redact an incremental. What it can do is stop silently
// producing a chain that leaks, at every door where the posture could
// change:
//
//   - [RefuseRedactedChainExtension] — the write door. `backup
//     incremental` and `backup stream` refuse to extend a chain whose
//     parent carries the marker. Because it refuses the FIRST such
//     link, a marker is never more than one link from the tip, which is
//     what makes checking the IMMEDIATE PARENT exhaustive rather than a
//     sample: no chain a current release wrote can have a redacted
//     ancestor further back with unredacted links in between.
//   - [refuseResumeUnderDifferentRedaction] — the same-manifest door. A
//     resumed `backup full` keeps the interrupted attempt's completed
//     tables; resuming under different rules would leave ONE manifest
//     listing chunks written under two policies.
//   - [refuseMixedRedactionChain] — the read door, on restore /
//     `backup verify` / export. The write doors cannot reach a chain an
//     OLDER binary extended (it ignores the marker) or one assembled by
//     hand, so the mix is refused on the way out too.
const redactionPostureRemedy = "Decide the chain's posture and re-take it. To keep a chain PII-clean, take a fresh `sluice backup full --redact ...` — redaction happens at chunk-write time and only `backup full` does it, so a redacted chain is a series of fulls rather than a full plus incrementals. To accept plaintext in the change window, start a fresh UNREDACTED chain (`sluice backup full` with no --redact, into a new destination) and take incrementals off that, so the choice is explicit. Background: docs/operator/error-codes.md, SLUICE-E-BACKUP-REDACTED-CHAIN"

// describeRedaction renders a marker for an operator-facing message.
// Never renders the rules themselves — the marker does not carry them
// (see [irbackup.RedactionInfo]).
func describeRedaction(r *irbackup.RedactionInfo) string {
	if r == nil {
		return "not redacted"
	}
	if r.Fingerprint == "" {
		return fmt.Sprintf("redacted (%d rule(s), unfingerprinted)", r.RuleCount)
	}
	return fmt.Sprintf("redacted (%d rule(s), policy %s)", r.RuleCount, r.Fingerprint)
}

// RefuseRedactedChainExtension refuses to extend a chain whose parent
// manifest records that its chunks were redacted. op names the command
// for the message ("backup incremental" / "backup stream").
//
// Exported because the two callers live in the parent `pipeline`
// package (incremental.go / stream.go) while the message, the code and
// the remedy belong next to the other backup refusals. A nil parent or
// a parent with no marker returns nil — the overwhelmingly common case,
// and the one that must never turn into a false refusal.
func RefuseRedactedChainExtension(parent *irbackup.Manifest, parentPath, op string) error {
	if parent == nil || parent.Redaction == nil {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRedactedChain, redactionPostureRemedy,
		fmt.Errorf("%s: the parent manifest %q is %s, and %s does not redact — its change events come off the source's CDC pump verbatim, so every row touched after the full would restore in PLAINTEXT out of a chain whose full was written to be PII-clean; refusing before the window opens",
			op, parentPath, describeRedaction(parent.Redaction), op))
}

// refuseResumeUnderDifferentRedaction refuses a `backup full` resume
// whose redaction policy differs from the interrupted attempt's.
//
// The prior attempt's completed tables are KEPT verbatim on a resume, so
// a policy change here does not re-redact them: it writes the remaining
// tables under the new rules and finalizes one manifest listing both.
// Every table would be clean by the policy that wrote it and the archive
// clean by neither — and the manifest can only record one marker, so
// whichever it recorded would be a lie about half its chunks.
//
// Dropping `--redact` entirely on the resume is the shape most likely to
// happen by accident (a shorter re-run of the command), and it is the
// worst one: the marker would say the archive is PII-clean while the
// tables the resume streamed carry source values.
func refuseResumeUnderDifferentRedaction(prior *irbackup.Manifest, cur *irbackup.RedactionInfo, priorPath string) error {
	if prior == nil {
		return nil
	}
	if redactionMarkersAgree(prior.Redaction, cur) {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRedactedChain,
		"Re-run with the SAME --redact rules the interrupted attempt used (the message names both policies' fingerprints), or discard that attempt with --force-overwrite and start the backup again under the policy you want. Background: docs/operator/error-codes.md, SLUICE-E-BACKUP-REDACTED-CHAIN",
		fmt.Errorf("backup: the interrupted attempt recorded in %q is %s and this run is %s — a resume KEEPS the prior attempt's completed tables, so continuing would write one manifest whose chunks were redacted under two different policies (each table clean by its own rules, the archive clean by neither); refusing before anything is read",
			priorPath, describeRedaction(prior.Redaction), describeRedaction(cur)))
}

// redactionMarkersAgree reports whether two markers describe the same
// redaction posture. Both nil (unredacted) agrees; one nil disagrees;
// two present agree only on an equal fingerprint AND rule count.
//
// Equality-only by design — there is no ordering on redaction policies
// and no "close enough". An unfingerprinted marker (a hand-edited or
// future-shaped manifest) therefore agrees only with another
// unfingerprinted marker of the same rule count, which is the
// conservative direction: it can over-refuse, never under-refuse.
func redactionMarkersAgree(a, b *irbackup.RedactionInfo) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// refuseMixedRedactionChain refuses a chain whose links disagree about
// redaction, BEFORE anything is read out of it. It is the read-side
// backstop in [restoreManifestIntegrityPreflights], so it reaches
// restore, chain restore, `backup verify` and `export-as-parquet`
// alike.
//
// The write doors above make this shape unreachable for a chain a
// current release produced. It is reachable for one an OLDER binary
// extended — that binary ignores the unknown marker, which is exactly
// why a redacted manifest is stamped [irbackup.FormatVersionRedaction]
// so it refuses the manifest outright — and for a lineage assembled by
// hand out of directories from two different backups. Neither is exotic
// enough to leave to the writer's good behaviour when the failure mode
// is plaintext PII landing on a restore target at exit 0.
//
// The check is DISAGREEMENT, not "any link unredacted": a chain of
// consistently-unredacted links is the ordinary case and must pass, and
// a chain that is redacted end to end (not producible today, since
// incrementals never redact) is internally coherent and also passes.
func refuseMixedRedactionChain(links []lineage.SegmentRecord) error {
	var (
		witness   *irbackup.RedactionInfo
		witnessAt string
		seen      bool
	)
	for i := range links {
		m := links[i].Manifest
		if m == nil {
			continue
		}
		if !seen {
			witness, witnessAt, seen = m.Redaction, links[i].Path, true
			continue
		}
		if redactionMarkersAgree(witness, m.Redaction) {
			continue
		}
		// Name the redacted side first regardless of walk order — that is
		// the link whose contents the operator believed were clean.
		clean, cleanAt := witnessAt, describeRedaction(witness)
		other, otherAt := links[i].Path, describeRedaction(m.Redaction)
		return sluicecode.Wrap(sluicecode.CodeBackupRedactedChain, redactionPostureRemedy,
			fmt.Errorf("chain: link %s is %s while link %s is %s — the chain mixes redaction policies, so restoring it would land the redacted link's columns in PLAINTEXT wherever a later link touched those rows; refusing before any data is read",
				clean, cleanAt, other, otherAt))
	}
	return nil
}

// redactionMarker renders this run's redaction policy as the marker the
// manifest records, or nil when no rules are configured. nil is the
// no-redaction hot path and the pre-v0.144.0 on-disk shape (the field is
// `omitempty`), so an unredacted backup writes byte-identical bytes to
// what it always did.
func (b *Backup) redactionMarker() *irbackup.RedactionInfo {
	if b.Redactor.Empty() {
		return nil
	}
	return &irbackup.RedactionInfo{
		RuleCount:   len(b.Redactor.Rules()),
		Fingerprint: b.Redactor.Fingerprint(),
	}
}

// redactionSummaryForLog renders the marker for the backup's own INFO
// line. Separate from [describeRedaction] only so the log stays terse.
func redactionSummaryForLog(r *irbackup.RedactionInfo) string {
	if r == nil {
		return "none"
	}
	return strings.TrimPrefix(describeRedaction(r), "redacted ")
}
