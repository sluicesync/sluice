// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// Durable evidence for the no-op maintenance signature heal (audit
// 2026-08-27 A3). The heal (healStaleLineageSignatures in
// internal/pipeline/backup) deliberately re-signs a non-verifying chain
// under the SAME key — the crash-stale recovery the SIGNATURE-MISSING
// remedy prescribes — but a re-sign is also exactly what laundering a
// tampered catalog looks like, and pre-A3 the only trace was one
// transient WARN on a typically-cron-driven run: the non-verifying
// signature (the forensic evidence of WHAT failed to verify) was
// overwritten, and every later `backup verify` reported all-valid
// forever. Two artifacts fix that, both written BEFORE ResignLineage
// runs:
//
//   - the pre-heal `lineage.json.sig` is preserved verbatim at
//     `lineage.json.sig.pre-heal-<unix-nanos>` (raw byte copy, so the
//     failed MAC and its recorded KeyID/scheme survive for offline
//     inspection against a pristine backup of the catalog);
//   - one [HealRecord] line is appended to `maintenance-heal.log`
//     (JSONL), carrying when/what/which-key/why-verify-failed.
//
// `backup verify` surfaces the log's presence as an informational line —
// never a failure: a healed chain is expected after a documented
// crash-recovery, and the record exists precisely so an operator can
// decide whether the heal was one of those.
//
// CONCURRENCY, and the two halves differ (audit 2026-08-31 C-3). This
// file used to claim "maintenance runs are the only writer and hold the
// chain maintenance flow single-threaded, so lost-update is not a live
// concern". Nothing enforced that: the ADR-0160 concurrent-writer guard
// arms inside WriteLineageCatalog, and the heal runs at compaction's and
// prune's NO-OP doors, which return before any catalog write, so no chain
// generation is ever claimed around a heal. A duplicated 03:00 cron
// running `backup prune` twice is exactly the shape chain_guard.go's own
// doc names. So:
//
//   - [PreserveLineageSigForHeal] — ENFORCED. The preserved copy is
//     written with the create-only [irbackup.ConditionalPutter] claim, so
//     the no-overwrite promise is the store's, not a probe's. Two heals
//     that pick the same timestamp (the wall clock advances in ~505 µs
//     ticks on Windows, so this is not the ns-resolution long shot it
//     looks like) cannot both believe the path was free — the loser bumps.
//     This is the half that matters: the preserved `.sig` is the
//     BYTE-VERBATIM forensic evidence.
//   - [AppendHealRecord] — read-modify-write, and still lost-update-prone
//     under a genuine concurrent heal. PutIfAbsent cannot express an
//     append: it claims a path exactly once, and the log is a single fixed
//     path that must accumulate. The lost-update-free form is one object
//     per record (`maintenance-heal/<ts>-<rand>.json`, read via List),
//     which is a FORMAT change to a shipped artifact with its own reader
//     and existing on-disk chains — deliberately not taken in this patch,
//     and filed. The residual is bounded: two racing heals leave two
//     preserved `.sig` copies (the evidence) and possibly one record (the
//     description), so the count under-reports while the evidence does
//     not.
//
// The optional [irbackup.Appender] capability exists (LocalStore
// implements it; the progress sidecar uses it) but is deliberately not
// taken here either: the log must behave identically on cloud stores that
// lack the capability, and one code path is easier to keep
// evidence-correct than two (A3-APPENDER-COMMENT, 2026-08-27; the prior
// wording claimed no append primitive existed, which was stale). Note it
// would not close the race anyway — concurrent O_APPEND writes interleave
// whole lines, they do not serialise a read-modify-write.
//
// A torn write is loud at read time (the JSONL parse refuses the line and
// [ReadHealRecords] counts it).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// MaintenanceHealLogFileName is the append-only JSONL record of every
// no-op maintenance signature heal performed on the chain.
const MaintenanceHealLogFileName = "maintenance-heal.log"

// PreHealLineageSigPrefix prefixes the preserved pre-heal copies of
// lineage.json.sig. The suffix is the heal's UnixNano timestamp.
const PreHealLineageSigPrefix = LineageSigFileName + ".pre-heal-"

// healLogMaxBytes caps how much of maintenance-heal.log [ReadHealRecords]
// will read (16 MiB). A chain sees 0–1 heals ever and each record is a few
// hundred bytes, so any log approaching this is already pathological; the
// cap exists so a store object of arbitrary size cannot be read into
// memory by a verify. Reaching it is reported as a defect (the tail was
// NOT read), never silently.
//
// It replaces a 1 MiB per-LINE ceiling that came from bufio.Scanner's
// ErrTooLong: with the whole-body read below there is no per-line limit to
// hit, so a pathologically long VerifyFailure now reads back instead of
// making the log unreadable forever (the A3-SCANNER-CAP concern, closed
// rather than raised).
const healLogMaxBytes = 16 << 20

// HealRecord is one durable maintenance-heal entry. Append-only,
// forward-compatible (readers ignore unknown fields).
type HealRecord struct {
	// HealedAt is the wall-clock UTC time the heal ran.
	HealedAt time.Time `json:"healed_at"`

	// Operation names the maintenance verb whose no-op door healed
	// (e.g. "backup compact", "prune").
	Operation string `json:"operation"`

	// KeyID is the signing key fingerprint the chain was re-signed
	// under (== the pre-heal signature's recorded KeyID — the wrong-key
	// guard refuses before a record is ever written otherwise).
	KeyID string `json:"key_id"`

	// VerifyFailure is the verification error that triggered the heal —
	// the one-shot WARN's payload, made durable. Error text can carry
	// arbitrary bytes (a tampered .sig's fields leak into the message),
	// and encoding/json silently replaces invalid UTF-8 with U+FFFD —
	// acceptable HERE because this record is descriptive provenance; the
	// BYTE-VERBATIM forensic evidence is the preserved pre-heal .sig
	// copy, never this log (VF review 2026-08-27).
	VerifyFailure string `json:"verify_failure"`

	// PreservedSig is the store path of the preserved pre-heal
	// lineage.json.sig copy.
	PreservedSig string `json:"preserved_sig"`
}

// PreserveLineageSigForHeal copies the CURRENT (non-verifying)
// lineage.json.sig aside before a heal overwrites it, returning the
// preserved copy's path. Raw byte copy — the evidence must survive
// exactly as recorded, not re-marshalled. Errors if the signature object
// vanished since the heal's key check read it (fail closed: a heal must
// not destroy evidence it cannot preserve).
func PreserveLineageSigForHeal(ctx context.Context, store irbackup.Store, now time.Time) (string, error) {
	rc, err := store.Get(ctx, LineageSigFileName)
	if err != nil {
		return "", fmt.Errorf("read %q for pre-heal preservation: %w", LineageSigFileName, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("read %q for pre-heal preservation: %w", LineageSigFileName, err)
	}
	// Never overwrite prior evidence: the wall clock's granularity (coarse
	// on Windows — measured at ~505 µs ticks, so "nanosecond" precision
	// buys nothing) can hand two heals the same UnixNano, and evidence
	// preservation is exactly the place an overwrite must be impossible.
	//
	// The no-overwrite promise is enforced by the STORE, not by a probe
	// (audit 2026-08-31 C-3): the create-only [irbackup.ConditionalPutter]
	// claim IS the arbitration, so ErrPathExists is the bump signal and
	// two processes racing on the same timestamp cannot both think the
	// path was free. The previous probe-then-Put was a TOCTOU across
	// processes — and the doc that dismissed concurrent heals as
	// impossible was itself unenforced (see the note at the top of this
	// file). Heals are rare, so the loop is effectively 0–1 iterations.
	ts := now.UnixNano()
	path := fmt.Sprintf("%s%d", PreHealLineageSigPrefix, ts)
	cp, conditional := store.(irbackup.ConditionalPutter)
	for {
		if !conditional {
			// Degradation path for a store without the optional
			// capability. Both stores sluice ships (LocalStore, BlobStore)
			// implement it, so this is reached only by a third-party or
			// test store; it keeps the old probe-then-Put semantics rather
			// than refusing a heal outright.
			exists, err := store.Exists(ctx, path)
			if err != nil {
				return "", fmt.Errorf("probe pre-heal preservation path %q: %w", path, err)
			}
			if !exists {
				if err := store.Put(ctx, path, bytes.NewReader(body)); err != nil {
					return "", fmt.Errorf("preserve pre-heal signature at %q: %w", path, err)
				}
				return path, nil
			}
		} else {
			err := cp.PutIfAbsent(ctx, path, bytes.NewReader(body))
			if err == nil {
				return path, nil
			}
			if !errors.Is(err, irbackup.ErrPathExists) {
				return "", fmt.Errorf("preserve pre-heal signature at %q: %w", path, err)
			}
		}
		ts++
		path = fmt.Sprintf("%s%d", PreHealLineageSigPrefix, ts)
	}
}

// AppendHealRecord appends rec to maintenance-heal.log (creating it on
// first heal).
func AppendHealRecord(ctx context.Context, store irbackup.Store, rec HealRecord) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal heal record: %w", err)
	}
	body := make([]byte, 0, len(line)+1)
	exists, err := store.Exists(ctx, MaintenanceHealLogFileName)
	if err != nil {
		return fmt.Errorf("inspect %q: %w", MaintenanceHealLogFileName, err)
	}
	if exists {
		rc, err := store.Get(ctx, MaintenanceHealLogFileName)
		if err != nil {
			return fmt.Errorf("read %q: %w", MaintenanceHealLogFileName, err)
		}
		body, err = io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return fmt.Errorf("read %q: %w", MaintenanceHealLogFileName, err)
		}
	}
	// Belt (A3-APPEND-NEWLINE-GUARD): a prior body whose tail lost its
	// newline — unreachable under this function's own writes, every append
	// ends in '\n' via one atomic Put — must not have the new record glued
	// onto its last line, which would corrupt BOTH records where a
	// separator keeps any tear loud and local at read time.
	if len(body) > 0 && body[len(body)-1] != '\n' {
		body = append(body, '\n')
	}
	body = append(body, line...)
	body = append(body, '\n')
	if err := store.Put(ctx, MaintenanceHealLogFileName, bytes.NewReader(body)); err != nil {
		return fmt.Errorf("append %q: %w", MaintenanceHealLogFileName, err)
	}
	return nil
}

// HealLogDefect names one piece of maintenance-heal.log that could not be
// decoded. Defects are REPORTED, never swallowed and never fatal — see
// [ReadHealRecords] for why the posture is per-line rather than refuse-all.
type HealLogDefect struct {
	// Line is the 1-based line number, or 0 for a whole-file defect
	// (e.g. the body exceeded [healLogMaxBytes]).
	Line int

	// Reason is the operator-facing description of what was unreadable.
	Reason string
}

// ReadHealRecords reads and decodes maintenance-heal.log, returning the
// records that parsed and one [HealLogDefect] per piece that did not. An
// absent log is (nil, nil, nil) — the common healthy-chain case. The error
// return is reserved for STORE failures (probe / open / read); malformed
// CONTENT is never an error.
//
// POSTURE — per-line skip with a counted, loud defect, changed from
// refuse-the-whole-file (audit 2026-08-31 SEC-7). The evidence-integrity
// argument genuinely cuts both ways, so the reasoning is recorded here:
//
// Refuse-all treats the log as one indivisible artifact — a torn record
// means "something is wrong with this evidence, stop". Attractive, but it
// hands a strictly WEAKER adversary than the one who can cause a heal
// (store write access, no signing key) a one-byte lever: append a single
// non-JSON byte and EVERY prior heal record becomes invisible, behind a
// warning that reads like a benign torn write, at exit 0. That converts
// "evidence exists" into "a warning appeared", which is the outcome the A3
// artifacts were built to prevent.
//
// Per-line skip keeps what refuse-all actually bought — an unreadable line
// is still surfaced loudly, with its line number and a count — and removes
// the lever, because hiding a record now costs corrupting THAT record
// rather than one byte anywhere. It is not the "skip-branch without proof"
// the new-surface checklist forbids: nothing is dropped quietly, the
// skipped content is counted and named, and the caller
// (backup.reportMaintenanceHeals) WARNs with the remedy. The format is
// JSONL precisely because records are independent.
//
// Neither posture protects against a store-writer who rewrites the whole
// log, and neither is meant to: the log is descriptive provenance. The
// byte-verbatim forensic evidence is the preserved `lineage.json.sig.pre-
// heal-*` copies, which are separate objects.
func ReadHealRecords(ctx context.Context, store irbackup.Store) ([]HealRecord, []HealLogDefect, error) {
	exists, err := store.Exists(ctx, MaintenanceHealLogFileName)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect %q: %w", MaintenanceHealLogFileName, err)
	}
	if !exists {
		return nil, nil, nil
	}
	rc, err := store.Get(ctx, MaintenanceHealLogFileName)
	if err != nil {
		return nil, nil, fmt.Errorf("read %q: %w", MaintenanceHealLogFileName, err)
	}
	defer func() { _ = rc.Close() }()
	// Whole body, capped. Reading it in one piece (rather than through a
	// bufio.Scanner) is what makes an overlong line a per-line defect
	// instead of a hard stop that hides every record after it.
	body, err := io.ReadAll(io.LimitReader(rc, healLogMaxBytes+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read %q: %w", MaintenanceHealLogFileName, err)
	}
	var defects []HealLogDefect
	if len(body) > healLogMaxBytes {
		body = body[:healLogMaxBytes]
		defects = append(defects, HealLogDefect{Reason: fmt.Sprintf(
			"the log exceeds the %d-byte read cap; everything past it was NOT read", healLogMaxBytes,
		)})
	}
	var recs []HealRecord
	for i, raw := range bytes.Split(body, []byte("\n")) {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			// Provably empty — the separator after the final record, or a
			// blank the newline guard inserted. Nothing to skip.
			continue
		}
		var rec HealRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			defects = append(defects, HealLogDefect{Line: i + 1, Reason: err.Error()})
			continue
		}
		recs = append(recs, rec)
	}
	return recs, defects, nil
}
