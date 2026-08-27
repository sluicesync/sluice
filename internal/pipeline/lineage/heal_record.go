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
// The append is read-modify-write. The optional [irbackup.Appender]
// capability exists (LocalStore implements it; the progress sidecar uses
// it) but is deliberately not taken here: heals are rare — a chain sees
// 0–1 records ever — the log must behave identically on cloud stores
// that lack the capability, and one code path is easier to keep
// evidence-correct than two (A3-APPENDER-COMMENT, 2026-08-27; the prior
// wording claimed no append primitive existed, which was stale).
// Maintenance runs are the only writer and hold the chain maintenance
// flow single-threaded, so lost-update is not a live concern; a torn
// write is loud at read time (the JSONL parse refuses).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

// healLogMaxLineBytes is [ReadHealRecords]' per-line ceiling (1 MiB —
// see the posture note at the Scanner). A line past it refuses loudly.
const healLogMaxLineBytes = 1 << 20

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
	// on Windows) can hand two heals the same UnixNano, and evidence
	// preservation is exactly the place an overwrite must be impossible —
	// probe and bump until the path is free (heals are rare; the loop is
	// effectively 0–1 iterations).
	ts := now.UnixNano()
	path := fmt.Sprintf("%s%d", PreHealLineageSigPrefix, ts)
	for {
		exists, err := store.Exists(ctx, path)
		if err != nil {
			return "", fmt.Errorf("probe pre-heal preservation path %q: %w", path, err)
		}
		if !exists {
			break
		}
		ts++
		path = fmt.Sprintf("%s%d", PreHealLineageSigPrefix, ts)
	}
	if err := store.Put(ctx, path, bytes.NewReader(body)); err != nil {
		return "", fmt.Errorf("preserve pre-heal signature at %q: %w", path, err)
	}
	return path, nil
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

// ReadHealRecords reads and decodes maintenance-heal.log. An absent log
// is (nil, nil) — the common healthy-chain case. A malformed line is a
// loud error, never a partial silent read: the log is evidence, and a
// truncated/torn record is itself worth surfacing.
func ReadHealRecords(ctx context.Context, store irbackup.Store) ([]HealRecord, error) {
	exists, err := store.Exists(ctx, MaintenanceHealLogFileName)
	if err != nil {
		return nil, fmt.Errorf("inspect %q: %w", MaintenanceHealLogFileName, err)
	}
	if !exists {
		return nil, nil
	}
	rc, err := store.Get(ctx, MaintenanceHealLogFileName)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", MaintenanceHealLogFileName, err)
	}
	defer func() { _ = rc.Close() }()
	var recs []HealRecord
	sc := bufio.NewScanner(rc)
	// A VerifyFailure carries arbitrary error text, and bufio.Scanner's
	// default 64 KiB line cap would make a log holding one long failure
	// unreadable FOREVER — ErrTooLong on every read (A3-SCANNER-CAP).
	// Raise the cap to 1 MiB. POSTURE: a line past 1 MiB still refuses
	// loudly via sc.Err() below rather than being skipped — the log is
	// evidence, and 1 MiB is orders of magnitude beyond any real verify
	// failure, so hitting the cap means something is wrong enough to stop
	// for. Pinned both ways by TestReadHealRecords_LongLines.
	sc.Buffer(make([]byte, 0, 64*1024), healLogMaxLineBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec HealRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("decode %q record %d: %w", MaintenanceHealLogFileName, len(recs)+1, err)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %q: %w", MaintenanceHealLogFileName, err)
	}
	return recs, nil
}
