// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// MariaDB lineage binding for resume positions (v0.138.0, audit
// 2026-09-01 SLM-2's MariaDB arm).
//
// # Why a MariaDB position needs its own binding
//
// MySQL binds a GTID position to a lineage through the source UUID inside
// every GTID and GTID_SUBSET(resume, @@gtid_executed). MariaDB GTIDs are
// (domain, server_id, seq): no instance identity, no @@server_uuid, and
// the server refuses a foreign position only when the resume domain is
// present with a DIFFERENT server_id or a HIGHER seq. Measured on
// mariadb:11.4 through sluice's own wire path: a foreign instance with a
// different gtid_domain_id, and a rebuilt instance with the same server_id
// whose own history reads the same "0-1-3", both ACCEPT the position and
// stream their entire history as its continuation — the whole-history
// replay into a `backup incremental` chain or a `sync` apply at exit 0.
//
// # The binding
//
// Every MariaDB capture door records, under the same lock it reads the
// position with, the binlog (file, offset) and BINLOG_GTID_POS(file,
// offset) — the GTID state at that byte of THIS server's binlog. On
// resume the source is asked the same question: the same lineage answers
// the same set; a rebuilt or foreign instance answers NULL (no such file
// or offset) or a different set, and the resume refuses with
// ir.ErrPositionInvalid. Measured: on the capturing instance
// BINLOG_GTID_POS('mysqld-bin.000002', 821) returned exactly "0-1-3"; on a
// rebuilt instance with the same server_id and the same "0-1-3" state it
// returned NULL; on a nonexistent file it returned NULL.
//
// A second, independent door for GTID-mode positions: every domain in
// the resume set must appear in the source's @@gtid_binlog_state. This
// closes the different-domain cell even for a position that carries no
// anchor.
//
// # Residuals, stated
//
// A rebuilt instance whose binlog is byte-identical up to the anchor
// reproduces the anchor and passes; with the anchor following rotations
// that means reproducing A's live traffic byte for byte. And once the
// anchor's FILE has been purged there is no independent witness at all
// (the 2026-08-01 rule: say so). The purge disambiguation can only show
// that the oldest retained file above the anchor starts at a state
// covering the anchor's set — consistent with a same-lineage purge, and
// equally produced by a rebuilt instance whose numbering rotated past the
// anchor and whose GTIDs collide at exactly that state at a file boundary
// (Bug 261, observed by the v0.138.0 regression cycle: `backup
// incremental` recorded the foreign rows as the chain's delta at exit 0
// while this branch logged INFO "lineage confirmed"). The branch therefore
// proceeds under the UNVERIFIED-INSTANCE-IDENTITY WARN, never a
// confirmation, and refusing instead was rejected because the same
// evidence is what a legitimate stop longer than binlog retention
// produces. It also accepts a wrong long-running host with the same
// server_id and domain whose OLDEST retained file happens to start inside
// [anchor seq, resume seq] — measured shape: anchor (mb.000003, 4, 0-1-12)
// absent on B, B's mb.000004:4 = "0-1-22", resume "0-1-30" retained on B →
// accepted, and B streams from after ITS OWN 0-1-30. Because the anchor
// follows rotations the band is one binlog file of A's transactions and B
// must have a file boundary inside it: coincidence-level, named here so it
// is a known residual rather than an assumption.
//
// # Positions without an anchor
//
// A MariaDB position persisted before v0.138.0 carries no anchor. It is
// still accepted — with the UNVERIFIED-INSTANCE-IDENTITY WARN the file/pos
// arm uses for the identical situation — because that population cannot
// grow (every capture door now anchors) and refusing would force a full
// re-copy on positions that are almost certainly fine. The domain door
// still applies to it.

// mariadbLineageSetAt returns BINLOG_GTID_POS(file, pos) on q: the GTID
// state at that byte of the server's own binlog, or "" with ok=false when
// the server answers NULL (no such file, or an offset that is not an
// event boundary of that file).
func mariadbLineageSetAt(ctx context.Context, q rowQuerier, file string, pos uint32) (set string, ok bool, err error) {
	var ns sql.NullString
	if err := q.QueryRowContext(ctx, "SELECT BINLOG_GTID_POS(?, ?)", file, pos).Scan(&ns); err != nil {
		return "", false, fmt.Errorf("mariadb: BINLOG_GTID_POS(%q, %d): %w", file, pos, err)
	}
	if !ns.Valid {
		return "", false, nil
	}
	return ns.String, true, nil
}

// captureMariaDBLineageAnchor stamps the lineage anchor onto p from the
// (file, pos) the caller read under its capture lock. A NULL answer at
// capture time means the server could not describe its own binlog tip,
// which is not a refusal-worthy condition for a backup or a cold start;
// it degrades to no anchor with a WARN, exactly as an unreadable
// @@server_uuid does on the file/pos arm.
func captureMariaDBLineageAnchor(ctx context.Context, q rowQuerier, p binlogPos, file string, pos uint32) binlogPos {
	set, ok, err := mariadbLineageSetAt(ctx, q, file, pos)
	if err != nil || !ok {
		slog.WarnContext(
			ctx, "mariadb: could not read BINLOG_GTID_POS at the captured binlog position; this position carries "+
				"no lineage anchor, so a resume from it cannot be checked against a rebuilt or replaced source "+
				"(it will resume with the "+unverifiedInstanceIdentityMarker+" warning)",
			slog.String("file", file), slog.Uint64("pos", uint64(pos)),
			slog.String("err", fmt.Sprint(err)),
		)
		return p
	}
	p.LineageFile, p.LineagePos, p.LineageSet = file, pos, set
	return p
}

// stampMariaDBLineageAnchor is the flavor-aware form of
// [captureMariaDBLineageAnchor] for the sync snapshot openers, which build
// file/pos anchors for every binlog flavor: a no-op unless the engine is
// MariaDB, whose file/pos positions have no @@server_uuid to bind and take
// the anchor instead. BINLOG_GTID_POS is a function of the binlog
// content, so it is valid on the snapshot conn after the lock.
func (e Engine) stampMariaDBLineageAnchor(ctx context.Context, q rowQuerier, p binlogPos, file string, pos uint32) binlogPos {
	if e.Flavor != FlavorMariaDB {
		return p
	}
	return captureMariaDBLineageAnchor(ctx, q, p, file, pos)
}

// verifyMariaDBLineage is the MariaDB arm of the resume check, for BOTH
// position modes (a MariaDB sync cold start anchors in file/pos mode;
// backups and from-now starts anchor in GTID mode — one binding covers
// both). It runs the domain door first for GTID-mode positions, then the
// anchor door; a position with no anchor passes with the WARN.
func verifyMariaDBLineage(ctx context.Context, db *sql.DB, p binlogPos) error {
	if p.Mode == positionModeGTID {
		if err := verifyMariaDBDomainsPresent(ctx, db, p.GTIDSet); err != nil {
			return err
		}
	}
	if p.LineageFile == "" {
		slog.WarnContext(
			ctx, "mariadb: cdc: "+unverifiedInstanceIdentityMarker+": this position carries no lineage anchor, so it "+
				"cannot be checked against the server being resumed from. It was captured before sluice recorded "+
				"BINLOG_GTID_POS anchors on MariaDB positions (v0.137.4 and earlier). MariaDB GTIDs carry no "+
				"instance identity, so a rebuilt source whose history reads the same GTIDs would NOT be caught on "+
				"this resume. One fresh full backup or cold start moves this chain onto the lineage check",
			slog.String("resume_gtid_set", p.GTIDSet), slog.String("resume_file", p.File),
		)
		return nil
	}
	set, ok, err := mariadbLineageSetAt(ctx, db, p.LineageFile, p.LineagePos)
	if err != nil {
		return err
	}
	if !ok {
		// NULL is two different situations. The anchor's file is PRESENT
		// but the offset is not an event boundary of it: a different
		// instance that happens to reuse the filename — refuse. The file
		// is ABSENT: either routine retention purged it on the SAME
		// lineage (the stream re-anchors at every rotation it sees, but
		// a stopped stream or an incremental-backup gap cannot), or this
		// is a different instance whose numbering never reached it.
		// Disambiguate with evidence only the same lineage can produce:
		// a retained file numbered ABOVE the anchor whose own start
		// state covers the anchor's set in every domain. A fresh
		// instance has no such file; a rebuilt one would need to have
		// rotated past the anchor's number AND to reproduce its GTIDs.
		// Measured on 11.4: after PURGE BINARY LOGS TO 'mb.000009' the
		// anchor at mb.000003:4 answers NULL while mb.000009:4 answers
		// "0-1-62" ⊇ the anchor's "0-1-12". Retention of the GTID
		// resume point itself is then the server's question (1236).
		purged, why, perr := mariadbAnchorPurgedOnSameLineage(ctx, db, p)
		if perr != nil {
			return perr
		}
		if purged {
			// Consistent with a same-lineage purge — NOT proof of one. A
			// rebuilt instance whose numbering rotated past the anchor's
			// file and whose GTIDs collide at exactly the anchor's set at
			// that file boundary reads identically, and MariaDB offers no
			// second witness (Bug 261, v0.138.0 regression cycle: observed,
			// `backup incremental` recorded the foreign instance's rows as
			// the chain's delta at exit 0 under the INFO this used to be).
			// So this is the same UNVERIFIED marker the anchorless case
			// carries, and the evidence is logged for the operator to judge.
			slog.WarnContext(ctx, "mariadb: cdc: "+unverifiedInstanceIdentityMarker+": the position's lineage anchor "+
				"file has been purged by binlog retention, so the anchor cannot be checked. The oldest retained binlog "+
				"above it starts at a state that covers the anchor's set, which is consistent with a purge on the same "+
				"lineage but is not proof of it: a rebuilt instance whose numbering rotated past the anchor and whose "+
				"GTIDs collide at exactly that state reads the same, and MariaDB carries no instance identity to tell "+
				"them apart. Proceeding; if this source was rebuilt or replaced, stop and take a fresh full backup or "+
				"cold start instead",
				slog.String("anchor", fmt.Sprintf("%s:%d", p.LineageFile, p.LineagePos)), slog.String("evidence", why))
			return nil
		}
		return fmt.Errorf("mariadb: the source has no binlog event at the position's lineage anchor (%s:%d) and %s — "+
			"the source is a different lineage (a fresh, reset, rebuilt or replaced instance); cannot resume: %w",
			p.LineageFile, p.LineagePos, why, ir.ErrPositionInvalid)
	}
	if set != p.LineageSet {
		return fmt.Errorf("mariadb: the source's binlog at the position's lineage anchor (%s:%d) reads GTID state %q, "+
			"the position was captured at %q — the source is a different lineage (a rebuilt or replaced instance "+
			"whose GTIDs happen to collide); cannot resume: %w",
			p.LineageFile, p.LineagePos, set, p.LineageSet, ir.ErrPositionInvalid)
	}
	return nil
}

// mariadbAnchorPurgedOnSameLineage decides the anchor-file-absent case
// (see the caller): purged=true only when the anchor's file is not in SHOW
// BINARY LOGS AND a retained file numbered above it has a start state
// (BINLOG_GTID_POS(file, 4) — offset 4 is every binlog file's first event
// boundary, measured) that covers the anchor's set in every domain. The
// returned why is the evidence either way, for the log or the refusal.
func mariadbAnchorPurgedOnSameLineage(ctx context.Context, db *sql.DB, p binlogPos) (purged bool, why string, err error) {
	names, err := binaryLogNames(ctx, db)
	if err != nil {
		return false, "", err
	}
	anchorNo, ok := binlogFileNumber(p.LineageFile)
	if !ok {
		return false, fmt.Sprintf("the anchor file name %q has no numeric suffix", p.LineageFile), nil
	}
	newest := ""
	newestNo := uint64(0)
	for _, n := range names {
		if n == p.LineageFile {
			return false, "the anchor's binlog file is still present, so the offset is simply not an event boundary of it", nil
		}
		no, ok := binlogFileNumber(n)
		if !ok || no <= anchorNo {
			continue
		}
		if newest == "" || no < newestNo {
			newest, newestNo = n, no
		}
	}
	if newest == "" {
		return false, "no retained binlog is numbered above the anchor's file (a same-lineage purge always leaves newer files)", nil
	}
	state, ok, err := mariadbLineageSetAt(ctx, db, newest, 4)
	if err != nil {
		return false, "", err
	}
	if !ok {
		return false, fmt.Sprintf("the oldest retained binlog above the anchor (%s) has no readable start state", newest), nil
	}
	if !mariadbStateCovers(state, p.LineageSet) {
		return false, fmt.Sprintf("the oldest retained binlog above the anchor (%s) starts at GTID state %q, which does not "+
			"cover the anchor's %q", newest, state, p.LineageSet), nil
	}
	return true, fmt.Sprintf("%s starts at %q ⊇ anchor %q", newest, state, p.LineageSet), nil
}

// binaryLogNames lists the source's retained binlog files, in index order.
func binaryLogNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("mariadb: SHOW BINARY LOGS: %w", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("mariadb: SHOW BINARY LOGS columns: %w", err)
	}
	var names []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("mariadb: SHOW BINARY LOGS scan: %w", err)
		}
		switch v := vals[0].(type) {
		case []byte:
			names = append(names, string(v))
		case string:
			names = append(names, v)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mariadb: SHOW BINARY LOGS rows: %w", err)
	}
	return names, nil
}

// binlogFileNumber returns the numeric suffix of a binlog file name
// ("mb.000009" → 9).
func binlogFileNumber(name string) (uint64, bool) {
	i := strings.LastIndexByte(name, '.')
	if i < 0 || i+1 >= len(name) {
		return 0, false
	}
	var n uint64
	for _, c := range name[i+1:] {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	return n, true
}

// mariadbStateCovers reports whether state reaches at least anchor's
// sequence in every domain anchor names. Both are MariaDB GTID lists; a
// state may carry several server_ids per domain, so the maximum sequence
// per domain is what counts.
func mariadbStateCovers(state, anchor string) bool {
	have := mariadbGTIDMaxSeqs(state)
	for d, seq := range mariadbGTIDMaxSeqs(anchor) {
		if have[d] < seq {
			return false
		}
	}
	return true
}

// mariadbGTIDMaxSeqs maps each domain in a MariaDB GTID list to the
// highest sequence it names ("0-1-5,0-2-9,7-1-1" → {0: 9, 7: 1}).
func mariadbGTIDMaxSeqs(set string) map[string]uint64 {
	out := map[string]uint64{}
	for _, g := range strings.Split(set, ",") {
		parts := strings.Split(strings.TrimSpace(g), "-")
		if len(parts) != 3 {
			continue
		}
		var seq uint64
		for _, c := range parts[2] {
			if c < '0' || c > '9' {
				seq = 0
				break
			}
			seq = seq*10 + uint64(c-'0')
		}
		if seq > out[parts[0]] {
			out[parts[0]] = seq
		}
	}
	return out
}

// reanchorMariaDBLineage is called by the pump on every binlog rotation
// it observes: the anchor moves to the new file's first event boundary
// (offset 4) with the server's own start state for it, so the persisted
// anchor is never older than the newest file the stream has seen and
// routine retention cannot purge it out from under a running stream. The
// set is always the SERVER's answer, never computed from the running
// GTID set: BINLOG_GTID_POS orders domains differently from
// @@gtid_binlog_state, so only function-versus-function comparison is
// stable. A failed re-anchor keeps the previous anchor (still valid until
// its file is purged) and logs once per rotation.
func (r *CDCReader) reanchorMariaDBLineage(ctx context.Context, newFile string) {
	if r.flavor != FlavorMariaDB || newFile == "" {
		return
	}
	set, ok, err := mariadbLineageSetAt(ctx, r.db, newFile, 4)
	if err != nil || !ok {
		slog.WarnContext(ctx, "mariadb: cdc: could not re-anchor the lineage at the new binlog file; keeping the "+
			"previous anchor, which stays valid until retention purges its file",
			slog.String("file", newFile), slog.String("err", fmt.Sprint(err)))
		return
	}
	r.lineageFile, r.lineagePos, r.lineageSet = newFile, 4, set
}

// verifyMariaDBDomainsPresent refuses a GTID-mode resume whose set names
// a replication domain the source has never written to. MariaDB's own
// replication ACCEPTS such a position (a replica at "5-1-1" against a
// master that has never seen domain 5 starts with Slave_IO_Running: Yes,
// even under gtid_strict_mode) and then streams the master's whole
// history — measured on 11.4 — so the server cannot be relied on here.
func verifyMariaDBDomainsPresent(ctx context.Context, db *sql.DB, resumeSet string) error {
	if strings.TrimSpace(resumeSet) == "" {
		// The empty set is the legitimate "from the beginning of history"
		// position of a brand-new source; nothing to bind.
		return nil
	}
	var state string
	if err := db.QueryRowContext(ctx, "SELECT @@gtid_binlog_state").Scan(&state); err != nil {
		return fmt.Errorf("mariadb: read @@gtid_binlog_state: %w", err)
	}
	have := mariadbGTIDDomains(state)
	var missing []string
	for d := range mariadbGTIDDomains(resumeSet) {
		if !have[d] {
			missing = append(missing, d)
		}
	}
	sort.Strings(missing)
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("mariadb: the resume GTID set names replication domain(s) %v the source has never written "+
		"(source @@gtid_binlog_state %q, resume %q) — the source is a different lineage; MariaDB itself would "+
		"accept this position and stream its entire history; cannot resume: %w",
		missing, state, resumeSet, ir.ErrPositionInvalid)
}

// mariadbGTIDDomains returns the set of domain ids named by a MariaDB
// GTID list ("0-1-3,7-4-4" → {0, 7}). Malformed entries are kept verbatim
// as their own key so they can never match a real domain by accident.
func mariadbGTIDDomains(set string) map[string]bool {
	out := map[string]bool{}
	for _, g := range strings.Split(set, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		if i := strings.IndexByte(g, '-'); i > 0 {
			out[g[:i]] = true
			continue
		}
		out[g] = true
	}
	return out
}
