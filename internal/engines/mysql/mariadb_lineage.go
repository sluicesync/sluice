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

// MariaDB lineage binding for resume positions (v0.137.5, audit
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
// # Positions without an anchor
//
// A MariaDB position persisted before v0.137.5 carries no anchor. It is
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
			slog.String("resume_position", p.GTIDSet+p.File),
		)
		return nil
	}
	set, ok, err := mariadbLineageSetAt(ctx, db, p.LineageFile, p.LineagePos)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("mariadb: the source has no binlog event at the position's lineage anchor (%s:%d) — "+
			"the source is a different lineage (a fresh, reset, rebuilt or replaced instance) or has purged that "+
			"binlog; cannot resume: %w", p.LineageFile, p.LineagePos, ir.ErrPositionInvalid)
	}
	if set != p.LineageSet {
		return fmt.Errorf("mariadb: the source's binlog at the position's lineage anchor (%s:%d) reads GTID state %q, "+
			"the position was captured at %q — the source is a different lineage (a rebuilt or replaced instance "+
			"whose GTIDs happen to collide); cannot resume: %w",
			p.LineageFile, p.LineagePos, set, p.LineageSet, ir.ErrPositionInvalid)
	}
	return nil
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
