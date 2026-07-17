//go:build integration && mariadbpreview

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MariaDB PREVIEW-line canary (roadmap item 73 matrix expansion). The
// released LTS lines (10.11 / 11.4 / 11.8 / 12.3) are pinned by the
// required engines-mysql shard; this leg extends the SAME native uuid/inet
// CDC value-fidelity ground-truth to the next, UNRELEASED MariaDB line so a
// future-LTS storage-byte-order or binlog-protocol change is caught while it
// is still a preview — not after it ships and a real migration corrupts.
//
// It is gated behind its OWN build tag (mariadbpreview) precisely so it can
// NEVER gate a merge: the preview image is a MOVING quay.io devel tag
// (quay.io/mariadb-foundation/mariadb-devel:13.1 today), and the only CI
// entry point is the ci.yml `integration-mariadb-preview` job, which runs
// continue-on-error. A quay outage or a preview-image change therefore shows
// up as a non-blocking red canary, never a blocked merge. All test funcs in
// this file MUST carry the TestMariaDB_Preview_ prefix (enforced by
// scripts/check-run-filter-coverage.sh against the job's -run filter).
//
// 2026-07-17 live ground truth on 13.1.0-MariaDB: uuid stores canonical
// big-endian (HEX == text sans dashes) and the inet6 renderings
// (::1.2.3.4 / ::0.1.0.0 / ::100 / ::ffff) are byte-identical to 10.11/11.4
// — the ADR-0171 residual risk is closed for 13.1 too. (13.1 additionally
// now ACCEPTS `SHOW BINARY LOG STATUS`, the MySQL-8.4 spelling, so the
// reader's masterStatusSpellings fallback lands on that first entry there;
// still zero code change.)

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestMariaDB_Preview_NativeUUIDInet_Decode runs the full native uuid/inet
// CDC value-fidelity matrix (assertMariaDBNativeDecodeConverges — every
// family × shape × DML arm, asserting CDC-decode == live driver text and the
// inet6 oracle) against the MariaDB preview line. Because that helper opens a
// live CDC reader and cold-starts the stream, this also exercises the
// preview line's binlog-status spelling and MariadbGTIDEvent pump end to end.
func TestMariaDB_Preview_NativeUUIDInet_Decode(t *testing.T) {
	image := mariadb131PreviewImage

	// Log the actual preview version for the informational leg's output —
	// the moving tag can advance under us, so which build ran matters.
	dsn := newMariaDB(t, image, "mdb_preview_probe")
	if v := probeMariaDBVersion(t, dsn); v != "" {
		t.Logf("MariaDB preview line under test: %s (image %s)", v, image)
	}

	assertMariaDBNativeDecodeConverges(t, image)
}

// probeMariaDBVersion returns SELECT VERSION() (best-effort; empty on error).
func probeMariaDBVersion(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var v string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return ""
	}
	return v
}
