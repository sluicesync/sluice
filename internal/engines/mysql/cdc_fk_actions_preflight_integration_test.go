//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 G9 — the FK referential-action capture WARN against a real
// mysqld, both directions on the shared container: a cascade-carrying
// schema WARNs at CDC open naming the FK, and the same schema rebuilt
// with plain/RESTRICT FKs stays silent. This pin holds the DOOR on the
// binlog lane end to end (the wire mechanism — cascaded child rows
// absent from the binlog — is the m2 sweep's recorded ground truth;
// the vstream lane shares the census via
// TestFKReferentialActionWarnRoster_BothLanes and the unit matrix).

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_FKReferentialActionWarn(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "fk_g9_src")
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// openStream opens a fresh from-now stream with a WARN-level log
	// capture installed, returning what the open logged.
	openStream := func(t *testing.T) string {
		t.Helper()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer func() { _ = rdr.(*CDCReader).Close() }()
		if _, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{}); err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		return buf.String()
	}

	// --- Firing direction: ON DELETE CASCADE + ON UPDATE SET NULL FKs
	// in scope WARN at open, naming each constraint and its rules.
	t.Run("cascade_schema_warns_at_open", func(t *testing.T) {
		applyMySQL(t, dsn, `
			CREATE TABLE parent (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
			CREATE TABLE child_cascade (
				id  BIGINT NOT NULL, pid BIGINT,
				PRIMARY KEY (id),
				CONSTRAINT fk_g9_cascade FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE
			) ENGINE=InnoDB;
			CREATE TABLE child_setnull (
				id  BIGINT NOT NULL, pid BIGINT,
				PRIMARY KEY (id),
				CONSTRAINT fk_g9_setnull FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE SET NULL
			) ENGINE=InnoDB;
		`)
		out := openStream(t)
		if !strings.Contains(out, fkReferentialActionMarker) {
			t.Fatalf("no %s WARN at CDC open on a cascade-carrying schema; log: %s", fkReferentialActionMarker, out)
		}
		for _, phrase := range []string{
			"fk_g9_src.child_cascade", "fk_g9_cascade", "ON DELETE CASCADE",
			"fk_g9_src.child_setnull", "fk_g9_setnull", "ON UPDATE SET NULL",
		} {
			if !strings.Contains(out, phrase) {
				t.Errorf("WARN missing %q; log: %s", phrase, out)
			}
		}
	})

	// --- Silent direction: the same shape with plain / RESTRICT FKs
	// causes no invisible source-side writes and must not warn.
	t.Run("plain_fk_schema_stays_silent", func(t *testing.T) {
		applyMySQL(t, dsn, `
			DROP TABLE child_cascade, child_setnull;
			CREATE TABLE child_plain (
				id  BIGINT NOT NULL, pid BIGINT,
				PRIMARY KEY (id),
				CONSTRAINT fk_g9_plain FOREIGN KEY (pid) REFERENCES parent (id)
			) ENGINE=InnoDB;
			CREATE TABLE child_restrict (
				id  BIGINT NOT NULL, pid BIGINT,
				PRIMARY KEY (id),
				CONSTRAINT fk_g9_restrict FOREIGN KEY (pid) REFERENCES parent (id)
					ON DELETE RESTRICT ON UPDATE RESTRICT
			) ENGINE=InnoDB;
		`)
		out := openStream(t)
		if strings.Contains(out, fkReferentialActionMarker) {
			t.Fatalf("plain/RESTRICT FK schema must stay silent at CDC open; log: %s", out)
		}
	})
}
