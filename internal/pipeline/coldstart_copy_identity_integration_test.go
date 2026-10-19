//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestResumeTableIdentity_PgoutputRecreatedWhileStoppedRefuses pins
// RESUMED-COLD-START-CERTIFIES-RECREATED-TABLE on Postgres 16. A fast-path
// cold start is stopped in its index build after both copies landed; kx is
// dropped and re-created with different rows while stopped; the re-run must
// refuse naming kx rather than resume over the old copy. Measured before
// the fix: the resume ran (COLD-START-RESUMED) under both publications;
// under FOR ALL TABLES the target ended 1:x,2:y,9:live9 against a source of
// 7,8,9 and the next start passed, and under FOR TABLE the resumed run
// applied none of the new table's changes until the next start refused.
func TestResumeTableIdentity_PgoutputRecreatedWhileStoppedRefuses(t *testing.T) {
	for _, pub := range []string{"for-table", "for-all-tables"} {
		t.Run(pub, func(t *testing.T) {
			r := pgAnchorRig(t)
			r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
			r.applySrc(`DROP TABLE kx; CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (7, 'new7'), (8, 'new8')`)
			if pub == "for-all-tables" {
				r.applySrc(`DROP PUBLICATION IF EXISTS sluice_pub; CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
			}
			logs := captureSlog(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			err := r.run("k*").Run(ctx)
			if err == nil || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") || !strings.Contains(err.Error(), "kx (copied as id") {
				t.Fatalf("a resume over a table re-created while stopped: %v; want TABLE-IDENTITY-CHANGED naming kx", err)
			}
			if strings.Contains(logs.String(), coldStartResumedMarker) {
				t.Fatalf("the refused run announced %s", coldStartResumedMarker)
			}
			if got := pgQueryOne[string](t, r.tgt, "SELECT string_agg(id||':'||e, ',' ORDER BY id) FROM kx"); got != "1:x,2:y" {
				t.Fatalf("the refused run changed the target: kx = %s; want the stopped copy 1:x,2:y untouched", got)
			}
		})
	}
}

// TestResumeTableIdentity_PgoutputVacuumFullWhileStoppedResumes is the
// no-false-refusal half: VACUUM FULL rewrites the table (a new relfilenode)
// and keeps its oid, so the resume still proves kx is the table it copied.
func TestResumeTableIdentity_PgoutputVacuumFullWhileStoppedResumes(t *testing.T) {
	r := pgAnchorRig(t)
	r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
	r.applySrc(`VACUUM FULL kx`)
	logs := captureSlog(t)
	stopAfterTheAnchor(t, r.run("k*"))
	if !strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Fatalf("a resume after VACUUM FULL did not resume:\n%s", logs.String())
	}
	r.requireTheCopyWithIDs([]string{"kd", "kx"})
}
