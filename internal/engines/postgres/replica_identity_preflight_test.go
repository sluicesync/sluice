// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestReplicaIdentityUsable walks the whole classification family, not a
// representative: each of the four pg_class.relreplident settings × the
// index states that setting can be in. The rules are PostgreSQL's, not
// sluice's, so every cell here is ground-truthed by the oracle in
// replica_identity_preflight_integration_test.go — this test pins that
// the pure function agrees with it without a server in the way.
func TestReplicaIdentityUsable(t *testing.T) {
	cases := []struct {
		name       string
		row        replicaIdentityRow
		want       bool
		reasonHas  string
		reasonOmit string
	}{
		{
			name: "DEFAULT with an immediate primary key — the ordinary case",
			row:  replicaIdentityRow{Identity: "d", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: true},
			want: true,
		},
		{
			name:      "DEFAULT with a DEFERRABLE primary key — the item-93 shape",
			row:       replicaIdentityRow{Identity: "d", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: false},
			want:      false,
			reasonHas: "DEFERRABLE",
		},
		{
			// The deferrable PK is unusable and the UNIQUE index does not
			// rescue it: REPLICA IDENTITY DEFAULT resolves to the primary
			// key and nothing else. (The TARGET-side sibling refusal takes
			// the opposite view for the same shape — ON CONFLICT can
			// arbitrate on any unique index. Deliberate asymmetry.)
			name: "DEFAULT with a DEFERRABLE primary key plus an immediate UNIQUE",
			row: replicaIdentityRow{
				Identity: "d", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: false,
				CandidateIndex: "t_alt_key",
			},
			want:      false,
			reasonHas: "DEFERRABLE",
		},
		{
			name:      "DEFAULT with no primary key — the keyless case",
			row:       replicaIdentityRow{Identity: "d"},
			want:      false,
			reasonHas: "no PRIMARY KEY",
		},
		{
			// A UNIQUE index is not a replica identity until the operator
			// nominates it, so this stays a refusal — with the candidate
			// available for the remedy text.
			name:      "DEFAULT with no primary key but a nominatable UNIQUE index",
			row:       replicaIdentityRow{Identity: "d", CandidateIndex: "t_alt_key"},
			want:      false,
			reasonHas: "no PRIMARY KEY",
		},
		{
			name: "USING INDEX with a usable index",
			row:  replicaIdentityRow{Identity: "i", ChosenIndex: "t_alt_key", ChosenIndexUsable: true},
			want: true,
		},
		{
			name:      "USING INDEX whose index is no longer usable",
			row:       replicaIdentityRow{Identity: "i", ChosenIndex: "t_alt_key", ChosenIndexUsable: false},
			want:      false,
			reasonHas: "t_alt_key",
		},
		{
			name:      "USING INDEX with no nominated index left",
			row:       replicaIdentityRow{Identity: "i"},
			want:      false,
			reasonHas: "USING INDEX",
		},
		{
			// FULL never needs a key — the keyless and deferrable-key
			// tables both become publishable this way, which is why it is
			// one of the two remedies the refusal names.
			name: "FULL with no key at all",
			row:  replicaIdentityRow{Identity: "f"},
			want: true,
		},
		{
			name: "FULL alongside a DEFERRABLE primary key",
			row:  replicaIdentityRow{Identity: "f", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: false},
			want: true,
		},
		{
			name:      "NOTHING, even with a perfectly good primary key",
			row:       replicaIdentityRow{Identity: "n", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: true},
			want:      false,
			reasonHas: "NOTHING",
		},
		{
			// Loud failure beats a silent pass: an unknown catalog letter
			// must not be assumed publishable.
			name:      "an relreplident letter this build does not know",
			row:       replicaIdentityRow{Identity: "z"},
			want:      false,
			reasonHas: "relreplident",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := replicaIdentityUsable(tc.row)
			if got != tc.want {
				t.Fatalf("usable = %v (reason %q); want %v", got, reason, tc.want)
			}
			if tc.want {
				if reason != "" {
					t.Errorf("a usable identity must carry no reason; got %q", reason)
				}
				return
			}
			if reason == "" {
				t.Fatal("a refusal must carry a reason")
			}
			if tc.reasonHas != "" && !strings.Contains(reason, tc.reasonHas) {
				t.Errorf("reason %q does not mention %q", reason, tc.reasonHas)
			}
		})
	}
}

// TestErrUnusableReplicaIdentity pins the operator-facing refusal: the
// code, the hint, every offending table named, and — the part that makes
// the deferrable-PK-plus-UNIQUE cell actionable — the concrete
// `REPLICA IDENTITY USING INDEX` remedy when a candidate index exists.
func TestErrUnusableReplicaIdentity(t *testing.T) {
	err := errUnusableReplicaIdentity("public", []replicaIdentityGap{
		{Table: "dpk", Reason: "its PRIMARY KEY index \"dpk_pk\" is DEFERRABLE"},
		{Table: "dpk_alt", Reason: "its PRIMARY KEY index \"dpk_alt_pk\" is DEFERRABLE", CandidateIndex: "dpk_alt_key"},
	})

	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeSourceReplicaIdentity {
		t.Fatalf("refusal is not %s: %v", sluicecode.CodeSourceReplicaIdentity, err)
	}
	if coded.Hint == "" {
		t.Error("refusal carries no remedy hint")
	}
	msg := err.Error()
	for _, want := range []string{
		"dpk",
		"dpk_alt",
		"REPLICA IDENTITY FULL",            // the always-available remedy
		"REPLICA IDENTITY USING INDEX",     // the narrow remedy, for dpk_alt
		"dpk_alt_key",                      // …naming the actual index
		"--exclude-table",                  // the take-it-out-of-scope escape
		"does not have a replica identity", // the error the operator will otherwise see
		"BEFORE touching the publication",  // the timing promise
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal does not mention %q:\n%s", want, msg)
		}
	}

	// A table with NO candidate index must not be offered the USING INDEX
	// remedy — pointing an operator at an index that does not exist is
	// worse than saying nothing.
	lone := errUnusableReplicaIdentity("public", []replicaIdentityGap{
		{Table: "dpk", Reason: "its PRIMARY KEY index \"dpk_pk\" is DEFERRABLE"},
	}).Error()
	if strings.Contains(lone, "USING INDEX") {
		t.Errorf("a table with no candidate index was offered the USING INDEX remedy:\n%s", lone)
	}
	if !strings.Contains(lone, "REPLICA IDENTITY FULL;` makes it publishable") {
		t.Errorf("a table with no candidate index was not offered the FULL remedy:\n%s", lone)
	}
}

// TestPreflightReplicaIdentity_EmptyScopeIsANoOp pins the no-op contract
// — a stream with no in-scope tables must not open a catalog read (the
// reader here has a nil *sql.DB, so a query would panic).
func TestPreflightReplicaIdentity_EmptyScopeIsANoOp(t *testing.T) {
	r := &SchemaReader{schema: "public"}
	if err := r.PreflightReplicaIdentity(t.Context(), nil); err != nil {
		t.Fatalf("nil table list: %v", err)
	}
	if err := r.PreflightReplicaIdentity(t.Context(), []string{}); err != nil {
		t.Fatalf("empty table list: %v", err)
	}
}
