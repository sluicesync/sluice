// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// THE FROZEN GOLDEN.
//
// [ComputeSchemaHash] is a SHA-256 over `json.Marshal` of the IR schema,
// and the IR schema structs carry NO json tags — every exported field is
// marshalled unconditionally under its Go name. So adding ANY field to
// [ir.Schema], [ir.Table], [ir.Column], [ir.Index], [ir.IndexColumn],
// [ir.ForeignKey], [ir.CheckConstraint], [ir.ExcludeConstraint],
// [ir.Policy], [ir.View] or [ir.Sequence] changes the marshalled bytes of
// every schema that has one of those objects — which is every schema —
// and therefore changes this fingerprint for schemas that did not change.
//
// That is not cosmetic. Every backup manifest records the fingerprint of
// the schema it was written with, and a chain restore RECOMPUTES it and
// REFUSES the whole chain on a mismatch (`verifySchemaHashes` in
// internal/pipeline/backup/chain_restore.go). The refusal is
// SLUICE-E-BACKUP-MANIFEST-INVALID — so a field addition here does not
// degrade a restore, it makes every chain an operator already holds
// unrestorable by this binary. (The refusal's WORDING was fixed for
// roadmap item 102: it now names release skew alongside corruption
// instead of telling that operator their store is corrupt. That makes the
// failure honest; it does not make it survivable — the chain is still
// refused.) The same recompute rides the broker's live-apply path
// (internal/pipeline/broker.go), so it is not DR-only, and
// `deltaTableFingerprint` (signature.go) folds it into the signing bytes,
// so signed chains carrying a schema delta fail signature verification too.
//
// The whole class is therefore invisible to every other test in this repo
// by construction: they all run ONE binary against its own output, and one
// binary always agrees with itself about what Index marshals to.
//
// HOW TO RESPOND WHEN THIS TEST FAILS. You almost certainly just added a
// field to one of the structs above. Two legitimate answers, in order:
//
//  1. Tag the new field `json:"<Name>,omitempty"` so its ZERO VALUE
//     marshals away. Every schema an older binary wrote has the zero
//     value, so the bytes — and the hash — stay exactly as before, and
//     only the new shape (which no older binary ever produced) adds a key.
//     This is the right answer for an additive, opt-in field, and it is
//     what [ir.Index.ConstraintNamed] does. Re-run this test; it should go
//     green WITHOUT touching the constant below.
//
//  2. If the field genuinely cannot be omitempty (a non-zero default, a
//     field whose absence means something different from its zero value),
//     then invalidating the fingerprint is a DELIBERATE format change:
//     bump [BackupFormatVersion], write the migration/compat story, and
//     only then update the constant below.
//
// DO NOT simply paste the new hash in to make the test green. Changing
// this constant without doing (2) invalidates EVERY BACKUP CHAIN IN THE
// FIELD — every chain an operator already holds becomes unrestorable by
// every binary from that release onward, and it surfaces to them as
// "your backup is corrupt".
//
// Also DO NOT retrofit `omitempty` onto fields that ALREADY shipped
// without it: that is the same break in the other direction, invalidating
// the chains written by the releases that carried them.
const goldenSchemaHash = "75a03532fd32bac87454e9ad6a53d45f5c9d3d7b14432270811bfaba45458e48"

// goldenSchema is the frozen input to [goldenSchemaHash]. It is FROZEN:
// changing it changes the hash and defeats the gate. It exists only to be
// hashed — it is deliberately NOT shared with any other test.
//
// It carries at least one instance of every IR schema object that rides
// the fingerprint, because a field added to a struct with no instance here
// would not move the hash and would slip the gate. Keep that property when
// a NEW schema object type is introduced: add one instance of it here (a
// change that legitimately moves the hash, and which — unlike a field
// addition — no older binary could have written, so it is safe to re-freeze
// the constant in the same commit).
func goldenSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Schema: "public",
			Name:   "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{
					Name:     "email",
					Type:     ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
					Nullable: true,
					Default:  ir.DefaultLiteral{Value: "unknown@example.test"},
					Comment:  "primary contact",
				},
				{
					Name:            "email_lower",
					Type:            ir.Text{Size: ir.TextLong},
					Nullable:        true,
					GeneratedExpr:   "lower(email)",
					GeneratedStored: true,
				},
				{Name: "org_id", Type: ir.Integer{Width: 32}},
				{Name: "created_at", Type: ir.Timestamp{}, Default: ir.DefaultExpression{Expr: "now()"}},
			},
			PrimaryKey: &ir.Index{
				Name:            "users_pkey",
				Unique:          true,
				ConstraintNamed: true,
				Columns:         []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{
					Name:             "users_email_idx",
					Unique:           true,
					ConstraintBacked: true,
					Columns:          []ir.IndexColumn{{Column: "email", Desc: true}},
					Kind:             ir.IndexKindBTree,
					IncludeColumns:   []string{"org_id"},
					Predicate:        "email IS NOT NULL",
					PredicateDialect: "postgres",
				},
				{
					Name:    "users_email_lower_idx",
					Columns: []ir.IndexColumn{{Expression: "lower(email)", ExpressionDialect: "postgres"}},
					Method:  "hnsw",
				},
			},
			ForeignKeys: []*ir.ForeignKey{{
				Name:              "users_org_id_fkey",
				Columns:           []string{"org_id"},
				ReferencedSchema:  "public",
				ReferencedTable:   "orgs",
				ReferencedColumns: []string{"id"},
			}},
			CheckConstraints: []*ir.CheckConstraint{{
				Name: "users_email_len_chk", Expr: "length(email) > 3", ExprDialect: "postgres",
			}},
			ExcludeConstraints: []*ir.ExcludeConstraint{{
				Name: "users_span_excl", Definition: "EXCLUDE USING gist (org_id WITH =)",
			}},
			RLSEnabled: true,
			Policies: []*ir.Policy{{
				Name: "users_self_read", Command: "SELECT", Permissive: true,
				Roles: []string{"public"}, Using: "org_id = current_org()",
			}},
			Comment: "application users",
		}},
		Views: []*ir.View{{
			Schema:            "public",
			Name:              "active_users",
			Definition:        "SELECT id, email FROM users WHERE email IS NOT NULL",
			DefinitionDialect: "postgres",
		}},
		Sequences: []*ir.Sequence{{
			Schema: "public", Name: "order_number_seq",
			Start: 1000, Increment: 5, MinValue: 1, MaxValue: 9223372036854775807, Cache: 1,
		}},
	}
}

// TestComputeSchemaHash_FrozenGolden is the gate. See the block comment on
// [goldenSchemaHash] before changing anything here.
func TestComputeSchemaHash_FrozenGolden(t *testing.T) {
	got, err := ComputeSchemaHash(goldenSchema())
	if err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	if got == goldenSchemaHash {
		return
	}

	// The failure diagnostic matters as much as the failure: print the
	// bytes actually hashed so the offending key is visible without a
	// second debugging round-trip.
	canonical, mErr := json.Marshal(canonicalSchemaForHash(goldenSchema()))
	if mErr != nil {
		canonical = []byte("<marshal failed: " + mErr.Error() + ">")
	}
	t.Errorf(`schema fingerprint CHANGED — every backup chain in the field is at stake.

  want %s  (frozen)
  got  %s

You almost certainly added a field to ir.Schema / ir.Table / ir.Column /
ir.Index / ir.IndexColumn / ir.ForeignKey / ir.CheckConstraint /
ir.ExcludeConstraint / ir.Policy / ir.View / ir.Sequence. Those structs have
no json tags, so an untagged field marshals on EVERY object of that type and
moves the fingerprint of schemas that did not change. A chain restore
recomputes this hash and REFUSES the whole chain on a mismatch
(SLUICE-E-BACKUP-MANIFEST-INVALID) — so shipping this makes every chain
backup an operator already holds unrestorable by the release that ships it.

FIX IT ONE OF TWO WAYS:

  1. Tag the new field  json:"<Name>,omitempty"  so its ZERO VALUE marshals
     away. Older schemas all carry the zero value, so the bytes and the hash
     stay identical, and only the new shape adds a key. Re-run this test: it
     should pass WITHOUT editing the constant. This is the usual answer.

  2. If the field truly cannot be omitempty, invalidating the fingerprint is
     a DELIBERATE format change: bump BackupFormatVersion, write the compat
     story, and only then re-freeze the constant.

Do NOT just paste the new hash in to go green.

Bytes actually hashed:
%s`, goldenSchemaHash, got, canonical)
}

// TestIndexConstraintNamed_OmitsZeroValue pins the mechanism the golden
// depends on, at the one field that needed it (roadmap item 84): a false
// ConstraintNamed must not appear in Index's JSON at all — that is what
// keeps a v0.100.0-through-v0.104.2 schema hashing to the same value under
// this binary — while a true one must still ride the wire, or the field is
// omitempty'd into uselessness.
func TestIndexConstraintNamed_OmitsZeroValue(t *testing.T) {
	idx := &ir.Index{Name: "t_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}

	off, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal (false): %v", err)
	}
	if strings.Contains(string(off), `"ConstraintNamed"`) {
		t.Errorf("a false Index.ConstraintNamed appears in the wire JSON — it must be omitempty, or it changes the schema fingerprint of every index ever written:\n%s", off)
	}

	idx.ConstraintNamed = true
	on, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal (true): %v", err)
	}
	if !strings.Contains(string(on), `"ConstraintNamed":true`) {
		t.Errorf("a true Index.ConstraintNamed did NOT reach the wire — the field cannot round-trip through a manifest or the schema history:\n%s", on)
	}

	// And the round trip must preserve it, so a restore of a chain written
	// by this binary keeps the source constraint name.
	var back ir.Index
	if err := json.Unmarshal(on, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !back.ConstraintNamed {
		t.Error("Index.ConstraintNamed did not survive a JSON round trip")
	}
}
