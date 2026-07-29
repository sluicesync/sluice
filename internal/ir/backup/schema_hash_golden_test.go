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
// field to one of the structs above. Three legitimate answers, in order:
//
//  1. Tag the new field `json:"<Name>,omitempty"` so its ZERO VALUE
//     marshals away. Every schema an older binary wrote has the zero
//     value, so the bytes — and the hash — stay exactly as before, and
//     only the new shape (which no older binary ever produced) adds a key.
//     Re-run this test; it should go green WITHOUT touching the constant
//     below.
//
//     BUT CHECK WHO SETS IT, because `omitempty` is only as good as how
//     often the zero value is what a real source produces. That is what
//     failed in v0.104.3: [ir.Index.ConstraintNamed] got the tag, and the
//     Postgres reader sets it TRUE on every constraint-owned index of
//     every primary-keyed table, so on a real PG chain the key was always
//     present and the fingerprint moved anyway (Bug 216). If a common
//     source shape carries the NON-zero value, this answer does not
//     apply — go to (3).
//
//  2. If the field genuinely cannot be omitempty (a non-zero default, a
//     field whose absence means something different from its zero value),
//     then invalidating the fingerprint is a DELIBERATE format change:
//     bump [BackupFormatVersion], write the migration/compat story, and
//     only then update the constant below.
//
//  3. EXCLUDE the field from the fingerprint, in `fingerprintIndex`
//     (chain.go) or its sibling for whatever struct grew. This is the
//     right answer when the field is a RE-EMISSION hint rather than part
//     of what the schema IS, and it is the only answer that can move the
//     hash BACK: v0.104.4 excludes ConstraintNamed and thereby returns to
//     the exact bytes v0.103.0 – v0.104.2 hashed. Excluding costs the
//     tamper-detection of that one field and nothing else; weigh that
//     against partitioning every chain in the field.
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
//
// PROVENANCE OF THE CONSTANT, which is the part this gate got wrong the
// first time. A frozen golden whose fixture carries post-bump fields at
// their NON-zero values is self-referential by construction: it proves
// this binary agrees with itself, which was never in doubt, and it is
// exactly why the v0.104.3 constant was measured green over a schema no
// older binary could have written. The value below was instead computed
// by a REAL v0.104.2 build — `git worktree add <dir> v0.104.2`, the
// fixture below pasted in with the `ConstraintNamed` line dropped (that
// field does not exist there), `ComputeSchemaHash` printed — and it
// matches. So this constant is the E3 epoch's own number, not this
// build's opinion of it. Re-verify that way if it ever legitimately
// changes, and keep the behavioural gate that actually found Bug 216:
// cell 6 of internal/pipeline/backup_crossversion_integration_test.go,
// which writes a chain with THIS binary and restores it with the
// PREVIOUS release.
const goldenSchemaHash = "07231daa5cdd1330a32d83668cc229d8302c518af0416579d7294b68da916868"

// goldenSchema is the frozen input to [goldenSchemaHash]. It is FROZEN:
// changing it changes the hash and defeats the gate. It exists only to be
// hashed — it is deliberately NOT shared with any other test.
//
// Its primary key deliberately carries ConstraintNamed TRUE even though
// the fingerprint now excludes that field: the golden then also witnesses
// the exclusion, so deleting it from `fingerprintIndex` fails HERE as well
// as in TestComputeSchemaHash_ExcludesIndexConstraintNamed. That is safe
// only BECAUSE the field is excluded — a post-bump field that still rides
// the hash must be held at its ZERO value here, or the constant stops
// being a number an older binary could have produced (see the provenance
// note above).
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

// TestComputeSchemaHash_ExcludesIndexConstraintNamed is the direct pin on
// the Bug 216 fix, and it is the assertion the frozen golden cannot make
// on its own: flipping [ir.Index.ConstraintNamed] — on a table's PRIMARY
// KEY or on a secondary index — must not move the fingerprint by so much
// as a bit.
//
// This is what the `omitempty` was supposed to deliver and did not. The
// tag only hides the FALSE case, and the Postgres reader sets the field
// true for every constraint-owned index of every primary-keyed table, so
// "the zero value keeps the bytes stable" was true of a shape real
// Postgres sources essentially never produce. A chain written by v0.104.3
// from any PG source with a primary key is unrestorable by v0.104.2 and
// earlier for exactly this reason.
func TestComputeSchemaHash_ExcludesIndexConstraintNamed(t *testing.T) {
	schemaWith := func(pkNamed, idxNamed bool) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Schema:  "public",
			Name:    "orders",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{
				Name: "orders_pkey", Unique: true, ConstraintNamed: pkNamed,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{{
				Name: "orders_id_key", Unique: true, ConstraintBacked: true, ConstraintNamed: idxNamed,
				Columns: []ir.IndexColumn{{Column: "id"}},
			}},
		}}}
	}

	base, err := ComputeSchemaHash(schemaWith(false, false))
	if err != nil {
		t.Fatalf("ComputeSchemaHash (both false): %v", err)
	}
	for _, tc := range []struct {
		what      string
		pk, idx   bool
		reachedBy string
	}{
		{"primary key named", true, false, "every PG table with a primary key — PG auto-names them"},
		{"secondary unique named", false, true, "every PG UNIQUE constraint"},
		{"both named", true, true, "the ordinary PG table"},
	} {
		got, err := ComputeSchemaHash(schemaWith(tc.pk, tc.idx))
		if err != nil {
			t.Fatalf("ComputeSchemaHash (%s): %v", tc.what, err)
		}
		if got != base {
			t.Errorf("Index.ConstraintNamed moved the schema fingerprint (%s — reached by %s):\n  without %s\n  with    %s\n\n"+
				"Every backup chain written by this binary from such a source becomes unrestorable by every release "+
				"that hashes it differently, reported to the operator as a corrupt manifest. The field must be dropped "+
				"in fingerprintIndex (chain.go) — see roadmap item 104 / Bug 216.", tc.what, tc.reachedBy, base, got)
		}
	}

	// And the exclusion must not have been implemented by MUTATING the
	// caller's schema — the manifest records the schema verbatim, so a
	// fingerprint helper that clears the field in place would silently
	// strip the constraint name a restore re-emits.
	s := schemaWith(true, true)
	if _, err := ComputeSchemaHash(s); err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	if !s.Tables[0].PrimaryKey.ConstraintNamed || !s.Tables[0].Indexes[0].ConstraintNamed {
		t.Error("hashing the schema CLEARED ConstraintNamed on the caller's own indexes — the restore that re-emits " +
			"this schema would lose the source constraint name")
	}
}

// TestIndexConstraintNamed_OmitsZeroValue pins the WIRE half (roadmap item
// 84): a false ConstraintNamed must not appear in Index's JSON at all, so
// a manifest this binary writes for an unnamed index stays byte-identical
// to what older binaries wrote and decodes on them unchanged — while a
// true one must still ride the wire, or the field is omitempty'd into
// uselessness. The FINGERPRINT no longer depends on this; that is
// TestComputeSchemaHash_ExcludesIndexConstraintNamed's job now.
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
