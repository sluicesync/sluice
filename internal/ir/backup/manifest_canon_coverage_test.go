// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// canonMutations maps a manifest FIELD PATH ("<Type>.<Field>") to a
// mutation that must change the canonical signed bytes. Every exported
// field of every type in [canonRoster] needs an entry here or in
// [canonExempt] — see [TestCanonicalManifestBytes_FieldCoverage].
var canonMutations = map[string]func(*Manifest){
	"Manifest.FormatVersion":  func(m *Manifest) { m.FormatVersion = 5 },
	"Manifest.CreatedAt":      func(m *Manifest) { m.CreatedAt = m.CreatedAt.Add(time.Second) },
	"Manifest.SourceEngine":   func(m *Manifest) { m.SourceEngine = "mysql" },
	"Manifest.Kind":           func(m *Manifest) { m.Kind = BackupKindFull },
	"Manifest.BackupID":       func(m *Manifest) { m.BackupID = "x" },
	"Manifest.ParentBackupID": func(m *Manifest) { m.ParentBackupID = "x" },
	"Manifest.SchemaHash":     func(m *Manifest) { m.SchemaHash = "x" },
	"Manifest.StartPosition":  func(m *Manifest) { m.StartPosition.Token = "x" },
	"Manifest.EndPosition":    func(m *Manifest) { m.EndPosition.Token = "x" },

	// ADR-0183: the schema itself, via its raw on-disk bytes. This is the
	// entry whose ABSENCE from the old curated allowlist is the whole
	// reason this gate is reflect-driven.
	"Manifest.Schema": func(m *Manifest) {
		m.Schema.Tables[0].Indexes = []*ir.Index{
			{Name: "idx", Method: `btree; CREATE ROLE attacker SUPERUSER; --`},
		}
	},

	"Manifest.Tables":        func(m *Manifest) { m.Tables = m.Tables[:1] },
	"Manifest.ChangeChunks":  func(m *Manifest) { m.ChangeChunks = m.ChangeChunks[:1] },
	"Manifest.SchemaDelta":   func(m *Manifest) { m.SchemaDelta = nil },
	"Manifest.SchemaHistory": func(m *Manifest) { m.SchemaHistory = nil },
	"Manifest.ChainEncryption": func(m *Manifest) {
		m.ChainEncryption = nil
	},

	"TableManifest.Schema":   func(m *Manifest) { m.Tables[0].Schema = "other" },
	"TableManifest.Name":     func(m *Manifest) { m.Tables[0].Name = "other" },
	"TableManifest.RowCount": func(m *Manifest) { m.Tables[0].RowCount = 99 },
	// SEC-F1 (v4): reassigning a row chunk to a DIFFERENT parent table.
	"TableManifest.Chunks": func(m *Manifest) {
		m.Tables[0].Chunks, m.Tables[1].Chunks = m.Tables[1].Chunks, m.Tables[0].Chunks
	},

	"ChunkInfo.File":     func(m *Manifest) { m.Tables[0].Chunks[0].File = "chunks/elsewhere" },
	"ChunkInfo.SHA256":   func(m *Manifest) { m.Tables[0].Chunks[0].SHA256 = "x" },
	"ChunkInfo.RowCount": func(m *Manifest) { m.Tables[0].Chunks[0].RowCount = 77 },

	"ChainEncryption.Algorithm":  func(m *Manifest) { m.ChainEncryption.Algorithm = "ChaCha20-Poly1305" },
	"ChainEncryption.Mode":       func(m *Manifest) { m.ChainEncryption.Mode = "per-chunk" },
	"ChainEncryption.KEKMode":    func(m *Manifest) { m.ChainEncryption.KEKMode = "aws-kms" },
	"ChainEncryption.KEKRef":     func(m *Manifest) { m.ChainEncryption.KEKRef = "arn:attacker" },
	"ChainEncryption.WrappedCEK": func(m *Manifest) { m.ChainEncryption.WrappedCEK = []byte{0x00} },
	"ChainEncryption.Argon2id":   func(m *Manifest) { m.ChainEncryption.Argon2id = nil },

	"Argon2idParams.Salt":        func(m *Manifest) { m.ChainEncryption.Argon2id.Salt = []byte{0xff} },
	"Argon2idParams.Memory":      func(m *Manifest) { m.ChainEncryption.Argon2id.Memory = 1 },
	"Argon2idParams.Iterations":  func(m *Manifest) { m.ChainEncryption.Argon2id.Iterations = 1 },
	"Argon2idParams.Parallelism": func(m *Manifest) { m.ChainEncryption.Argon2id.Parallelism = 1 },
	"Argon2idParams.KeyLen":      func(m *Manifest) { m.ChainEncryption.Argon2id.KeyLen = 16 },

	"SchemaDeltaEntry.Kind":   func(m *Manifest) { m.SchemaDelta[0].Kind = SchemaDeltaDropTable },
	"SchemaDeltaEntry.Schema": func(m *Manifest) { m.SchemaDelta[0].Schema = "other" },
	"SchemaDeltaEntry.Table":  func(m *Manifest) { m.SchemaDelta[0].Table = "other" },
	"SchemaDeltaEntry.Before": func(m *Manifest) {
		m.SchemaDelta[0].Before = &ir.Table{Name: "before", Columns: []*ir.Column{{Name: "c", Type: ir.Text{}}}}
	},
	"SchemaDeltaEntry.After": func(m *Manifest) { m.SchemaDelta[0].After.Columns[0].Type = ir.Text{} },

	"SchemaHistoryEntry.StreamID":       func(m *Manifest) { m.SchemaHistory[0].StreamID = "s2" },
	"SchemaHistoryEntry.Schema":         func(m *Manifest) { m.SchemaHistory[0].Schema = "other" },
	"SchemaHistoryEntry.Table":          func(m *Manifest) { m.SchemaHistory[0].Table = "other" },
	"SchemaHistoryEntry.AnchorPosition": func(m *Manifest) { m.SchemaHistory[0].AnchorPosition.Token = "0/999" },
	"SchemaHistoryEntry.TableJSON":      func(m *Manifest) { m.SchemaHistory[0].TableJSON = []byte("{}") },
}

// canonExempt maps a manifest field path to the REASON it is deliberately
// outside the signed canonical bytes. An exemption is a claim about how
// the field is bound (or why it needs no binding); it is not a shrug. Each
// one below either names the binding that covers it transitively or names
// the reader that makes tampering it inert.
//
// An exempt field is asserted to be genuinely INVISIBLE to the canonical
// bytes, so a future change that starts folding it fails this gate and
// forces the exemption to be removed — a stale exemption cannot rot.
var canonExempt = map[string]string{
	"Manifest.SluiceVersion": "informational provenance — the build that wrote the manifest. Nothing gates on it " +
		"(restore/verify read it only to NAME the writing release in the schema-fingerprint refusal, " +
		"schemaFingerprintCauses), so an edit misattributes a message and changes no decision. Deliberately " +
		"unsigned so a re-stamped version string never invalidates a signature.",

	"Manifest.PartialState": "a signature is never produced over an in_progress manifest — enforced, not assumed, by " +
		"lineage.refuseSigningInProgressManifest (pinned by TestSignManifestRefusesInProgressManifest). With no " +
		"valid signature over an in-progress manifest to forge from, in_progress→complete is unreachable, and " +
		"complete→in_progress lands on refuseInterruptedManifest — loud in both directions.",

	"Manifest.ProgressSidecar": "nil on every finalized (and therefore every signed) manifest, and the READ path " +
		"clears it during ADR-0086 sidecar replay (lineage.replayManifestProgress) — folding it would make a " +
		"decoded manifest render different bytes than the writer signed.",

	"Manifest.CDCPositionCommitsAfterRows": "bound transitively AND checkably: ComputeBackupID folds it at " +
		"FormatVersion >= FormatVersionCDCPositionBinding, Manifest.BackupID is itself folded here, and " +
		"restoreManifestIntegrityPreflights recomputes the id on BOTH chain shapes. Flipping the flag alone fails " +
		"that recompute; flipping it AND re-stamping BackupID changes these canonical bytes and fails the MAC.",

	"TableManifest.Partial": "read only by the backup RESUME classifier (tableManifestFullyComplete), which reads " +
		"the prior IN-PROGRESS manifest — never signed. No restore/verify/export path consults it.",

	"ChunkInfo.Encryption": "per-chunk envelope metadata, bound by the ADR-0152 chunk AAD rather than by the " +
		"manifest signature: the chunk's SHA256 IS folded here, so the bytes are authentic, and any edit to how " +
		"they are interpreted (plaintext↔ciphertext, a swapped WrappedCEK) fails the authenticated open loudly " +
		"with SLUICE-E-BACKUP-CHUNK-AUTH-FAILED. Descent into ChunkEncryption stops here for the same reason.",
}

// canonRoster is the set of manifest-reachable struct types whose exported
// fields this gate enumerates. It is deliberately explicit rather than a
// full transitive reflect walk: these are exactly the types
// [CanonicalManifestBytesForVersion] renders FIELD BY FIELD (a new field on
// any of them silently drops out of the signature otherwise). Types under
// an exempt parent field (ChunkEncryption, ProgressSidecarRef) are not
// descended into — their exemption reason covers the whole subtree — and
// ir.Table / ir.Schema are not descended into because they are folded
// WHOLE (as raw bytes at v5, as a fingerprint for schema deltas), which is
// precisely what makes them immune to field additions.
var canonRoster = []reflect.Type{
	reflect.TypeOf(Manifest{}),
	reflect.TypeOf(TableManifest{}),
	reflect.TypeOf(ChunkInfo{}),
	reflect.TypeOf(ChainEncryption{}),
	reflect.TypeOf(Argon2idParams{}),
	reflect.TypeOf(SchemaDeltaEntry{}),
	reflect.TypeOf(SchemaHistoryEntry{}),
}

// TestCanonicalManifestBytes_FieldCoverage is the FAIL-BY-DEFAULT
// replacement for the old curated TamperSensitivity allowlist.
//
// The allowlist version was green while Manifest.Schema — the entire
// source schema, from which restore drives every CREATE TABLE / CREATE
// INDEX / CREATE POLICY statement — was outside the signature (ADR-0183,
// audit-2026-08-01 C1). It could not have caught that, because a field
// nobody adds to a curated list is invisible to a curated list: the gate
// needed a human to do the exact thing it existed to guarantee.
//
// This version enumerates every exported field of every type in
// [canonRoster] via reflect and demands each one carry EITHER a mutator
// that provably changes the signed bytes OR a written exemption. A new
// field on any of those types fails the build until someone decides which.
// Same fail-by-default shape as TestCopyPhaseFlagParityMigratorStreamer in
// internal/pipeline.
func TestCanonicalManifestBytes_FieldCoverage(t *testing.T) {
	base := canon(t, fixedSignedManifest(), 2)

	seen := make(map[string]bool)
	for _, typ := range canonRoster {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if !f.IsExported() {
				continue
			}
			path := typ.Name() + "." + f.Name
			seen[path] = true

			mut, hasMut := canonMutations[path]
			reason, hasExempt := canonExempt[path]
			switch {
			case hasMut && hasExempt:
				t.Errorf("%s: has BOTH a mutator and an exemption — pick one", path)
			case !hasMut && !hasExempt:
				t.Errorf("%s is not covered: add a mutator to canonMutations proving the signature covers it, "+
					"or an entry to canonExempt with a written reason it is deliberately outside the signed bytes", path)
			case hasExempt:
				if strings.TrimSpace(reason) == "" {
					t.Errorf("%s: exemption carries no reason", path)
					continue
				}
				// An exemption is a claim that the field is INVISIBLE to the
				// canonical bytes. Nothing verifies that claim for a field with
				// no mutator, so exempt fields get the inverse assertion below
				// via canonExemptInvisible.
			default:
				m := fixedSignedManifest()
				mut(m)
				if canon(t, m, 2) == base {
					t.Errorf("mutation %q did not change the canonical bytes — the field is NOT under the signature", path)
				}
			}
		}
	}

	// Every registered key must correspond to a real field, so a rename
	// cannot leave a decorative entry behind claiming coverage that no
	// longer exists.
	for path := range canonMutations {
		if !seen[path] {
			t.Errorf("canonMutations has %q, which is not an exported field of any type in canonRoster (renamed? removed?)", path)
		}
	}
	for path := range canonExempt {
		if !seen[path] {
			t.Errorf("canonExempt has %q, which is not an exported field of any type in canonRoster (renamed? removed?)", path)
		}
	}

	// Sequence is a signed freshness anchor, not a manifest field.
	if canon(t, fixedSignedManifest(), 3) == base {
		t.Error("sequence change did not alter the canonical bytes")
	}
}

// canonExemptInvisible mutates each exempt field and asserts the canonical
// bytes DO NOT change — the inverse half of the coverage gate. Without it
// an exemption is unfalsifiable: the day someone folds an exempt field in,
// its reason becomes wrong and nothing says so.
func TestCanonicalManifestBytes_ExemptFieldsStayInvisible(t *testing.T) {
	base := canon(t, fixedSignedManifest(), 2)
	mutations := map[string]func(*Manifest){
		"Manifest.SluiceVersion":               func(m *Manifest) { m.SluiceVersion = "different" },
		"Manifest.PartialState":                func(m *Manifest) { m.PartialState = BackupStateInProgress },
		"Manifest.ProgressSidecar":             func(m *Manifest) { m.ProgressSidecar = &ProgressSidecarRef{File: "p.jsonl", AttemptID: "a"} },
		"Manifest.CDCPositionCommitsAfterRows": func(m *Manifest) { m.CDCPositionCommitsAfterRows = true },
		"TableManifest.Partial":                func(m *Manifest) { m.Tables[0].Partial = true },
		"ChunkInfo.Encryption":                 func(m *Manifest) { m.Tables[0].Chunks[0].Encryption = &ChunkEncryption{Algorithm: "AES-256-GCM"} },
	}
	// The two maps must agree: an exemption with no invisibility check is
	// an unverified claim, and a check for a non-exempt field is stale.
	for path := range canonExempt {
		if _, ok := mutations[path]; !ok {
			t.Errorf("exempt field %q has no invisibility check here — the exemption is unverified", path)
		}
	}
	for path, mut := range mutations {
		if _, ok := canonExempt[path]; !ok {
			t.Errorf("%q is checked as invisible but is not in canonExempt", path)
			continue
		}
		m := fixedSignedManifest()
		mut(m)
		if canon(t, m, 2) != base {
			t.Errorf("exempt field %q DID change the canonical bytes — it is now signed, so remove its canonExempt entry and give it a mutator", path)
		}
	}
}

// TestManifestSchemaBytesWriteReadRoundTrip is the correctness pin for the
// ADR-0183 fold: the bytes the WRITER folds must be the bytes the READER
// folds back. A mismatch here would make every freshly-written signature
// fail to verify on read — the worst possible outcome of this change, and
// the one most reachable by an innocent-looking edit to the write path.
func TestManifestSchemaBytesWriteReadRoundTrip(t *testing.T) {
	written := fixedSignedManifest()
	doc, err := MarshalManifest(written)
	if err != nil {
		t.Fatal(err)
	}
	readBack := &Manifest{}
	if err := json.Unmarshal(doc, readBack); err != nil {
		t.Fatal(err)
	}

	wb, err := written.SchemaCanonBytes()
	if err != nil {
		t.Fatal(err)
	}
	rb, err := readBack.SchemaCanonBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wb, rb) {
		t.Fatalf("write/read schema-byte drift:\n write %q\n read  %q", wb, rb)
	}

	// Independently extracted: the document's own `schema` member,
	// compacted here rather than through the production helper, so this
	// leg does not agree with the writer merely by sharing its code.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(doc, &probe); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if err := json.Compact(&want, probe["schema"]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wb, want.Bytes()) {
		t.Fatalf("folded bytes are not the document's schema member:\n folded %q\n on-disk %q", wb, want.Bytes())
	}

	// And the whole canonical payload agrees across the boundary.
	if canon(t, written, 2) != canon(t, readBack, 2) {
		t.Fatal("canonical bytes differ between the written and the read-back manifest — a fresh signature would not verify")
	}
}

// TestManifestSchemaBytesInMemoryFallbackMatches pins the third rendering:
// a manifest that has NEVER crossed the JSON boundary (built in memory by
// a test or a future caller) must fold the same bytes it will fold once
// written. This is what makes the fallback in [Manifest.SchemaCanonBytes]
// safe — sign-before-write and sign-after-write agree.
func TestManifestSchemaBytesInMemoryFallbackMatches(t *testing.T) {
	inMemory := fixedSignedManifest()
	beforeWrite := canon(t, inMemory, 2)
	if _, err := MarshalManifest(inMemory); err != nil {
		t.Fatal(err)
	}
	if afterWrite := canon(t, inMemory, 2); afterWrite != beforeWrite {
		t.Fatalf("canonical bytes changed when the manifest was rendered:\n before %q\n after  %q", beforeWrite, afterWrite)
	}
}

// TestManifestSchemaBytesArePreservedNotRemarshalled is the pin for the
// ADR-0183 decision itself: the fold is the bytes ON DISK, not a
// re-marshal of the decoded struct.
//
// The proof is a manifest whose `schema` carries a member this build's
// ir.Schema does not know — exactly what a manifest written by a NEWER
// binary (or a future field addition) looks like from here. A re-marshal
// would silently drop it; the raw capture keeps it, so the bytes a
// verifier folds are the bytes the signer folded. Without this property a
// field addition to ir.Column would invalidate every v5 signature in the
// field at once.
func TestManifestSchemaBytesArePreservedNotRemarshalled(t *testing.T) {
	doc := []byte(`{
	  "format_version": 6,
	  "source_engine": "postgres",
	  "kind": "full",
	  "schema": {
	    "Tables": [{"Name": "orders", "future_column_property": "kept"}]
	  }
	}`)
	m := &Manifest{}
	if err := json.Unmarshal(doc, m); err != nil {
		t.Fatal(err)
	}
	raw, err := m.SchemaCanonBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("future_column_property")) {
		t.Fatalf("the unknown schema member was dropped — the fold is re-marshalling the decoded struct, not preserving the on-disk bytes: %q", raw)
	}
	if !strings.Contains(canon(t, m, 0), "future_column_property") {
		t.Fatal("the canonical bytes do not carry the on-disk schema bytes")
	}
	// A manifest document with NO `schema` member at all has nothing to
	// capture, so the fold falls back to marshalling the (nil) decoded
	// Schema — the same "null" a document carrying `"schema": null`
	// captures directly. Both mean "this manifest has no schema".
	noSchema := &Manifest{}
	if err := json.Unmarshal([]byte(`{"format_version":6,"kind":"full"}`), noSchema); err != nil {
		t.Fatal(err)
	}
	got, err := noSchema.SchemaCanonBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "null" {
		t.Fatalf("absent schema member folded %q; want the nil-Schema marshal \"null\"", got)
	}
}
