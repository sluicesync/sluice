// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// ADR-0181 (audit SEC-2) gates for the injective chunk-AAD encoding.
//
// The load-bearing test here is a PROPERTY test, not a golden. A golden
// proves one input renders to one output and says nothing about
// COLLISIONS — which is exactly what the raw-concatenation encoding got
// wrong, and exactly why the defect survived a golden-pinned v7 table
// binding. The property is: DISTINCT field tuples must render to
// DISTINCT AAD, for every binding kind, over a corpus whose values carry
// the structural delimiters (`\n`, `=`, `:`, `|`) and whole field-name
// fragments (`\nschema=`, `\ntable=`, `\nindex=`, …) in every position.
//
// The sweep runs at BOTH tiers and asserts opposite outcomes: injective
// at v9, colliding at v8. The v8 leg is the vacuity guard — if the
// corpus ever stops carrying real forgery material the v8 leg goes green
// and this test fails loudly, rather than the v9 leg passing for the
// wrong reason.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// aadFuzzFragments are the byte sequences the FROZEN v5–v8 encoding
// treats as structure. A value built from these can forge a boundary
// under raw concatenation; under length-prefixed tokens it cannot,
// because a decoder reads exactly len bytes and never scans for a
// delimiter. The plain letters/digits are filler so a fragment can land
// mid-value rather than only at a boundary.
var aadFuzzFragments = []string{
	"", "a", "b", "z", "0", "1",
	"\n", "=", ":", "|", "\n=", "0:", "12:",
	"schema=", "table=", "file=", "kind=", "index=", "source_engine=", "created_at=",
	"\nschema=", "\ntable=", "\nfile=", "\nkind=", "\nindex=", "\nsource_engine=", "\ncreated_at=",
	"sluice-chunk-aad/v1\n", "sluice-cek-binding/v1\n", "sluice-chunk-aad/v2",
}

// aadFuzzValue draws a value by concatenating 0–3 fragments, so a
// delimiter can appear at the head, the tail, in the middle, or AS the
// entire value.
func aadFuzzValue(r *rand.Rand) string {
	var b strings.Builder
	for n := r.Intn(4); n > 0; n-- {
		b.WriteString(aadFuzzFragments[r.Intn(len(aadFuzzFragments))])
	}
	return b.String()
}

// aadTuple is the full set of AAD-carried fields a store adversary can
// influence: the manifest identity's two string fields, the recorded
// chunk path, the parent table, and the change-chunk ordinal. CreatedAt
// is fixed — it is a time, not an attacker-supplied string.
type aadTuple struct {
	sourceEngine string
	kind         string
	file         string
	schema       string
	table        string
	index        int
}

func (tu aadTuple) manifest(fv int) *Manifest {
	return &Manifest{
		FormatVersion: fv,
		CreatedAt:     time.Date(2026, 7, 26, 9, 0, 0, 0, time.UTC),
		SourceEngine:  tu.sourceEngine,
		Kind:          tu.kind,
	}
}

func (tu aadTuple) encryptedChunk() *ChunkInfo {
	return &ChunkInfo{File: tu.file, Encryption: &ChunkEncryption{Algorithm: "AES-256-GCM"}}
}

// aadBindingKind is one renderer under test plus the projection of the
// tuple onto the fields that renderer actually binds. Tuples agreeing on
// the projection SHOULD render identically, so only projection-distinct
// tuples are collision candidates. The projection uses %q — Go's own
// quoting, injective and entirely independent of the code under test, so
// the oracle is not a second copy of the encoder.
type aadBindingKind struct {
	name   string
	key    func(aadTuple) string
	render func(aadTuple, int) string
}

// aadBindingKinds is the family matrix: every binding [ChunkAAD] gates.
// Row and change chunks share ONE chain CEK, so a cross-KIND collision
// is as exploitable as a within-kind one — which is why all three
// renderers feed a SINGLE collision map below rather than one map each.
var aadBindingKinds = []aadBindingKind{
	{
		name: "row_chunk",
		key: func(tu aadTuple) string {
			return fmt.Sprintf("%q %q %q %q %q", tu.sourceEngine, tu.kind, tu.file, tu.schema, tu.table)
		},
		render: func(tu aadTuple, fv int) string {
			return string(ChunkAADFor(tu.manifest(fv), tu.encryptedChunk(), tu.schema, tu.table))
		},
	},
	{
		name: "change_chunk",
		key: func(tu aadTuple) string {
			return fmt.Sprintf("%q %q %q %d", tu.sourceEngine, tu.kind, tu.file, tu.index)
		},
		render: func(tu aadTuple, fv int) string {
			return string(ChangeChunkAADFor(tu.manifest(fv), tu.encryptedChunk(), tu.index))
		},
	},
	{
		name: "cek_binding",
		key: func(tu aadTuple) string {
			return fmt.Sprintf("%q %q", tu.sourceEngine, tu.kind)
		},
		render: func(tu aadTuple, fv int) string { return CEKBinding(tu.manifest(fv)) },
	},
}

// aadCollisions renders the corpus at format version fv through every
// binding kind and returns a description of each distinct-tuple /
// identical-AAD pair it finds, plus the number of distinct (kind, tuple)
// identities rendered.
func aadCollisions(fv int, tuples []aadTuple) (found []string, identities int) {
	seen := make(map[string]string, len(tuples)*len(aadBindingKinds))
	ids := make(map[string]bool, len(tuples)*len(aadBindingKinds))
	for _, k := range aadBindingKinds {
		for _, tu := range tuples {
			id := k.name + " " + k.key(tu)
			ids[id] = true
			aad := k.render(tu, fv)
			prev, ok := seen[aad]
			if !ok {
				seen[aad] = id
				continue
			}
			if prev == id {
				continue // same identity rendered twice — not a collision
			}
			found = append(found, fmt.Sprintf("%s\n  collides with\n%s\n  both render %q", prev, id, aad))
		}
	}
	return found, len(ids)
}

// aadFuzzCorpus builds the deterministic tuple corpus. Deduplicated on
// the FULL tuple so the identity bookkeeping above stays honest.
func aadFuzzCorpus(n int) []aadTuple {
	r := rand.New(rand.NewSource(0x5EC2)) //nolint:gosec // deterministic corpus, not cryptographic
	seen := make(map[aadTuple]bool, n)
	out := make([]aadTuple, 0, n)
	for len(out) < n {
		tu := aadTuple{
			sourceEngine: aadFuzzValue(r),
			kind:         aadFuzzValue(r),
			file:         aadFuzzValue(r),
			schema:       aadFuzzValue(r),
			table:        aadFuzzValue(r),
			index:        r.Intn(3),
		}
		// Empty Kind canonicalizes to full (the [ComputeBackupID] rule,
		// pinned in TestChunkAAD_GoldenDerivation), so "" and "full" are the
		// SAME identity by design. Never emit the empty form — otherwise the
		// oracle would report a documented equivalence as a collision.
		if tu.kind == "" {
			tu.kind = BackupKindFull
		}
		if seen[tu] {
			continue
		}
		seen[tu] = true
		out = append(out, tu)
	}
	return out
}

// aadBoundaryShiftCorpus emits, for every adjacent field pair in the
// bindings, the two tuples raw concatenation cannot tell apart: the
// `\n<name>=` delimiter moved from the tail of the left field to the
// head of the right one. Deterministic, so the v8 leg's collision count
// never depends on the RNG — the random sweep looks for shapes nobody
// thought of, this half guarantees the ones we know about.
func aadBoundaryShiftCorpus() []aadTuple {
	pieces := []string{"", "a", "public", "orders\n", "=x"}
	base := aadTuple{sourceEngine: "postgres", kind: BackupKindFull, file: "chunks/t-0", schema: "public", table: "t"}
	shift := func(l, r, name string, set func(*aadTuple, string, string)) (aadTuple, aadTuple) {
		a, b := base, base
		set(&a, l+"\n"+name+"="+r, "tail")
		set(&b, l, r+"\n"+name+"=tail")
		return a, b
	}
	out := make([]aadTuple, 0, 6*len(pieces)*len(pieces))
	for _, l := range pieces {
		for _, r := range pieces {
			a, b := shift(l, r, "kind", func(tu *aadTuple, x, y string) { tu.sourceEngine, tu.kind = x, y })
			out = append(out, a, b)
			a, b = shift(l, r, "schema", func(tu *aadTuple, x, y string) { tu.file, tu.schema = x, y })
			out = append(out, a, b)
			a, b = shift(l, r, "table", func(tu *aadTuple, x, y string) { tu.schema, tu.table = x, y })
			out = append(out, a, b)
		}
	}
	return out
}

// aadMinLegacyCollisions is the floor the v8 leg must clear. The
// deterministic boundary-shift corpus alone produces well above it, so a
// count that drops here means the corpus stopped exercising the forgery,
// not that the forgery went away — those bytes are frozen.
const aadMinLegacyCollisions = 50

// TestChunkAAD_InjectiveProperty is the ADR-0181 gate. At
// [FormatVersionInjectiveChunkAAD] no two distinct field tuples may
// render to the same AAD, across every binding kind and across kinds.
// The v8 leg is the non-vacuity guard: the SAME corpus must still
// collide under the frozen raw-concatenation encoding, so a corpus that
// silently stopped carrying forgery material fails here instead of
// passing the v9 leg for free.
func TestChunkAAD_InjectiveProperty(t *testing.T) {
	tuples := append(aadBoundaryShiftCorpus(), aadFuzzCorpus(4000)...)

	legacy, legacyIDs := aadCollisions(FormatVersionCDCPositionBinding, tuples)
	if len(legacy) < aadMinLegacyCollisions {
		t.Fatalf("VACUOUS CORPUS: %d identities rendered under the raw-concatenation (v8) encoding produced only %d collision(s), want >= %d. "+
			"That encoding IS forgeable (see TestChunkAAD_LegacyEncodingIsForgeable); a corpus that cannot show it "+
			"cannot vouch for the v9 leg below either — restore the delimiter fragments in aadFuzzFragments / aadBoundaryShiftCorpus",
			legacyIDs, len(legacy), aadMinLegacyCollisions)
	}

	injective, injectiveIDs := aadCollisions(FormatVersionInjectiveChunkAAD, tuples)
	if injectiveIDs != legacyIDs {
		t.Fatalf("identity count differs between tiers (%d v9 vs %d v8) — the projections and the corpus must be tier-independent", injectiveIDs, legacyIDs)
	}
	if len(injective) > 0 {
		t.Errorf("the v9 encoding is NOT injective — %d collision(s), first:\n%s", len(injective), injective[0])
	}
	t.Logf("corpus: %d tuples / %d identities; raw-concat collisions: %d; injective collisions: %d",
		len(tuples), legacyIDs, len(legacy), len(injective))
}

// aadForgeryPairs are the concrete forgeries the audit reproduced,
// expressed as pairs of DISTINCT tuples that the frozen v5–v8 encoding
// renders to IDENTICAL bytes. Each is a named instance of the class;
// they are pinned separately from the random sweep because a named
// repro is what a future reader needs to understand the defect.
var aadForgeryPairs = []struct {
	name string
	kind string // which aadBindingKinds entry renders it
	a, b aadTuple
}{
	{
		// The audit's harmful direction, end to end. The adversary creates a
		// source table whose NAME embeds `\nschema=public\ntable=orders`, so
		// its chunk seals under an AAD that also parses as "chunk P' of
		// public.orders". A store adversary then rewrites the manifest's
		// File to P' and lists the chunk under the legitimate public.orders —
		// and it decrypts, injecting attacker rows into the victim's table.
		name: "attacker_rows_into_a_legitimate_table",
		kind: "row_chunk",
		a: aadTuple{
			sourceEngine: "postgres", kind: BackupKindFull,
			file:   "chunks/evil/evil-0.jsonl.gz",
			schema: "public", table: "evil\nschema=public\ntable=orders",
		},
		b: aadTuple{
			sourceEngine: "postgres", kind: BackupKindFull,
			file:   "chunks/evil/evil-0.jsonl.gz\nschema=public\ntable=evil",
			schema: "public", table: "orders",
		},
	},
	{
		// The (schema, table) boundary on its own: the delimiter shifts one
		// field to the left and both parents render identically.
		name: "parent_schema_table_boundary_shift",
		kind: "row_chunk",
		a: aadTuple{
			sourceEngine: "postgres", kind: BackupKindFull,
			file: "c-0", schema: "s\ntable=t", table: "",
		},
		b: aadTuple{
			sourceEngine: "postgres", kind: BackupKindFull,
			file: "c-0", schema: "s", table: "t\ntable=",
		},
	},
	{
		// The manifest-identity fields are forgeable too — SourceEngine is
		// AAD-bound precisely so a cross-engine chunk cannot be replayed
		// (audit H-1 leans on it), and the raw encoding lets it slide into
		// Kind.
		name: "source_engine_kind_boundary_shift",
		kind: "cek_binding",
		a: aadTuple{
			sourceEngine: "postgres\nkind=full", kind: BackupKindIncremental,
		},
		b: aadTuple{
			sourceEngine: "postgres", kind: "full\nkind=" + BackupKindIncremental,
		},
	},
	{
		// CROSS-KIND: row chunks and change chunks share one chain CEK, so a
		// change chunk whose recorded path ends in the row-chunk table suffix
		// seals identically to a row chunk of a table named `…\nindex=0`.
		// Replay order is semantic for change chunks; a relabel across kinds
		// evades the ordinal binding entirely.
		name: "change_chunk_relabelled_as_a_row_chunk",
		kind: "", // rendered by BOTH kinds — see the test
		a: aadTuple{
			sourceEngine: "postgres", kind: BackupKindIncremental,
			file: "changes/c-0.jsonl.gz\nschema=public\ntable=t", index: 0,
		},
		b: aadTuple{
			sourceEngine: "postgres", kind: BackupKindIncremental,
			file: "changes/c-0.jsonl.gz", schema: "public", table: "t\nindex=0",
		},
	},
}

// renderByKind renders tu with the named binding kind.
func renderByKind(t *testing.T, name string, tu aadTuple, fv int) string {
	t.Helper()
	for _, k := range aadBindingKinds {
		if k.name == name {
			return k.render(tu, fv)
		}
	}
	t.Fatalf("unknown binding kind %q", name)
	return ""
}

// TestChunkAAD_LegacyEncodingIsForgeable pins the defect ADR-0181
// exists for, in BOTH directions at once: each named pair renders to
// IDENTICAL bytes under the frozen v5–v8 encoding and to DIFFERENT bytes
// at v9.
//
// The v8 half is not "pinning a bug" — those bytes are on-disk contract
// for every chain ever written and can never be repaired in place, so
// the collision is a permanent property of that tier. Asserting it is
// what makes the v9 half meaningful: without it, a v9 renderer that
// simply differed from v8 in some incidental way would look just as
// green.
func TestChunkAAD_LegacyEncodingIsForgeable(t *testing.T) {
	for _, p := range aadForgeryPairs {
		t.Run(p.name, func(t *testing.T) {
			kindA, kindB := p.kind, p.kind
			if p.kind == "" {
				// The cross-kind pair: a change chunk versus a row chunk.
				kindA, kindB = "change_chunk", "row_chunk"
			}
			legacyA := renderByKind(t, kindA, p.a, FormatVersionCDCPositionBinding)
			legacyB := renderByKind(t, kindB, p.b, FormatVersionCDCPositionBinding)
			if legacyA != legacyB {
				t.Fatalf("the raw-concatenation encoding no longer collides here:\n a = %q\n b = %q\n"+
					"v5–v8 bytes are FROZEN on-disk contract — if they changed, every chain ever written just became unreadable", legacyA, legacyB)
			}
			injectiveA := renderByKind(t, kindA, p.a, FormatVersionInjectiveChunkAAD)
			injectiveB := renderByKind(t, kindB, p.b, FormatVersionInjectiveChunkAAD)
			if injectiveA == injectiveB {
				t.Errorf("the v9 encoding still collides on this forgery:\n a = %q\n b = %q", injectiveA, injectiveB)
			}
		})
	}
}

// TestChunkAAD_InjectiveGoldenDerivation pins the EXACT v9 bytes — the
// on-disk contract for every chunk sealed from FormatVersion 9 onward,
// in the same shape as the [CanonicalManifestBytes] goldens. Each token
// is `<len>:<bytes>\n`. The v5–v8 goldens in chunk_binding_test.go are
// deliberately NOT touched by this: the two derivations are parallel,
// never one replacing the other.
func TestChunkAAD_InjectiveGoldenDerivation(t *testing.T) {
	m := bindingTestManifest(FormatVersionInjectiveChunkAAD)
	base := strings.Join([]string{
		"19:sluice-chunk-aad/v2",
		"10:created_at", "30:2026-07-08T12:30:45.123456789Z",
		"13:source_engine", "8:postgres",
		"4:kind", "4:full",
		"4:file", "23:chunks/users-0.jsonl.gz",
		"",
	}, "\n")
	if got := string(ChunkAAD(m, "chunks/users-0.jsonl.gz")); got != base {
		t.Errorf("v9 ChunkAAD drift:\n got %q\nwant %q\n(on-disk contract — changing it requires a new FormatVersion)", got, base)
	}

	enc := &ChunkInfo{File: "chunks/users-0.jsonl.gz", Encryption: &ChunkEncryption{Algorithm: "AES-256-GCM"}}
	wantRow := base + strings.Join([]string{"6:schema", "6:public", "5:table", "5:users", ""}, "\n")
	if got := string(ChunkAADFor(m, enc, "public", "users")); got != wantRow {
		t.Errorf("v9 row-chunk AAD drift:\n got %q\nwant %q", got, wantRow)
	}
	if got := string(ChunkAADForWrite(m, "chunks/users-0.jsonl.gz", "public", "users", []byte("cek"))); got != wantRow {
		t.Errorf("v9 write-side row-chunk AAD %q != read-side %q", got, wantRow)
	}

	wantChange := base + strings.Join([]string{"5:index", "1:3", ""}, "\n")
	if got := string(ChangeChunkAAD(m, "chunks/users-0.jsonl.gz", 3)); got != wantChange {
		t.Errorf("v9 change-chunk AAD drift:\n got %q\nwant %q", got, wantChange)
	}

	wantCEK := strings.Join([]string{
		"21:sluice-cek-binding/v2",
		"10:created_at", "30:2026-07-08T12:30:45.123456789Z",
		"13:source_engine", "8:postgres",
		"4:kind", "4:full",
		"",
	}, "\n")
	if got := CEKBinding(m); got != wantCEK {
		t.Errorf("v9 CEKBinding drift:\n got %q\nwant %q", got, wantCEK)
	}
}

// TestChunkAAD_TierBoundary pins the version gate itself: v8 renders the
// frozen raw concatenation, v9 the tokens, and nothing in between shifts.
// This is the assertion that catches a `>` written where a `>=` belongs —
// the mistake that would silently strand a whole tier's chunks.
func TestChunkAAD_TierBoundary(t *testing.T) {
	const file = "chunks/users-0.jsonl.gz"
	for _, v := range []int{
		FormatVersionEncryptedChunkBinding,
		FormatVersionSignedManifest,
		FormatVersionChunkTableBinding,
		FormatVersionCDCPositionBinding,
	} {
		m := bindingTestManifest(v)
		if got := string(ChunkAAD(m, file)); !strings.HasPrefix(got, chunkAADPrefix) {
			t.Errorf("FormatVersion %d rendered %q; every pre-v9 chunk on every store was sealed with the %q derivation", v, got, chunkAADPrefix)
		}
		if got := CEKBinding(m); !strings.HasPrefix(got, cekBindingPrefix) {
			t.Errorf("FormatVersion %d CEK binding %q; pre-v9 wraps used the %q derivation", v, got, cekBindingPrefix)
		}
	}
	m := bindingTestManifest(FormatVersionInjectiveChunkAAD)
	if got := string(ChunkAAD(m, file)); !strings.HasPrefix(got, "19:"+chunkAADPrefixV2+"\n") {
		t.Errorf("FormatVersion 9 rendered %q; want the length-prefixed %q tag", got, chunkAADPrefixV2)
	}
	if got := CEKBinding(m); !strings.HasPrefix(got, "21:"+cekBindingPrefixV2+"\n") {
		t.Errorf("FormatVersion 9 CEK binding %q; want the length-prefixed %q tag", got, cekBindingPrefixV2)
	}
	// A future version keeps the v9 encoding until a new tier is declared —
	// the gate is `>=`, so an unrecognised-but-newer stamp must not fall back
	// to the forgeable derivation.
	future := bindingTestManifest(FormatVersionInjectiveChunkAAD + 1)
	if got := string(ChunkAAD(future, file)); !strings.HasPrefix(got, "19:"+chunkAADPrefixV2+"\n") {
		t.Errorf("FormatVersion %d fell back to a pre-v9 derivation: %q", FormatVersionInjectiveChunkAAD+1, got)
	}
}
