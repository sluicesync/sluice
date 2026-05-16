//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

import (
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"
)

// Corpus is one of the workload shapes sluice's backup chunks see in
// practice. Unlike compressbench (which works on pre-rendered chunk
// BYTES because compression is byte-in/byte-out), jsonbench works on
// the STRUCTURED records — a slice of per-row `map[string]any` values
// shaped exactly like what `chunkWriter.WriteRow` /
// `changeChunkWriter.WriteChange` hand to `json.Marshal` in the
// production path. The harness encodes the slice record-by-record
// (one JSON document per row, as the JSON-Lines format does) and
// decodes each line back, so the numbers reflect the real per-record
// marshal/unmarshal the chunk path performs — not a single giant-doc
// encode that would mis-weight the allocator behaviour.
//
// Records carry the tagged-value envelope verbatim:
//
//   - {"_t":"i64","v":N}                int64
//   - {"_t":"u64","v":"<decimal-str>"}  uint64 (string to dodge 2^53)
//   - {"_t":"f64","v":N}                float64
//   - {"_t":"bytes","v":"<base64>"}     []byte
//   - {"_t":"time","v":"<RFC3339Nano>"} time.Time
//   - {"_t":"map","v":{...}}            nested JSON object
//   - bare string / bool / null         natively-roundtrippable
//
// json_change reuses the CDC change wrapper shape from
// backup_change_chunk.go: {"_t":"insert","schema":..,"table":..,
// "row":{...},"position":{"engine":..,"token":..}}.
type Corpus struct {
	Name string

	// Records are the per-line values, each a map[string]any exactly as
	// the production WriteRow / WriteChange builds before json.Marshal.
	Records []map[string]any
}

// CorpusRowCount is the default rows-per-corpus. Override via env var
// SLUICE_JSONBENCH_ROWS for decision-grade benchmarking; see
// rowsFromEnv(). 50_000 keeps the default `go test` run near ~30s on a
// laptop; 1_000_000 is the decision-grade scale the report cites.
const CorpusRowCount = 50_000

// corpusGen pairs a corpus name with its row generator. Fixed PRNG
// seed per corpus so successive runs stay comparable across commits.
type corpusGen struct {
	name string
	fn   func(rows int, rng *rand.Rand) []map[string]any
}

// corpusGens is the ordered registry. Mirrors compressbench's four
// shapes (text_heavy / numeric_heavy / binary_heavy / json_mixed) so
// results are directly comparable, plus json_change for the CDC /
// incremental-restore path (the change-chunk decode is equally
// DR-critical and has a distinct shape — nested row maps inside a
// kind-tagged wrapper).
var corpusGens = []corpusGen{
	{"text_heavy", genTextHeavy},
	{"numeric_heavy", genNumericHeavy},
	{"binary_heavy", genBinaryHeavy},
	{"json_mixed", genJSONMixed},
	{"json_change", genJSONChange},
}

// newCorpusRNG builds the deterministic PRNG a corpus generator uses.
// Fixed per-corpus seed so successive runs stay comparable across
// commits; factored out so RunAll's one-corpus-at-a-time path and
// generateCorpora share identical seeding.
func newCorpusRNG(seed [32]byte) *rand.Rand {
	return rand.New(rand.NewChaCha8(seed))
}

// generateCorpora builds every corpus deterministically. Used by the
// small-scale test / Go-benchmark paths; RunAll generates corpora one
// at a time (see its comment) to bound peak RAM at decision scale.
func generateCorpora(rows int) []Corpus {
	if rows <= 0 {
		rows = CorpusRowCount
	}
	out := make([]Corpus, 0, len(corpusGens))
	for _, g := range corpusGens {
		seed := [32]byte{byte(len(g.name))}
		out = append(out, Corpus{Name: g.name, Records: g.fn(rows, newCorpusRNG(seed))})
	}
	return out
}

// genTextHeavy approximates an OLTP table dominated by varchar / text
// columns. Mirrors compressbench's genTextHeavy shape exactly so the
// two harnesses' "text_heavy" rows describe the same workload.
func genTextHeavy(rows int, rng *rand.Rand) []map[string]any {
	words := []string{
		"the", "of", "and", "to", "a", "in", "that", "have", "I", "it",
		"for", "not", "on", "with", "he", "as", "you", "do", "at", "this",
		"but", "his", "by", "from", "they", "we", "say", "her", "she", "or",
		"an", "will", "my", "one", "all", "would", "there", "their", "what", "so",
		"up", "out", "if", "about", "who", "get", "which", "go", "me", "when",
		"make", "can", "like", "time", "no", "just", "him", "know", "take", "people",
		"into", "year", "your", "good", "some", "could", "them", "see", "other", "than",
		"then", "now", "look", "only", "come", "its", "over", "think", "also", "back",
	}
	firstNames := []string{"alice", "bob", "carol", "dave", "eve", "faythe", "grace", "heidi", "ivan", "judy"}
	lastNames := []string{"smith", "jones", "taylor", "brown", "patel", "garcia", "kim", "nguyen", "wong", "ali"}
	domains := []string{"example.com", "mail.org", "corp.co", "biz.io", "data.net"}
	out := make([]map[string]any, rows)
	for i := 0; i < rows; i++ {
		first := firstNames[rng.IntN(len(firstNames))]
		last := lastNames[rng.IntN(len(lastNames))]
		bioWords := 20 + rng.IntN(40)
		bio := make([]byte, 0, bioWords*5)
		for w := 0; w < bioWords; w++ {
			if w > 0 {
				bio = append(bio, ' ')
			}
			bio = append(bio, words[rng.IntN(len(words))]...)
		}
		out[i] = map[string]any{
			"id":    map[string]any{"_t": "i64", "v": int64(i)},
			"name":  first + " " + last,
			"email": first + "." + last + strconv.Itoa(i) + "@" + domains[rng.IntN(len(domains))],
			"bio":   string(bio),
		}
	}
	return out
}

// genNumericHeavy approximates an OLAP fact table — all integer
// columns wearing the i64 envelope, plus a u64 and an f64 so the
// numeric-precision fidelity paths are exercised.
func genNumericHeavy(rows int, rng *rand.Rand) []map[string]any {
	out := make([]map[string]any, rows)
	for i := 0; i < rows; i++ {
		out[i] = map[string]any{
			"id":           map[string]any{"_t": "i64", "v": int64(i)},
			"user_id":      map[string]any{"_t": "i64", "v": int64(rng.IntN(100_000))},
			"tenant_id":    map[string]any{"_t": "i64", "v": int64(rng.IntN(500))},
			"amount_cents": map[string]any{"_t": "i64", "v": int64(rng.IntN(10_000_000))},
			// A genuine >2^53 magnitude so any float64 coercion in a
			// candidate surfaces as a fidelity failure, not a silent
			// rounding the default scale would hide.
			"big_id":       map[string]any{"_t": "u64", "v": strconv.FormatUint(1<<62+uint64(i), 10)},
			"ratio":        map[string]any{"_t": "f64", "v": rng.Float64()},
			"discount_bps": map[string]any{"_t": "i64", "v": int64(rng.IntN(10_000))},
			// Decimals travel as strings per docs/value-types.md.
			"price": "12345.67890123456789",
		}
	}
	return out
}

// genBinaryHeavy approximates bytea / blob columns: the base64 bytes
// envelope is what hits the JSON codec.
func genBinaryHeavy(rows int, rng *rand.Rand) []map[string]any {
	out := make([]map[string]any, rows)
	for i := 0; i < rows; i++ {
		payload := make([]byte, 256)
		for j := range payload {
			payload[j] = byte(rng.UintN(256))
		}
		checksum := make([]byte, 32)
		for j := range checksum {
			checksum[j] = byte(rng.UintN(256))
		}
		out[i] = map[string]any{
			"id":       map[string]any{"_t": "i64", "v": int64(i)},
			"checksum": map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(checksum)},
			"payload":  map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(payload)},
		}
	}
	return out
}

// genJSONMixed is the representative OLTP-mixed shape — the corpus
// closest to what most production tables look like. Includes a string
// carrying HTML-significant characters so the HTML-escaping behaviour
// of every candidate is exercised on a real corpus, not only in the
// dedicated fidelity test.
func genJSONMixed(rows int, rng *rand.Rand) []map[string]any {
	statuses := []string{"pending", "active", "suspended", "deleted", "archived"}
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]map[string]any, rows)
	for i := 0; i < rows; i++ {
		offset := time.Duration(rng.Int64N(int64(365 * 24 * time.Hour)))
		ts := t0.Add(offset).UTC().Format(time.RFC3339Nano)
		metadata := map[string]any{
			"source":  labels[rng.IntN(len(labels))],
			"retries": map[string]any{"_t": "i64", "v": int64(rng.IntN(10))},
			// HTML-significant content: a < b && c > d — the exact bytes
			// that diverge between HTML-escaping and non-escaping codecs.
			"note": "a < b && c > d </tag>",
		}
		out[i] = map[string]any{
			"id":         map[string]any{"_t": "i64", "v": int64(i)},
			"tenant_id":  map[string]any{"_t": "i64", "v": int64(rng.IntN(200))},
			"status":     statuses[rng.IntN(len(statuses))],
			"created_at": map[string]any{"_t": "time", "v": ts},
			"label":      labels[rng.IntN(len(labels))],
			"active":     i%2 == 0,
			"deleted_at": nil, // SQL NULL — must round-trip to nil
			"metadata":   map[string]any{"_t": "map", "v": metadata},
		}
	}
	return out
}

// genJSONChange mirrors the CDC change-chunk wrapper (changeWire in
// backup_change_chunk.go): a kind-tagged record carrying schema/table,
// a row map of tagged-value envelopes, and an engine-tagged position.
// Restore of an incremental chain decodes millions of these, so the
// change-wrapper decode is on the DR-critical axis the report weights.
func genJSONChange(rows int, rng *rand.Rand) []map[string]any {
	kinds := []string{"insert", "update", "delete"}
	tables := []string{"users", "orders", "events", "ledger"}
	out := make([]map[string]any, rows)
	for i := 0; i < rows; i++ {
		kind := kinds[rng.IntN(len(kinds))]
		row := map[string]any{
			"id":         map[string]any{"_t": "i64", "v": int64(i)},
			"amount":     map[string]any{"_t": "i64", "v": int64(rng.IntN(1_000_000))},
			"name":       fmt.Sprintf("row-%d <%d>", i, rng.IntN(100)),
			"updated_at": map[string]any{"_t": "time", "v": time.Unix(int64(i), 0).UTC().Format(time.RFC3339Nano)},
			"blob":       map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString([]byte{byte(i), byte(i >> 8), 0x00, 0xff})},
		}
		rec := map[string]any{
			"_t":     kind,
			"schema": "public",
			"table":  tables[rng.IntN(len(tables))],
			"position": map[string]any{
				"engine": "postgres",
				"token":  fmt.Sprintf("0/%X", 0x1A2B000+i),
			},
		}
		switch kind {
		case "insert":
			rec["row"] = row
		case "update":
			rec["before"] = row
			rec["after"] = row
		case "delete":
			rec["before"] = row
		}
		out[i] = rec
	}
	return out
}
