//go:build compressbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package compressbench

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"
)

// Corpus is one of the four workload shapes sluice's backup chunks
// see in practice. Each corpus carries its rendered JSON-Lines bytes
// (a representative chunk pre-compression) plus a short label the
// markdown emitter uses.
//
// The renderer below produces ~RowCount rows per corpus. The roadmap
// (Item 7) suggests ~1M rows for production-grade benchmarking;
// CorpusRowCount defaults to 50_000 to keep `go test -bench` runs
// under ~30s on a laptop. Operators wanting the full 1M-row pass can
// set SLUICE_COMPRESSBENCH_ROWS=1000000 and re-run.
type Corpus struct {
	Name string
	Data []byte
}

// CorpusRowCount is the default rows-per-corpus. Override via env var
// SLUICE_COMPRESSBENCH_ROWS for production-grade benchmarking; see
// rowsFromEnv().
const CorpusRowCount = 50_000

// generateCorpora builds all four corpora deterministically (fixed
// PRNG seed) so successive bench runs stay comparable across commits.
// Returns nil and an error only if JSON marshalling fails — corpus
// generators don't otherwise touch I/O.
func generateCorpora(rows int) ([]Corpus, error) {
	if rows <= 0 {
		rows = CorpusRowCount
	}
	out := make([]Corpus, 0, 4)
	for _, gen := range []struct {
		name string
		fn   func(rows int, rng *rand.Rand) ([]byte, error)
	}{
		{"text_heavy", genTextHeavy},
		{"numeric_heavy", genNumericHeavy},
		{"binary_heavy", genBinaryHeavy},
		{"json_mixed", genJSONMixed},
	} {
		// Fresh deterministic PRNG per corpus — different seeds keep
		// corpus shapes from accidentally rhyming with each other.
		seed := [32]byte{byte(len(gen.name))}
		rng := rand.New(rand.NewChaCha8(seed))
		data, err := gen.fn(rows, rng)
		if err != nil {
			return nil, fmt.Errorf("generate %s: %w", gen.name, err)
		}
		out = append(out, Corpus{Name: gen.name, Data: data})
	}
	return out, nil
}

// renderChunk emits a sluice-shaped chunk header + N JSON Lines rows
// drawn from genRow. The byte layout mirrors backup_chunk.go: a
// header line `{"_h":1,"columns":[...]}` followed by `\n`-separated
// row JSON. This matches what gzip / zstd / snappy actually see in
// the prod write path.
func renderChunk(columns []string, rows int, genRow func(rowIdx int) map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	header := map[string]any{
		"_h":      1,
		"columns": columns,
	}
	hb, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("render header: %w", err)
	}
	buf.Write(hb)
	buf.WriteByte('\n')
	for i := 0; i < rows; i++ {
		row := genRow(i)
		rb, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("render row %d: %w", i, err)
		}
		buf.Write(rb)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// genTextHeavy approximates an OLTP table dominated by varchar / text
// columns: id (int), name (short varchar), email (varchar with @-
// pattern), bio (longer text with English-shape word distribution).
// English text compresses well — this corpus exercises the high-ratio
// end of the algorithm spectrum.
func genTextHeavy(rows int, rng *rand.Rand) ([]byte, error) {
	cols := []string{"id", "name", "email", "bio"}
	// Word pool drawn from the most common English nouns and verbs
	// (frequency-weighted toward common terms — natural-text-like
	// entropy without depending on an external corpus file).
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
	return renderChunk(cols, rows, func(i int) map[string]any {
		first := firstNames[rng.IntN(len(firstNames))]
		last := lastNames[rng.IntN(len(lastNames))]
		bioWords := 20 + rng.IntN(40)
		var bio bytes.Buffer
		for w := 0; w < bioWords; w++ {
			if w > 0 {
				bio.WriteByte(' ')
			}
			bio.WriteString(words[rng.IntN(len(words))])
		}
		return map[string]any{
			"id":    map[string]any{"_t": "i64", "v": int64(i)},
			"name":  first + " " + last,
			"email": first + "." + last + strconv.Itoa(i) + "@" + domains[rng.IntN(len(domains))],
			"bio":   bio.String(),
		}
	})
}

// genNumericHeavy approximates an OLAP fact table: ids, foreign keys,
// metric counters (integers), money columns (decimals encoded as
// strings in the tagged envelope). Repetitive integer patterns
// compress reasonably well; the bigint envelope wrapping
// `{"_t":"i64","v":N}` adds repeated framing tokens that compressors
// dedupe efficiently.
func genNumericHeavy(rows int, rng *rand.Rand) ([]byte, error) {
	cols := []string{"id", "user_id", "tenant_id", "amount_cents", "quantity", "discount_bps"}
	return renderChunk(cols, rows, func(i int) map[string]any {
		return map[string]any{
			"id":           map[string]any{"_t": "i64", "v": int64(i)},
			"user_id":      map[string]any{"_t": "i64", "v": int64(rng.IntN(100_000))},
			"tenant_id":    map[string]any{"_t": "i64", "v": int64(rng.IntN(500))},
			"amount_cents": map[string]any{"_t": "i64", "v": int64(rng.IntN(10_000_000))},
			"quantity":     map[string]any{"_t": "i64", "v": int64(rng.IntN(1000))},
			"discount_bps": map[string]any{"_t": "i64", "v": int64(rng.IntN(10_000))},
		}
	})
}

// genBinaryHeavy approximates a table with bytea / blob columns —
// e.g. image thumbnails, encrypted blobs, binary protocol payloads.
// The base64-encoded tagged envelope shape `{"_t":"bytes","v":"..."}`
// is what hits the compressor. Random bytes are incompressible; the
// envelope framing + base64 padding contribute the only compressible
// tokens, so this corpus pins the floor of the ratio range.
func genBinaryHeavy(rows int, rng *rand.Rand) ([]byte, error) {
	cols := []string{"id", "checksum", "payload"}
	return renderChunk(cols, rows, func(i int) map[string]any {
		// 256-byte random payload — representative of thumbnail-size
		// blobs or short binary protocol frames. Larger payloads would
		// just amplify the "random bytes don't compress" signal.
		payload := make([]byte, 256)
		for j := range payload {
			payload[j] = byte(rng.UintN(256))
		}
		checksum := make([]byte, 32)
		for j := range checksum {
			checksum[j] = byte(rng.UintN(256))
		}
		return map[string]any{
			"id":       map[string]any{"_t": "i64", "v": int64(i)},
			"checksum": map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(checksum)},
			"payload":  map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(payload)},
		}
	})
}

// genJSONMixed is the representative OLTP-mixed shape: a primary key,
// a few small varchars, a timestamp, an enum-like status, and a
// flexible JSON column carrying a small object. Most production
// tables resemble this more than any of the three pure-shape corpora.
func genJSONMixed(rows int, rng *rand.Rand) ([]byte, error) {
	cols := []string{"id", "tenant_id", "status", "created_at", "label", "metadata"}
	statuses := []string{"pending", "active", "suspended", "deleted", "archived"}
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	return renderChunk(cols, rows, func(i int) map[string]any {
		offset := time.Duration(rng.Int64N(int64(365 * 24 * time.Hour)))
		ts := t0.Add(offset).UTC().Format(time.RFC3339Nano)
		metadata := map[string]any{
			"source":  labels[rng.IntN(len(labels))],
			"retries": rng.IntN(10),
			"flags": []string{
				labels[rng.IntN(len(labels))],
				labels[rng.IntN(len(labels))],
			},
		}
		return map[string]any{
			"id":         map[string]any{"_t": "i64", "v": int64(i)},
			"tenant_id":  map[string]any{"_t": "i64", "v": int64(rng.IntN(200))},
			"status":     statuses[rng.IntN(len(statuses))],
			"created_at": map[string]any{"_t": "time", "v": ts},
			"label":      labels[rng.IntN(len(labels))],
			"metadata":   map[string]any{"_t": "map", "v": metadata},
		}
	})
}
