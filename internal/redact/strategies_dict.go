// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// PII Phase 3 — dictionary-based redaction strategies (v0.61.0).
//
// Two new strategies map source values into operator-supplied
// dictionaries:
//
//   - [RandomizeDict] — picks a dictionary entry per-row using the
//     v0.59.0 PK-derived seed contract. Same source PK → same dict
//     entry across re-runs. Inherits the no-PK preflight refusal
//     because Name() starts with "randomize:".
//   - [TokenizeDict] — picks a dictionary entry by HMAC of the input
//     value. Same input value → same dict entry regardless of PK,
//     and (crucially) regardless of which table/column the value
//     came from. Operators use this for stable per-value surrogates
//     that survive across tables — every "Alice" maps to the same
//     dict entry everywhere in the database.
//
// Both strategies materialise the full dictionary at parser-time
// (the strategy struct holds `Entries []string`). The per-strategy
// memory cost is one slice header plus the dict's data; typical
// dictionaries are 10s-100s of entries which is trivial. See ADR-0040
// for the determinism rationale and the contract differences between
// these two strategies.

// PII Phase 4 (ADR-0041): the hardcoded `tokenize:dict` HMAC
// constant was DELETED. tokenize:dict now REQUIRES an operator
// keyset (via --keyset-source); there is NO synthetic back-compat
// keyset entry and NO zero-surrogate-drift commitment (clean break
// — sluice is pre-users). The key is resolved from the loaded
// keyset at strategy-construction time and carried on TokenizeDict.Key.

// RandomizeDict picks a dictionary entry per row using the per-row
// seed (PII Phase 3, v0.61.0). The first 8 bytes of the v0.59.0
// SHA-256 seed are interpreted as a little-endian uint64; the
// resulting integer modulo len(Entries) selects the entry.
//
// Because [Name] returns `randomize:dict:<name>` (i.e. starts with
// "randomize:"), this strategy inherits the no-PK preflight refusal
// automatically — the pipeline's preflight matches by prefix, not
// by type. ADR-0039's PK-keyed determinism contract applies: same
// source row → same dict entry across runs, CDC resumes, and
// backup→restore.
//
// Empty Entries refused at Redact time (defense in depth; the
// dictionary loader refuses empty dicts at config-load time, so this
// case should be unreachable in normal operation).
type RandomizeDict struct {
	// DictName is the dictionary's operator-supplied name (e.g.
	// "first_names"). Used only by [Name] for audit-log identification.
	// Named DictName (not "Name") to avoid shadowing the [Name] method.
	DictName string

	// Entries is the resolved dictionary content. Pre-loaded by
	// [LoadDictionaries] and embedded into the strategy at parser-time
	// so [Redact] is self-contained at row-process time.
	Entries []string
}

// Name returns "randomize:dict:<name>".
func (r RandomizeDict) Name() string {
	return "randomize:dict:" + r.DictName
}

// Redact returns a dictionary entry selected by the per-row seed.
// See type doc for the selection algorithm.
func (r RandomizeDict) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired(r.Name(), col)
	}
	if len(r.Entries) == 0 {
		return nil, fmt.Errorf("redact: column %s strategy %s has 0 dictionary entries (loader should have refused this earlier)", colIdentity(col), r.Name())
	}
	idx := seedToIndex(seed, len(r.Entries))
	return r.Entries[idx], nil
}

// seedToIndex reduces the seed to an index into a slice of length n.
// Reads the first 8 bytes of seed as little-endian uint64 and reduces
// modulo n. The first 8 bytes are sufficient — the v0.59.0 seed is
// SHA-256 output (32 bytes); collapsing to a uint64 still leaves more
// entropy than any reasonable dictionary size demands.
func seedToIndex(seed []byte, n int) int {
	if n <= 0 {
		return 0 // defensive — caller should have refused empty dict
	}
	var buf [8]byte
	copy(buf[:], seed)
	v := binary.LittleEndian.Uint64(buf[:])
	return int(v % uint64(n))
}

// TokenizeDict picks a dictionary entry by HMAC of the source value
// (PII Phase 3, v0.61.0). Determinism contract: same input value →
// same dictionary entry regardless of PK, regardless of which
// (table, column) the value came from.
//
// This is the FIRST sluice strategy whose output depends on the
// input value rather than the row's PK. Operators use it for stable
// per-value surrogates that survive across tables — every "Alice" in
// every column of every table maps to the same dictionary entry.
//
// The contract:
//
//	output = Entries[HMAC_SHA256(key, streamID || ":" || dictName || ":" || input) mod len(Entries)]
//
// The streamID prefix prevents one stream's tokenization from being
// trivially inverted by an attacker who also has another stream's
// data (a small but real concern for tenant-isolated multi-stream
// operators). An empty streamID still produces deterministic output
// — different streams that operators want to share a tokenization
// space just declare the same `--stream-id`.
//
// Because [Name] returns `tokenize:dict:<name>` (does NOT start with
// "randomize:"), this strategy is NOT subject to the no-PK preflight
// — `tokenize:dict` works on any table regardless of PK presence,
// which is the whole point: the output is keyed by the input value,
// not by the row's identity.
//
// Empty Entries refused at Redact time (defense in depth).
type TokenizeDict struct {
	// DictName is the dictionary's operator-supplied name (e.g.
	// "first_names"). Used by [Name] and threaded into the HMAC
	// message so two different dictionaries with overlapping content
	// produce different tokenizations. Named DictName (not "Name")
	// to avoid shadowing the [Name] method.
	DictName string

	// Entries is the resolved dictionary content. Pre-loaded by
	// [LoadDictionaries] and embedded into the strategy at parser-time.
	Entries []string

	// StreamID is the active stream's identifier, captured at
	// strategy-construction time. Mixed into the HMAC message so
	// different streams produce different tokenizations of the same
	// input. Empty when no stream-id is available (e.g. `migrate`
	// path); the HMAC still computes deterministically.
	StreamID string

	// Key is the HMAC secret resolved from the operator keyset at
	// strategy-construction time (PII Phase 4, ADR-0041). REQUIRED:
	// the Phase 1/3 hardcoded constant was deleted (clean break,
	// no shim). Two streams sharing the same keyset key produce
	// identical surrogates — the cross-stream-stability primitive.
	// An empty Key is refused at [TokenizeDict.Redact] time; the
	// CLI/YAML parsers + preflight refuse it earlier with an
	// actionable message.
	Key []byte
}

// Name returns "tokenize:dict:<name>".
func (t TokenizeDict) Name() string {
	return "tokenize:dict:" + t.DictName
}

// Redact returns a dictionary entry selected by HMAC of the input
// value. See type doc for the selection algorithm.
//
// Nil input: returned as nil (NULL passes through). Real-world
// tokenization on a NULL doesn't have a useful surrogate — every
// NULL would map to the same dict entry, which is operator-
// surprising. Treat NULL as pass-through; operators wanting a
// specific NULL replacement compose with `static:` instead.
//
// Non-nil non-string input: stringified via fmt.Sprintf("%v", val)
// so the strategy tolerates integer / boolean / []byte source
// columns. The canonical-format choice mirrors [DeriveRowSeed]'s
// approach — operator's data is what it is; the only contract is
// stable input → stable output.
func (t TokenizeDict) Redact(col *ir.Column, val any, _ []byte) (any, error) {
	if val == nil {
		return nil, nil
	}
	if len(t.Entries) == 0 {
		return nil, fmt.Errorf("redact: column %s strategy %s has 0 dictionary entries (loader should have refused this earlier)", colIdentity(col), t.Name())
	}
	if len(t.Key) == 0 {
		return nil, fmt.Errorf("redact: column %s strategy %s has no keyset key; tokenize:dict requires --keyset-source (the built-in v0.61.0 key was removed in PII Phase 4, ADR-0041)", colIdentity(col), t.Name())
	}
	var canonical string
	switch v := val.(type) {
	case string:
		canonical = v
	case []byte:
		canonical = string(v)
	default:
		canonical = fmt.Sprintf("%v", v)
	}
	m := hmac.New(sha256.New, t.Key)
	_, _ = m.Write([]byte(t.StreamID))
	_, _ = m.Write([]byte(":"))
	_, _ = m.Write([]byte(t.DictName))
	_, _ = m.Write([]byte(":"))
	_, _ = m.Write([]byte(canonical))
	sum := m.Sum(nil)
	idx := seedToIndex(sum, len(t.Entries))
	return t.Entries[idx], nil
}

// ResolveDictEntries looks up a dictionary by name in the
// loader-resolved map and returns a defensive copy of the entries.
// Refuses with an operator-actionable error when the dictionary is
// missing — the caller (CLI / YAML parsers) is expected to wrap with
// the rule's source location.
//
// Returns ([]string{...}, nil) for a found dictionary or
// (nil, error) for a missing one. A nil/empty `loaded` map means no
// dictionaries were declared at all — every lookup misses.
func ResolveDictEntries(loaded map[string][]string, name string) ([]string, error) {
	if name == "" {
		return nil, errors.New("redact: dictionary name is empty")
	}
	entries, ok := loaded[name]
	if !ok {
		available := make([]string, 0, len(loaded))
		for k := range loaded {
			available = append(available, k)
		}
		if len(available) == 0 {
			return nil, fmt.Errorf("redact: dictionary %q is not declared (no dictionaries are loaded; declare it under YAML 'dictionaries:' — CLI form does not support inline dictionary declarations)", name)
		}
		return nil, fmt.Errorf("redact: dictionary %q is not declared (available dictionaries: %v)", name, available)
	}
	// Defensive copy: callers may mutate the slice (we don't, but
	// future strategy authors might). The cost is one allocation per
	// strategy construction, which is negligible at startup.
	out := make([]string, len(entries))
	copy(out, entries)
	return out, nil
}

// Compile-time interface checks: every Phase 3 strategy must satisfy
// [Strategy].
var (
	_ Strategy = RandomizeDict{}
	_ Strategy = TokenizeDict{}
)
