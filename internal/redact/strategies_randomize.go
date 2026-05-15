// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"

	"github.com/orware/sluice/internal/ir"
)

// PII Phase 2.c first wave — replay-stable randomize strategies.
//
// MySQL Enterprise's data-masking-component ships pure-random
// generators (`gen_rnd_pan`, `gen_rnd_us_phone`, etc.) whose outputs
// differ across calls. Sluice deviates: every randomize:* strategy
// is **deterministic per (table, column, primary-key)** so a CDC
// resume or cold-start re-apply produces the same target values.
// This makes sync mode idempotent — operators replaying a backup
// see the same redacted values both before and after the replay.
// The deviation is documented in ADR-0039.
//
// Each generator:
//
//   - Refuses if seed is nil. The caller (Registry.ApplyRow or
//     pipeline.redactRow) supplies a 32-byte SHA-256 seed derived
//     from streamID + table + column + PK values; a nil seed means
//     no PK was available, which is an operator misconfiguration
//     (preflight catches it).
//   - Uses [math/rand/v2.New] with a seeded [rand.ChaCha8] source.
//     Crypto-strength randomness is not required (the output is a
//     placeholder, not a secret); ChaCha8 is fast and stable
//     across Go versions.
//
// Adding new randomize:* strategies: implement a Name() that starts
// with "randomize:" (the prefix Registry.ApplyRow / pipeline use
// to detect seed-requiring strategies) and refuse on seed==nil at
// the top of Redact. No interface marker needed.

// errRandomizeSeedRequired is the shared refusal for randomize:*
// strategies invoked without a seed. The pipeline's preflight should
// catch the no-PK case before the strategy ever sees a nil seed;
// this guard is a defense-in-depth for direct API users.
func errRandomizeSeedRequired(name string, col *ir.Column) error {
	return fmt.Errorf("redact: column %s strategy %s requires a per-row seed (table has no primary key, or seed plumbing was not wired); randomize:* strategies are replay-stable only when the source table has a PK",
		colIdentity(col), name)
}

// newSeededRand turns the 32-byte SHA-256 seed into a ChaCha8 source.
// ChaCha8 needs a 32-byte seed; we use the supplied 32 bytes verbatim.
// A shorter seed is right-padded with zeros (defensive — production
// callers always supply exactly 32 bytes).
func newSeededRand(seed []byte) *rand.Rand {
	var key [32]byte
	copy(key[:], seed)
	src := rand.NewChaCha8(key)
	return rand.New(src)
}

// RandomizeInt generates an integer uniformly distributed in
// [Min, Max] inclusive, derived from the per-row seed. Refuses if
// the input value is non-nil and its Go type is not one of the
// integer flavours (int, int8, int16, int32, int64, uint*, *int8…).
// The output is always int64 (matches sluice's IR integer column
// representation).
//
// Bounds:
//
//   - Min == Max → the generator returns Min always (allowed; the
//     CLI parser also tolerates it — useful for "redact to a fixed
//     value" workflows that want to type-check as integer).
//   - Min > Max → refused at CLI parse time, but defensively the
//     generator returns an error if it sees the case at runtime.
type RandomizeInt struct {
	Min int64
	Max int64
}

// Name returns "randomize:int:<min>,<max>".
func (r RandomizeInt) Name() string {
	return fmt.Sprintf("randomize:int:%d,%d", r.Min, r.Max)
}

// Redact returns a seeded random int64 in [Min, Max]. See type doc
// for accepted input shapes.
func (r RandomizeInt) Redact(col *ir.Column, val any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired(r.Name(), col)
	}
	if r.Min > r.Max {
		return nil, fmt.Errorf("redact: column %s strategy %s has Min > Max", colIdentity(col), r.Name())
	}
	if val != nil {
		switch val.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			// ok — integer-flavour input
		default:
			return nil, fmt.Errorf("redact: column %s has unsupported type %T for randomize:int strategy (only integer types are supported)", colIdentity(col), val)
		}
	}
	rng := newSeededRand(seed)
	span := r.Max - r.Min + 1 // inclusive range
	if span <= 0 {
		// Min == Max → span == 1
		return r.Min, nil
	}
	// rand.Int64N requires a positive bound; cast through uint64 to
	// dodge the case where Max-Min+1 overflows signed int64 (e.g.
	// Min=math.MinInt64, Max=math.MaxInt64). Practical operator inputs
	// stay well clear of that range but the math is cheap.
	uspan := uint64(span)
	out := r.Min + int64(rng.Uint64N(uspan))
	return out, nil
}

// RandomizeEmail generates `<rand-local>@<rand-domain>.test` where
// the local part is 6-12 lowercase ASCII letters and the domain is
// 5-10 lowercase ASCII letters. The lengths are themselves derived
// from the seed (so two seeded calls produce different-length
// outputs when their seeds differ).
//
// The `.test` TLD is reserved by RFC 6761 for testing purposes and
// will never resolve to a real domain — operators using sluice's
// randomize:email output can be confident no surrogate accidentally
// collides with a real address.
//
// Accepts any value (nil included); the original is discarded.
// String columns are the typical target; binary or numeric columns
// will get a runtime type-coercion at the engine's prepareValue
// step.
type RandomizeEmail struct{}

// Name returns "randomize:email".
func (RandomizeEmail) Name() string { return "randomize:email" }

// Redact returns a seeded random email-shaped string.
func (RandomizeEmail) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired("randomize:email", col)
	}
	rng := newSeededRand(seed)
	// Local part: 6-12 lowercase letters. Domain: 5-10 lowercase letters.
	localLen := 6 + rng.IntN(7)  // 6..12 inclusive
	domainLen := 5 + rng.IntN(6) // 5..10 inclusive
	local := randomLowerAlpha(rng, localLen)
	domain := randomLowerAlpha(rng, domainLen)
	return local + "@" + domain + ".test", nil
}

// randomLowerAlpha builds an n-char lowercase ASCII string by
// drawing from rng. Shared by RandomizeEmail's local/domain
// generators; defined as a helper rather than inlined for
// readability + future reuse.
func randomLowerAlpha(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	out := make([]byte, n)
	for i := range out {
		out[i] = alphabet[rng.IntN(len(alphabet))]
	}
	return string(out)
}

// RandomizeUSPhone generates a US-style phone number in
// `XXX-XXX-XXXX` form. Area code (positions 0-2) and exchange code
// (positions 4-6) are restricted to valid ranges per NANP rules:
//
//   - Area code:  200-999, never 555 (reserved for fictional use)
//   - Exchange:   200-999
//
// Subscriber number (positions 8-11) can be any 0000-9999. The
// constraints mean every output is a syntactically valid NANP
// number that won't collide with reserved test ranges.
//
// Refuses on nil seed. Accepts any value; the original is discarded.
type RandomizeUSPhone struct{}

// Name returns "randomize:us-phone".
func (RandomizeUSPhone) Name() string { return "randomize:us-phone" }

// Redact returns a seeded random US-style phone number.
func (RandomizeUSPhone) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired("randomize:us-phone", col)
	}
	rng := newSeededRand(seed)
	// Area code: 200-999, but never 555. Reject-and-resample is
	// cleaner than an arithmetic transform that would skew the
	// distribution across the 800-range.
	var area int
	for {
		area = 200 + rng.IntN(800) // 200..999
		if area != 555 {
			break
		}
	}
	exchange := 200 + rng.IntN(800) // 200..999
	subscriber := rng.IntN(10000)   // 0..9999
	return fmt.Sprintf("%03d-%03d-%04d", area, exchange, subscriber), nil
}

// RandomizeUUID generates a UUIDv4 in canonical 8-4-4-4-12
// hyphenated form. Sets the version (bits 48-51 = 0100) and variant
// (bits 64-65 = 10) per RFC 4122 §4.1.1 / §4.1.3 so the output
// validates as a v4 UUID under any compliant parser.
//
// All 16 underlying bytes come from the seeded RNG (minus the two
// bit-patches), so two different seeds produce different UUIDs;
// the same seed always produces the same UUID.
//
// Refuses on nil seed. Accepts any value; the original is discarded.
type RandomizeUUID struct{}

// Name returns "randomize:uuid".
func (RandomizeUUID) Name() string { return "randomize:uuid" }

// Redact returns a seeded random UUIDv4 string.
func (RandomizeUUID) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired("randomize:uuid", col)
	}
	rng := newSeededRand(seed)
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], rng.Uint64())
	binary.BigEndian.PutUint64(b[8:16], rng.Uint64())
	// Version 4 (bits 12-15 of time_hi_and_version, which is byte
	// index 6 in network byte order): clear top 4 bits, set 0100.
	b[6] = (b[6] & 0x0F) | 0x40
	// Variant RFC 4122 (bits 6-7 of clock_seq_hi_and_reserved, byte
	// index 8): clear top 2 bits, set 10.
	b[8] = (b[8] & 0x3F) | 0x80
	// Render as 8-4-4-4-12 hex.
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
