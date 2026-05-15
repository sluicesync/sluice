// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"fmt"
	"math/rand/v2"
	"strconv"

	"github.com/orware/sluice/internal/ir"
)

// PII Phase 2.c second wave — checksum-aware replay-stable
// randomize strategies (v0.60.0).
//
// Same determinism contract as the first wave (see
// strategies_randomize.go): seed-derived ChaCha8 RNG; refusal on
// nil seed; same row in always produces same output. ADR-0039
// documents the rationale.
//
// What's new in this wave:
//
//   - `randomize:ssn`               — US SSN, reserved-range-avoiding
//   - `randomize:pan[:<brand>]`     — Luhn-valid PAN
//   - `randomize:ca-sin`            — Luhn-valid Canadian SIN
//   - `randomize:uk-nin`            — UK NIN, HMRC-shape
//   - `randomize:iban[:<country>]`  — mod-97-valid IBAN
//
// The PAN + CA SIN generators reuse [luhnCheckDigit] from
// internal/redact/luhn.go; the IBAN generator computes mod-97 check
// digits via [ibanCheckDigits] in internal/redact/iban.go. Both
// helpers are tested standalone so the strategy code stays focused
// on shape + RNG.

// errRandomizeBrand returns an operator-actionable refusal for an
// unsupported PAN brand. Listed brands track [supportedPANBrands].
func errRandomizeBrand(brand string) error {
	return fmt.Errorf("strategy 'randomize:pan': unknown brand %q (supported: visa, mastercard, amex)", brand)
}

// errRandomizeIBANCountry returns an operator-actionable refusal
// for an unsupported IBAN country code.
func errRandomizeIBANCountry(country string) error {
	return fmt.Errorf("strategy 'randomize:iban': unknown country code %q (supported: DE, GB, FR)", country)
}

// randDigits returns n decimal digits from rng as a string.
func randDigits(rng *rand.Rand, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('0' + rng.IntN(10))
	}
	return string(out)
}

// randUpperAlpha returns n uppercase ASCII letters from rng as a
// string, drawn from the supplied alphabet. If alphabet is empty,
// defaults to A-Z.
func randUpperAlpha(rng *rand.Rand, n int, alphabet string) string {
	if alphabet == "" {
		alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = alphabet[rng.IntN(len(alphabet))]
	}
	return string(out)
}

// RandomizeSSN generates a US Social Security Number in the
// canonical `XXX-XX-XXXX` form. The SSA's reserved ranges are
// avoided so output never collides with a real-world special-case:
//
//   - Area  (first group):  001-665 OR 667-899 (no 000; SSA's 666
//     skip is honoured; 900-999 are reserved ITIN ranges so we
//     stop at 899).
//   - Group (middle):       01-99   (no 00).
//   - Serial (last group):  0001-9999 (no 0000).
//
// The 666-skip is conservative but cheap; SSA documentation
// reserves the value administratively. Operators wanting an
// implementation that emits 666 can override via a static rule.
//
// Refuses on nil seed.
type RandomizeSSN struct{}

// Name returns "randomize:ssn".
func (RandomizeSSN) Name() string { return "randomize:ssn" }

// Redact returns a seeded random SSN.
func (RandomizeSSN) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired("randomize:ssn", col)
	}
	rng := newSeededRand(seed)
	// Area: 001-665, 667-899. Reject-and-resample on 666 keeps the
	// distribution flat across both sub-ranges.
	var area int
	for {
		area = 1 + rng.IntN(899) // 1..899
		if area != 666 {
			break
		}
	}
	group := 1 + rng.IntN(99)    // 1..99
	serial := 1 + rng.IntN(9999) // 1..9999
	return fmt.Sprintf("%03d-%02d-%04d", area, group, serial), nil
}

// supportedPANBrands lists the brands [RandomizePAN] accepts. The
// set tracks MySQL Enterprise's `gen_rnd_pan()` (Visa, Mastercard,
// AmEx); Discover / JCB / UnionPay are not in the Enterprise
// catalog and not shipped here. Operators wanting other brands can
// open an issue or use `randomize:pan` (random-brand) and override
// via a static rule.
//
// Each entry's prefix range maps to the brand-specific IIN per
// ISO/IEC 7812:
//
//   - Visa       — first digit 4, total length 16
//   - Mastercard — first digit 5 (or 2 in newer issuance; we ship 5
//     for the Enterprise compatibility surface), length 16
//   - AmEx       — first 2 digits 34 or 37, length 15
var supportedPANBrands = map[string]bool{
	"visa":       true,
	"mastercard": true,
	"amex":       true,
}

// RandomizePAN generates a synthetic credit-card PAN with a valid
// Luhn check digit. Brand selects the issuer-prefix + length per
// [supportedPANBrands]; an empty Brand string selects a brand at
// random from the supported set (deterministic per-seed).
//
// Output is digit-only (no separators); operators wanting hyphen
// separators should compose with a custom format pass.
//
// Refuses on nil seed. Brand validation happens at strategy-
// construction time (in the CLI / YAML parsers); the Redact path
// trusts the field is already vetted.
type RandomizePAN struct {
	Brand string // "" | "visa" | "mastercard" | "amex"
}

// Name returns "randomize:pan" or "randomize:pan:<brand>".
func (r RandomizePAN) Name() string {
	if r.Brand == "" {
		return "randomize:pan"
	}
	return "randomize:pan:" + r.Brand
}

// Redact returns a seeded random Luhn-valid PAN.
func (r RandomizePAN) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired(r.Name(), col)
	}
	if r.Brand != "" && !supportedPANBrands[r.Brand] {
		return nil, errRandomizeBrand(r.Brand)
	}
	rng := newSeededRand(seed)
	brand := r.Brand
	if brand == "" {
		// Random-brand: pick from a stable-ordered list so seed
		// determinism is preserved (map iteration order isn't).
		choices := []string{"visa", "mastercard", "amex"}
		brand = choices[rng.IntN(len(choices))]
	}
	var prefix string
	var totalLen int
	switch brand {
	case "visa":
		prefix = "4"
		totalLen = 16
	case "mastercard":
		prefix = "5"
		totalLen = 16
	case "amex":
		// AmEx uses 34 or 37; pick deterministically from seed.
		if rng.IntN(2) == 0 {
			prefix = "34"
		} else {
			prefix = "37"
		}
		totalLen = 15
	default:
		// Defensive — should be caught by Brand validation above.
		return nil, errRandomizeBrand(brand)
	}
	// Fill the body with random digits, leaving 1 slot for the
	// Luhn check digit.
	bodyLen := totalLen - len(prefix) - 1
	digits := prefix + randDigits(rng, bodyLen)
	check := luhnCheckDigit(digits)
	return digits + strconv.Itoa(check), nil
}

// RandomizeCASIN generates a Canadian Social Insurance Number in
// the canonical `XXX-XXX-XXX` form with a valid Luhn check digit.
// CA SINs are 9 digits and are required to satisfy Luhn per
// Government of Canada rules.
//
// The first digit encodes the province / federal status:
//
//   - 1: Atlantic provinces
//   - 2-3: Quebec
//   - 4-5: Ontario
//   - 6: Prairie provinces / NWT / Nunavut
//   - 7: British Columbia / Yukon
//   - 8: NOT issued (reserved)
//   - 9: Temporary residents
//   - 0: NOT issued (reserved)
//
// We exclude 0 and 8 from the first-digit pool to avoid emitting
// SINs that visibly cannot exist. The 9-prefix (temporary residents)
// IS included — it's a real shape and operators may want it
// represented in test data.
//
// Refuses on nil seed.
type RandomizeCASIN struct{}

// Name returns "randomize:ca-sin".
func (RandomizeCASIN) Name() string { return "randomize:ca-sin" }

// Redact returns a seeded random Luhn-valid CA SIN.
func (RandomizeCASIN) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired("randomize:ca-sin", col)
	}
	rng := newSeededRand(seed)
	// First digit from {1,2,3,4,5,6,7,9}; reject-and-resample on
	// 0 or 8 keeps the distribution uniform across the valid set.
	var first int
	for {
		first = rng.IntN(10) // 0..9
		if first != 0 && first != 8 {
			break
		}
	}
	body := strconv.Itoa(first) + randDigits(rng, 7) // 8 digits before check
	check := luhnCheckDigit(body)
	full := body + strconv.Itoa(check)
	// Render as XXX-XXX-XXX.
	return full[:3] + "-" + full[3:6] + "-" + full[6:], nil
}

// uknPrefixAlphabet is the curated subset of letters used for UK
// NIN prefix generation. The HMRC authoritative list is large and
// changes; this subset excludes letters HMRC reserves (D, F, I, Q,
// U, V) per published guidance. Suffix letters are restricted to
// the canonical {A,B,C,D} set per [RandomizeUKNIN] doc.
//
// Operators needing a different prefix set can replace
// `randomize:uk-nin` with a `static:` rule or a custom strategy.
const uknPrefixAlphabet = "ABCEGHJKLMNOPRSTWXYZ"

// uknSuffixAlphabet is the HMRC-canonical suffix-letter set for
// UK NINs. The mask preset (`mask:uk-nin`) accepts only this set;
// the randomize generator emits only this set.
const uknSuffixAlphabet = "ABCD"

// RandomizeUKNIN generates a UK National Insurance Number in the
// canonical `AA999999A` 9-char shape. The two prefix letters are
// drawn from [uknPrefixAlphabet] (HMRC-reserved letters excluded);
// the 6 digits are fully random; the suffix letter is from
// [uknSuffixAlphabet] = {A,B,C,D} per HMRC convention.
//
// Operators noting the HMRC authoritative prefix list is larger
// than [uknPrefixAlphabet] can override individual columns via
// `static:` or implement a custom Strategy — the curated subset
// keeps generator-output unambiguously NOT a real-world NIN
// (HMRC's full list contains letters this subset omits).
//
// Refuses on nil seed.
type RandomizeUKNIN struct{}

// Name returns "randomize:uk-nin".
func (RandomizeUKNIN) Name() string { return "randomize:uk-nin" }

// Redact returns a seeded random UK NIN.
func (RandomizeUKNIN) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired("randomize:uk-nin", col)
	}
	rng := newSeededRand(seed)
	prefix := randUpperAlpha(rng, 2, uknPrefixAlphabet)
	digits := randDigits(rng, 6)
	suffix := randUpperAlpha(rng, 1, uknSuffixAlphabet)
	return prefix + digits + suffix, nil
}

// supportedIBANCountries maps a country code to its canonical
// total IBAN length per ISO 13616-1 / SWIFT registry. The set
// tracks MySQL Enterprise's default IBAN demographic (DE/GB/FR);
// other countries can be added on operator demand.
var supportedIBANCountries = map[string]int{
	"DE": 22, // Germany — DE + 2 check + 18 BBAN
	"GB": 22, // United Kingdom — GB + 2 check + 4 bank + 6 sort + 8 account
	"FR": 27, // France — FR + 2 check + 23 BBAN
}

// RandomizeIBAN generates an International Bank Account Number
// with a valid mod-97 check digit. CountryCode selects the country
// (and therefore the total length); an empty CountryCode picks
// one at random from [supportedIBANCountries].
//
// Implementation note: the BBAN structure is country-specific
// (bank-code/sort-code/account-number layouts differ), but a
// simple "all-digits BBAN of the right length" generation is
// indistinguishable from a real one to anyone outside that
// country's banking system. Country-aware structural generation
// (e.g. emitting valid Bundesbank Bankleitzahl prefixes for DE)
// can be added later if operators demand it; for now, the
// generator's contract is "mod-97-valid and country-coded
// correctly" — which is what every IBAN validator checks.
//
// Refuses on nil seed. CountryCode validation happens at strategy-
// construction time.
type RandomizeIBAN struct {
	CountryCode string // "" | "DE" | "GB" | "FR"
}

// Name returns "randomize:iban" or "randomize:iban:<country>".
func (r RandomizeIBAN) Name() string {
	if r.CountryCode == "" {
		return "randomize:iban"
	}
	return "randomize:iban:" + r.CountryCode
}

// Redact returns a seeded random mod-97-valid IBAN.
func (r RandomizeIBAN) Redact(col *ir.Column, _ any, seed []byte) (any, error) {
	if seed == nil {
		return nil, errRandomizeSeedRequired(r.Name(), col)
	}
	if r.CountryCode != "" {
		if _, ok := supportedIBANCountries[r.CountryCode]; !ok {
			return nil, errRandomizeIBANCountry(r.CountryCode)
		}
	}
	rng := newSeededRand(seed)
	country := r.CountryCode
	if country == "" {
		// Random-country: pick from a stable-ordered list so seed
		// determinism is preserved (map iteration order isn't).
		choices := []string{"DE", "FR", "GB"}
		country = choices[rng.IntN(len(choices))]
	}
	totalLen, ok := supportedIBANCountries[country]
	if !ok {
		// Defensive — should be caught above.
		return nil, errRandomizeIBANCountry(country)
	}
	// BBAN is everything after the 4-char country+check prefix.
	bbanLen := totalLen - 4
	// Generate a digit-only BBAN. Real-world BBANs are country-
	// specific structure (digits + optional letters); a digit-only
	// BBAN is valid for all 3 shipped countries.
	bban := randDigits(rng, bbanLen)
	check := ibanCheckDigits(country, bban)
	return country + check + bban, nil
}

// Compile-time interface checks: every strategy in the wave must
// satisfy [Strategy]. The unused variable trick forces the compiler
// to verify the interface at build time; the variables are
// discarded.
var (
	_ Strategy = RandomizeSSN{}
	_ Strategy = RandomizePAN{}
	_ Strategy = RandomizeCASIN{}
	_ Strategy = RandomizeUKNIN{}
	_ Strategy = RandomizeIBAN{}
)

// ValidatePANBrand validates the operator-supplied PAN brand
// against the supported set. Used by the CLI + YAML parsers to
// fail loudly at config-load instead of at Redact time. Empty
// brand is valid (means "random brand").
func ValidatePANBrand(brand string) error {
	if brand == "" {
		return nil
	}
	if !supportedPANBrands[brand] {
		return errRandomizeBrand(brand)
	}
	return nil
}

// ValidateIBANCountry validates the operator-supplied IBAN country
// code against the supported set. Empty country is valid (means
// "random country").
func ValidateIBANCountry(country string) error {
	if country == "" {
		return nil
	}
	if _, ok := supportedIBANCountries[country]; !ok {
		return errRandomizeIBANCountry(country)
	}
	return nil
}
