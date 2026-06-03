// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"regexp"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// PII Phase 2.c second wave — checksum-aware randomize strategies.
//
// Each generator's tests cover the contract triad:
//
//   - same seed → same output (determinism)
//   - shape correctness (regex / length / checksum)
//   - nil seed refusal (operator-actionable error)
//
// Plus generator-specific assertions: Luhn-valid output for PAN +
// CA SIN; mod-97-valid output for IBAN; reserved-range avoidance
// for SSN; HMRC-shape compliance for UK NIN; brand / country
// parsing for the parametric generators.

// TestRandomizeSSN covers shape, determinism, reserved-range
// avoidance, and refusal-on-nil-seed for RandomizeSSN.
func TestRandomizeSSN(t *testing.T) {
	col := &ir.Column{Name: "ssn", Type: ir.Varchar{Length: 11}}
	r := RandomizeSSN{}
	ssnRe := regexp.MustCompile(`^(\d{3})-(\d{2})-(\d{4})$`)

	t.Run("Name is 'randomize:ssn'", func(t *testing.T) {
		if got := r.Name(); got != "randomize:ssn" {
			t.Errorf("Name = %q; want 'randomize:ssn'", got)
		}
	})

	t.Run("output matches XXX-XX-XXXX shape, no reserved ranges", func(t *testing.T) {
		for i := 0; i < 200; i++ {
			s := seed32("seed-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			m := ssnRe.FindStringSubmatch(str)
			if m == nil {
				t.Fatalf("seed %d: %q does not match XXX-XX-XXXX", i, str)
			}
			// Reserved-range exclusions per generator doc:
			// area != 000, area != 666, area in 001-899
			// group != 00
			// serial != 0000
			if m[1] == "000" {
				t.Errorf("seed %d: area is 000 (reserved)", i)
			}
			if m[1] == "666" {
				t.Errorf("seed %d: area is 666 (reserved per SSA)", i)
			}
			if m[1] >= "900" {
				t.Errorf("seed %d: area %s in ITIN range 900-999 (reserved)", i, m[1])
			}
			if m[2] == "00" {
				t.Errorf("seed %d: group is 00 (reserved)", i)
			}
			if m[3] == "0000" {
				t.Errorf("seed %d: serial is 0000 (reserved)", i)
			}
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		s := seed32("ssn-deterministic")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := r.Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
		if !strings.Contains(err.Error(), "randomize:ssn") {
			t.Errorf("err %q should mention the strategy", err.Error())
		}
	})
}

// TestRandomizePAN covers shape, determinism, Luhn validity, and
// brand parsing for RandomizePAN.
func TestRandomizePAN(t *testing.T) {
	col := &ir.Column{Name: "pan", Type: ir.Varchar{Length: 19}}

	t.Run("Name reflects brand", func(t *testing.T) {
		if got := (RandomizePAN{}).Name(); got != "randomize:pan" {
			t.Errorf("empty brand: Name = %q; want 'randomize:pan'", got)
		}
		if got := (RandomizePAN{Brand: "visa"}).Name(); got != "randomize:pan:visa" {
			t.Errorf("visa: Name = %q; want 'randomize:pan:visa'", got)
		}
	})

	t.Run("random-brand: Luhn-valid output, length 15 or 16", func(t *testing.T) {
		r := RandomizePAN{}
		for i := 0; i < 100; i++ {
			s := seed32("pan-rand-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			if len(str) != 15 && len(str) != 16 {
				t.Errorf("seed %d: length %d; want 15 or 16", i, len(str))
			}
			if !luhnValid(str) {
				t.Errorf("seed %d: %q is not Luhn-valid", i, str)
			}
		}
	})

	t.Run("brand visa: starts with 4, length 16, Luhn-valid", func(t *testing.T) {
		r := RandomizePAN{Brand: "visa"}
		for i := 0; i < 50; i++ {
			s := seed32("visa-" + string(rune('A'+i)))
			out, _ := r.Redact(col, nil, s)
			str := out.(string)
			if len(str) != 16 {
				t.Errorf("visa seed %d: length %d; want 16", i, len(str))
			}
			if str[0] != '4' {
				t.Errorf("visa seed %d: first digit %q; want '4'", i, str[0])
			}
			if !luhnValid(str) {
				t.Errorf("visa seed %d: %q is not Luhn-valid", i, str)
			}
		}
	})

	t.Run("brand mastercard: starts with 5, length 16, Luhn-valid", func(t *testing.T) {
		r := RandomizePAN{Brand: "mastercard"}
		for i := 0; i < 50; i++ {
			s := seed32("mc-" + string(rune('A'+i)))
			out, _ := r.Redact(col, nil, s)
			str := out.(string)
			if len(str) != 16 {
				t.Errorf("mc seed %d: length %d; want 16", i, len(str))
			}
			if str[0] != '5' {
				t.Errorf("mc seed %d: first digit %q; want '5'", i, str[0])
			}
			if !luhnValid(str) {
				t.Errorf("mc seed %d: %q is not Luhn-valid", i, str)
			}
		}
	})

	t.Run("brand amex: starts with 34 or 37, length 15, Luhn-valid", func(t *testing.T) {
		r := RandomizePAN{Brand: "amex"}
		saw34, saw37 := false, false
		for i := 0; i < 100; i++ {
			s := seed32("amex-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, _ := r.Redact(col, nil, s)
			str := out.(string)
			if len(str) != 15 {
				t.Errorf("amex seed %d: length %d; want 15", i, len(str))
			}
			prefix := str[:2]
			if prefix != "34" && prefix != "37" {
				t.Errorf("amex seed %d: prefix %q; want '34' or '37'", i, prefix)
			}
			if prefix == "34" {
				saw34 = true
			}
			if prefix == "37" {
				saw37 = true
			}
			if !luhnValid(str) {
				t.Errorf("amex seed %d: %q is not Luhn-valid", i, str)
			}
		}
		if !saw34 || !saw37 {
			t.Errorf("amex distribution: saw34=%v saw37=%v; want both", saw34, saw37)
		}
	})

	t.Run("deterministic per brand", func(t *testing.T) {
		r := RandomizePAN{Brand: "visa"}
		s := seed32("pan-determinism")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := (RandomizePAN{}).Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
	})

	t.Run("unsupported brand refused at Redact time (defense-in-depth)", func(t *testing.T) {
		_, err := RandomizePAN{Brand: "discover"}.Redact(col, nil, seed32("seed"))
		if err == nil {
			t.Fatal("expected error for unknown brand")
		}
		if !strings.Contains(err.Error(), "discover") {
			t.Errorf("err %q should name the unknown brand", err.Error())
		}
	})

	t.Run("ValidatePANBrand: empty ok, unknown refused, known ok", func(t *testing.T) {
		if err := ValidatePANBrand(""); err != nil {
			t.Errorf("empty brand: unexpected error %v", err)
		}
		for _, b := range []string{"visa", "mastercard", "amex"} {
			if err := ValidatePANBrand(b); err != nil {
				t.Errorf("brand %q: unexpected error %v", b, err)
			}
		}
		if err := ValidatePANBrand("jcb"); err == nil {
			t.Error("expected error for jcb brand")
		}
	})
}

// TestRandomizeCASIN covers shape, Luhn validity, determinism, and
// first-digit restrictions for RandomizeCASIN.
func TestRandomizeCASIN(t *testing.T) {
	col := &ir.Column{Name: "sin", Type: ir.Varchar{Length: 11}}
	r := RandomizeCASIN{}
	sinRe := regexp.MustCompile(`^(\d)(\d{2})-(\d{3})-(\d{3})$`)

	t.Run("Name is 'randomize:ca-sin'", func(t *testing.T) {
		if got := r.Name(); got != "randomize:ca-sin" {
			t.Errorf("Name = %q", got)
		}
	})

	t.Run("output matches XXX-XXX-XXX, Luhn-valid, first digit valid", func(t *testing.T) {
		for i := 0; i < 200; i++ {
			s := seed32("sin-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			m := sinRe.FindStringSubmatch(str)
			if m == nil {
				t.Fatalf("seed %d: %q does not match XXX-XXX-XXX", i, str)
			}
			// First digit must NOT be 0 or 8 (reserved).
			if m[1] == "0" {
				t.Errorf("seed %d: first digit is 0 (reserved)", i)
			}
			if m[1] == "8" {
				t.Errorf("seed %d: first digit is 8 (reserved)", i)
			}
			if !luhnValid(str) {
				t.Errorf("seed %d: %q is not Luhn-valid", i, str)
			}
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		s := seed32("sin-determinism")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := r.Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
	})
}

// TestRandomizeUKNIN covers shape, determinism, and prefix/suffix
// letter alphabet compliance for RandomizeUKNIN.
func TestRandomizeUKNIN(t *testing.T) {
	col := &ir.Column{Name: "nin", Type: ir.Varchar{Length: 9}}
	r := RandomizeUKNIN{}
	ninRe := regexp.MustCompile(`^([A-Z]{2})(\d{6})([A-Z])$`)

	t.Run("Name is 'randomize:uk-nin'", func(t *testing.T) {
		if got := r.Name(); got != "randomize:uk-nin" {
			t.Errorf("Name = %q", got)
		}
	})

	t.Run("output matches AA999999A shape, suffix in {A,B,C,D}", func(t *testing.T) {
		for i := 0; i < 200; i++ {
			s := seed32("nin-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			m := ninRe.FindStringSubmatch(str)
			if m == nil {
				t.Fatalf("seed %d: %q does not match AA999999A", i, str)
			}
			// Suffix must be one of A/B/C/D per HMRC convention.
			switch m[3] {
			case "A", "B", "C", "D":
				// ok
			default:
				t.Errorf("seed %d: suffix %q not in {A,B,C,D}", i, m[3])
			}
			// Prefix letters must not be in HMRC-reserved set (D,F,I,Q,U,V).
			for j := 0; j < 2; j++ {
				c := m[1][j]
				switch c {
				case 'D', 'F', 'I', 'Q', 'U', 'V':
					t.Errorf("seed %d: prefix position %d is %q (HMRC-reserved)", i, j, c)
				}
			}
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		s := seed32("nin-determinism")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := r.Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
	})
}

// TestRandomizeIBAN covers shape, mod-97 validity, determinism, and
// country-code parsing for RandomizeIBAN.
func TestRandomizeIBAN(t *testing.T) {
	col := &ir.Column{Name: "iban", Type: ir.Varchar{Length: 34}}

	t.Run("Name reflects country", func(t *testing.T) {
		if got := (RandomizeIBAN{}).Name(); got != "randomize:iban" {
			t.Errorf("empty country: Name = %q", got)
		}
		if got := (RandomizeIBAN{CountryCode: "DE"}).Name(); got != "randomize:iban:DE" {
			t.Errorf("DE country: Name = %q", got)
		}
	})

	t.Run("random-country: 22 or 27 chars, mod-97-valid", func(t *testing.T) {
		r := RandomizeIBAN{}
		for i := 0; i < 100; i++ {
			s := seed32("iban-rand-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			if len(str) != 22 && len(str) != 27 {
				t.Errorf("seed %d: length %d; want 22 or 27", i, len(str))
			}
			country := str[:2]
			if country != "DE" && country != "GB" && country != "FR" {
				t.Errorf("seed %d: country %q; want DE/GB/FR", i, country)
			}
			if !ibanValid(str) {
				t.Errorf("seed %d: %q failed mod-97 check", i, str)
			}
		}
	})

	t.Run("country DE: 22 chars, starts with DE, mod-97-valid", func(t *testing.T) {
		r := RandomizeIBAN{CountryCode: "DE"}
		for i := 0; i < 50; i++ {
			s := seed32("de-" + string(rune('A'+i)))
			out, _ := r.Redact(col, nil, s)
			str := out.(string)
			if len(str) != 22 {
				t.Errorf("DE seed %d: length %d; want 22", i, len(str))
			}
			if str[:2] != "DE" {
				t.Errorf("DE seed %d: country %q", i, str[:2])
			}
			if !ibanValid(str) {
				t.Errorf("DE seed %d: %q failed mod-97 check", i, str)
			}
		}
	})

	t.Run("country GB: 22 chars, starts with GB, mod-97-valid", func(t *testing.T) {
		r := RandomizeIBAN{CountryCode: "GB"}
		for i := 0; i < 50; i++ {
			s := seed32("gb-" + string(rune('A'+i)))
			out, _ := r.Redact(col, nil, s)
			str := out.(string)
			if len(str) != 22 {
				t.Errorf("GB seed %d: length %d; want 22", i, len(str))
			}
			if str[:2] != "GB" {
				t.Errorf("GB seed %d: country %q", i, str[:2])
			}
			if !ibanValid(str) {
				t.Errorf("GB seed %d: %q failed mod-97 check", i, str)
			}
		}
	})

	t.Run("country FR: 27 chars, starts with FR, mod-97-valid", func(t *testing.T) {
		r := RandomizeIBAN{CountryCode: "FR"}
		for i := 0; i < 50; i++ {
			s := seed32("fr-" + string(rune('A'+i)))
			out, _ := r.Redact(col, nil, s)
			str := out.(string)
			if len(str) != 27 {
				t.Errorf("FR seed %d: length %d; want 27", i, len(str))
			}
			if str[:2] != "FR" {
				t.Errorf("FR seed %d: country %q", i, str[:2])
			}
			if !ibanValid(str) {
				t.Errorf("FR seed %d: %q failed mod-97 check", i, str)
			}
		}
	})

	t.Run("deterministic per country", func(t *testing.T) {
		r := RandomizeIBAN{CountryCode: "DE"}
		s := seed32("iban-determinism")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := (RandomizeIBAN{}).Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
	})

	t.Run("unsupported country refused at Redact (defense-in-depth)", func(t *testing.T) {
		_, err := RandomizeIBAN{CountryCode: "ZZ"}.Redact(col, nil, seed32("seed"))
		if err == nil {
			t.Fatal("expected error for unknown country")
		}
		if !strings.Contains(err.Error(), "ZZ") {
			t.Errorf("err %q should name the unknown country", err.Error())
		}
	})

	t.Run("ValidateIBANCountry: empty ok, unknown refused, known ok", func(t *testing.T) {
		if err := ValidateIBANCountry(""); err != nil {
			t.Errorf("empty country: unexpected error %v", err)
		}
		for _, c := range []string{"DE", "GB", "FR"} {
			if err := ValidateIBANCountry(c); err != nil {
				t.Errorf("country %q: unexpected error %v", c, err)
			}
		}
		if err := ValidateIBANCountry("US"); err == nil {
			t.Error("expected error for US country code")
		}
	})
}

// TestIBANCheckDigits pins the mod-97 helper against known-good
// IBANs from the SWIFT registry. If the algorithm ever drifts,
// this test catches it before any randomize:iban output ships.
func TestIBANCheckDigits(t *testing.T) {
	// Known IBANs from public references (SWIFT registry / ECBS).
	// Each entry: country, full IBAN, BBAN portion (chars 4 onwards).
	cases := []struct {
		name    string
		country string
		bban    string
		want    string // the 2-digit check the helper should compute
	}{
		{
			name:    "DE89370400440532013000",
			country: "DE",
			bban:    "370400440532013000",
			want:    "89",
		},
		{
			name:    "GB82WEST12345698765432",
			country: "GB",
			bban:    "WEST12345698765432",
			want:    "82",
		},
		{
			name:    "FR1420041010050500013M02606",
			country: "FR",
			bban:    "20041010050500013M02606",
			want:    "14",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := ibanCheckDigits(c.country, c.bban)
			if got != c.want {
				t.Errorf("ibanCheckDigits(%q, %q) = %q; want %q", c.country, c.bban, got, c.want)
			}
			// Self-consistency: the full IBAN must pass ibanValid.
			full := c.country + got + c.bban
			if !ibanValid(full) {
				t.Errorf("constructed IBAN %q does not satisfy ibanValid", full)
			}
		})
	}
}

// TestIBANValid covers the validator helper used in the strategy
// tests. Negative cases ensure it doesn't false-positive on
// malformed input.
func TestIBANValid(t *testing.T) {
	t.Run("valid IBANs pass", func(t *testing.T) {
		for _, s := range []string{
			"DE89370400440532013000",
			"GB82WEST12345698765432",
			"FR1420041010050500013M02606",
			"DE89 3704 0044 0532 0130 00", // with spaces
			"de89370400440532013000",      // lowercase
		} {
			if !ibanValid(s) {
				t.Errorf("expected valid: %q", s)
			}
		}
	})

	t.Run("invalid IBANs refused", func(t *testing.T) {
		for _, s := range []string{
			"",                        // empty
			"DE",                      // too short
			strings.Repeat("D", 35),   // too long
			"DE00370400440532013000",  // wrong check digits
			"DE89370400440532013!!!",  // non-alphanumeric
			"DE89_370400440532013000", // bad char
		} {
			if ibanValid(s) {
				t.Errorf("expected invalid: %q", s)
			}
		}
	})
}

// TestApplyRow_SecondWaveRandomize covers the integration of the
// new strategies with Registry.ApplyRow: PK supplied → replay-stable
// output; nil PK → refusal mentioning the strategy.
func TestApplyRow_SecondWaveRandomize(t *testing.T) {
	cases := []struct {
		name     string
		col      string
		strategy Strategy
		wantName string
	}{
		{"ssn", "ssn", RandomizeSSN{}, "randomize:ssn"},
		{"pan", "pan", RandomizePAN{}, "randomize:pan"},
		{"pan:visa", "pan", RandomizePAN{Brand: "visa"}, "randomize:pan:visa"},
		{"ca-sin", "sin", RandomizeCASIN{}, "randomize:ca-sin"},
		{"uk-nin", "nin", RandomizeUKNIN{}, "randomize:uk-nin"},
		{"iban", "iban", RandomizeIBAN{}, "randomize:iban"},
		{"iban:DE", "iban", RandomizeIBAN{CountryCode: "DE"}, "randomize:iban:DE"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name+" no-PK refuses", func(t *testing.T) {
			r := New()
			r.Set("public", "t", c.col, c.strategy)
			row := ir.Row{"id": int64(1), c.col: "x"}
			err := r.ApplyRow("public", "t", nil, row, "s")
			if err == nil {
				t.Fatal("expected refusal")
			}
			if !strings.Contains(err.Error(), "primary key") {
				t.Errorf("err %q should mention primary key", err.Error())
			}
			if !strings.Contains(err.Error(), c.wantName) {
				t.Errorf("err %q should name the strategy %q", err.Error(), c.wantName)
			}
		})
		t.Run(c.name+" with-PK replay-stable", func(t *testing.T) {
			r := New()
			r.Set("public", "t", c.col, c.strategy)
			row1 := ir.Row{"id": int64(1), c.col: "x"}
			if err := r.ApplyRow("public", "t", []string{"id"}, row1, "s"); err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			first := row1[c.col]
			row2 := ir.Row{"id": int64(1), c.col: "different-input"}
			if err := r.ApplyRow("public", "t", []string{"id"}, row2, "s"); err != nil {
				t.Fatalf("replay error %v", err)
			}
			if row2[c.col] != first {
				t.Errorf("replay stability broken: %v != %v", row2[c.col], first)
			}
		})
	}
}
