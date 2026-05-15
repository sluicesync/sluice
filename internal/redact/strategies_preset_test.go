// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestMaskSSN covers the happy path + every refusal branch.
func TestMaskSSN(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		got, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, "123-45-6789", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "XXX-XX-6789" {
			t.Errorf("got %q; want XXX-XX-6789", got)
		}
	})
	t.Run("zeros preserved at tail", func(t *testing.T) {
		got, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, "001-02-0003", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "XXX-XX-0003" {
			t.Errorf("got %q; want XXX-XX-0003", got)
		}
	})
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, nil, nil)
		if err != nil || got != nil {
			t.Errorf("nil: got %v err %v; want nil nil", got, err)
		}
	})
	t.Run("Name()", func(t *testing.T) {
		if (MaskSSN{}).Name() != "mask:ssn" {
			t.Errorf("Name() = %q", (MaskSSN{}).Name())
		}
	})
	refusals := []struct {
		name   string
		input  any
		errSub string
	}{
		{"non-string", 123456789, "unsupported type"},
		{"wrong length short", "12-34-5678", "got 10 chars"},
		{"wrong length long", "123-45-67890", "got 12 chars"},
		{"no dashes", "123456789AB", "SSN format"},
		{"wrong dash positions", "12-345-6789", "SSN format"},
		{"non-digit in body", "12A-45-6789", "non-digit"},
		{"non-digit at end", "123-45-678X", "non-digit"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil for input %v", c.input)
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}
}

// TestMaskPAN covers strict PAN masking (Luhn-checked).
func TestMaskPAN(t *testing.T) {
	cases := []struct {
		name, input, want string
	}{
		{"Visa 16", "4111111111111111", "411111XXXXXX1111"},
		{"Mastercard 16", "5555555555554444", "555555XXXXXX4444"},
		{"AmEx 15", "378282246310005", "378282XXXXX0005"},
		{"Discover 16", "6011111111111117", "601111XXXXXX1117"},
		{"with hyphens (preserved)", "4111-1111-1111-1111", "4111-11XX-XXXX-1111"},
		{"with spaces (preserved)", "4111 1111 1111 1111", "4111 11XX XXXX 1111"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, c.input, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, nil, nil)
		if err != nil || got != nil {
			t.Errorf("nil: got %v err %v; want nil nil", got, err)
		}
	})
	t.Run("Name()", func(t *testing.T) {
		if (MaskPAN{}).Name() != "mask:pan" {
			t.Errorf("Name() = %q", (MaskPAN{}).Name())
		}
	})

	refusals := []struct {
		name, input, errSub string
	}{
		{"too few digits", "411111111111", "fails Luhn"}, // 12 digits but Luhn-invalid
		{"only 11 digits", "41111111111", "12-19 digits"},
		{"too many digits", "12345678901234567890", "12-19 digits"},
		{"bad Luhn 16", "4111111111111112", "fails Luhn"},
		{"empty", "", "12-19 digits"},
		{"all letters", "ABCDEFGHIJKLMNOP", "12-19 digits"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}

	t.Run("non-string refused", func(t *testing.T) {
		_, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, int64(4111111111111111), nil)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Errorf("expected unsupported-type error; got %v", err)
		}
	})
}

// TestMaskPANRelaxed covers lenient PAN masking (no Luhn check).
func TestMaskPANRelaxed(t *testing.T) {
	t.Run("Luhn-invalid accepted", func(t *testing.T) {
		got, err := MaskPANRelaxed{}.Redact(&ir.Column{Name: "pan"}, "4111111111111112", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "411111XXXXXX1112" {
			t.Errorf("got %q; want 411111XXXXXX1112", got)
		}
	})
	t.Run("Luhn-valid still works", func(t *testing.T) {
		got, err := MaskPANRelaxed{}.Redact(&ir.Column{Name: "pan"}, "4111111111111111", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "411111XXXXXX1111" {
			t.Errorf("got %q; want 411111XXXXXX1111", got)
		}
	})
	t.Run("Name()", func(t *testing.T) {
		if (MaskPANRelaxed{}).Name() != "mask:pan-relaxed" {
			t.Errorf("Name() = %q", (MaskPANRelaxed{}).Name())
		}
	})
	t.Run("digit-count check still applies", func(t *testing.T) {
		_, err := MaskPANRelaxed{}.Redact(&ir.Column{Name: "pan"}, "41111111111", nil) // 11 digits
		if err == nil || !strings.Contains(err.Error(), "12-19 digits") {
			t.Errorf("expected digit-count refusal; got %v", err)
		}
	})
}

// TestMaskEmail covers the sluice-native email preset.
func TestMaskEmail(t *testing.T) {
	cases := []struct {
		name, input, want string
	}{
		{"standard", "alice@example.com", "aXXXX@example.com"},
		{"long local", "support+billing@example.com", "sXXXXXXXXXXXXXX@example.com"},
		{"single-char local (no-op on local)", "a@x.test", "a@x.test"},
		{"long domain preserved", "alice@subdomain.long.example.co.uk", "aXXXX@subdomain.long.example.co.uk"},
		{"UTF-8 local part (rune-counted)", "alíce@example.com", "aXXXX@example.com"},
		{"quoted local part — last @ is the separator", `"a@b"@example.com`, `"XXXX@example.com`},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, c.input, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, nil, nil)
		if err != nil || got != nil {
			t.Errorf("nil: got %v err %v; want nil nil", got, err)
		}
	})
	t.Run("Name()", func(t *testing.T) {
		if (MaskEmail{}).Name() != "mask:email" {
			t.Errorf("Name() = %q", (MaskEmail{}).Name())
		}
	})

	refusals := []struct {
		name, input, errSub string
	}{
		{"no @", "alice-example.com", "no '@'"},
		{"empty local", "@example.com", "empty local part"},
		{"empty string", "", "no '@'"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}

	t.Run("non-string refused", func(t *testing.T) {
		_, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, 42, nil)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Errorf("expected unsupported-type error; got %v", err)
		}
	})
}

// TestMaskCASIN covers the Canadian SIN preset.
func TestMaskCASIN(t *testing.T) {
	// 046-454-286 / 046454286: digits 0,4,6,4,5,4,2,8,6 — Luhn-valid (sum=50).
	cases := []struct {
		name, input, want string
	}{
		{"dashed valid", "046-454-286", "XXX-XXX-286"},
		{"undashed valid", "046454286", "XXXXXX286"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := MaskCASIN{}.Redact(&ir.Column{Name: "sin"}, c.input, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskCASIN{}.Redact(&ir.Column{Name: "sin"}, nil, nil)
		if err != nil || got != nil {
			t.Errorf("nil: got %v err %v", got, err)
		}
	})
	t.Run("Name()", func(t *testing.T) {
		if (MaskCASIN{}).Name() != "mask:ca-sin" {
			t.Errorf("Name() = %q", (MaskCASIN{}).Name())
		}
	})

	refusals := []struct {
		name, input, errSub string
	}{
		{"bad Luhn dashed", "046-454-287", "fails Luhn"},
		{"bad Luhn undashed", "046454287", "fails Luhn"},
		{"wrong length 8", "04645428", "SIN format"},
		{"wrong length 10", "0464542867", "SIN format"},
		{"wrong dashes", "046-4542-86", "SIN format"},
		{"non-digit dashed", "ABC-DEF-GHI", "non-digit"},
		{"non-digit undashed", "ABCDEFGHI", "non-digit"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskCASIN{}.Redact(&ir.Column{Name: "sin"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}
}

// TestMaskUKNIN covers the UK NIN preset.
func TestMaskUKNIN(t *testing.T) {
	cases := []struct {
		name, input, want string
	}{
		{"AB123456C", "AB123456C", "ABXXXXXXC"},
		{"WP123456A", "WP123456A", "WPXXXXXXA"},
		{"QQ999999D", "QQ999999D", "QQXXXXXXD"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := MaskUKNIN{}.Redact(&ir.Column{Name: "nin"}, c.input, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("Name()", func(t *testing.T) {
		if (MaskUKNIN{}).Name() != "mask:uk-nin" {
			t.Errorf("Name() = %q", (MaskUKNIN{}).Name())
		}
	})
	refusals := []struct {
		name, input, errSub string
	}{
		{"too short", "AB12345C", "9 chars"},
		{"too long", "AB1234567C", "9 chars"},
		{"lowercase prefix", "ab123456C", "uppercase letter"},
		{"digit in prefix", "1B123456C", "uppercase letter"},
		{"letter in digit body", "ABA23456C", "not a digit"},
		{"bad suffix Z", "AB123456Z", "A/B/C/D"},
		{"lowercase suffix", "AB123456c", "A/B/C/D"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskUKNIN{}.Redact(&ir.Column{Name: "nin"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}
}

// TestMaskIBAN covers the IBAN preset.
func TestMaskIBAN(t *testing.T) {
	cases := []struct {
		name, input, want string
	}{
		{"German 22", "DE89370400440532013000", "DE8937XXXXXXXXXXXX3000"},
		{"UK 22", "GB82WEST12345698765432", "GB82WEXXXXXXXXXXXX5432"},
		{"Norwegian 15 (shortest)", "NO9386011117947", "NO9386XXXXX7947"},
		{"with spaces (re-inserted)", "DE89 3704 0044 0532 0130 00", "DE89 37XX XXXX XXXX XX30 00"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := MaskIBAN{}.Redact(&ir.Column{Name: "iban"}, c.input, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("Name()", func(t *testing.T) {
		if (MaskIBAN{}).Name() != "mask:iban" {
			t.Errorf("Name() = %q", (MaskIBAN{}).Name())
		}
	})
	refusals := []struct {
		name, input, errSub string
	}{
		{"too short", "DE891234567", "15-34"},
		{"too long", strings.Repeat("X", 35), "15-34"},
		{"no country code", "1289370400440532013000", "country-code"},
		{"lowercase country", "de89370400440532013000", "country-code"},
		{"non-digit check", "DEAA370400440532013000", "check digits"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskIBAN{}.Redact(&ir.Column{Name: "iban"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}
}

// TestMaskUUID covers the UUID preset.
func TestMaskUUID(t *testing.T) {
	cases := []struct {
		name, input, want string
	}{
		{"v4 lowercase", "550e8400-e29b-41d4-a716-446655440000", "550e----0000"},
		{"v1 lowercase", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "f47a----d479"},
		{"uppercase preserved", "550E8400-E29B-41D4-A716-446655440000", "550E----0000"},
		{"mixed case preserved", "550e8400-E29b-41D4-a716-446655440000", "550e----0000"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := MaskUUID{}.Redact(&ir.Column{Name: "id"}, c.input, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Reconstruct the expected output: first 4 from input, then
			// for each hyphen-group: the 4 hyphens + masked Xs +
			// preserved last 4 from input.
			// The test's `want` is a compact form; build the full expected.
			expected := c.input[:4] + "XXXX-XXXX-XXXX-XXXX-XXXXXXXX" + c.input[32:]
			if got != expected {
				t.Errorf("got %q; want %q", got, expected)
			}
			_ = c.want // referenced only to document the compact shape
		})
	}
	t.Run("Name()", func(t *testing.T) {
		if (MaskUUID{}).Name() != "mask:uuid" {
			t.Errorf("Name() = %q", (MaskUUID{}).Name())
		}
	})
	refusals := []struct {
		name, input, errSub string
	}{
		{"too short", "550e8400-e29b-41d4-a716-44665544000", "36 chars"},
		{"too long", "550e8400-e29b-41d4-a716-4466554400000", "36 chars"},
		{"missing hyphen", "550e8400 e29b-41d4-a716-446655440000", "missing hyphen"},
		{"wrong hyphen position", "550e840-0e29b-41d4-a716-446655440000", "missing hyphen"},
		{"non-hex char", "550e8400-e29b-41d4-a716-446G55440000", "not a hex"},
	}
	for _, c := range refusals {
		c := c
		t.Run("refuse "+c.name, func(t *testing.T) {
			_, err := MaskUUID{}.Redact(&ir.Column{Name: "id"}, c.input, nil)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}
}
