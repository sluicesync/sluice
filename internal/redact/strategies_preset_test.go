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
		got, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, "123-45-6789")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "XXX-XX-6789" {
			t.Errorf("got %q; want XXX-XX-6789", got)
		}
	})
	t.Run("zeros preserved at tail", func(t *testing.T) {
		got, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, "001-02-0003")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "XXX-XX-0003" {
			t.Errorf("got %q; want XXX-XX-0003", got)
		}
	})
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, nil)
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
			_, err := MaskSSN{}.Redact(&ir.Column{Name: "ssn"}, c.input)
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
			got, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, c.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, nil)
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
			_, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, c.input)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}

	t.Run("non-string refused", func(t *testing.T) {
		_, err := MaskPAN{}.Redact(&ir.Column{Name: "pan"}, int64(4111111111111111))
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Errorf("expected unsupported-type error; got %v", err)
		}
	})
}

// TestMaskPANRelaxed covers lenient PAN masking (no Luhn check).
func TestMaskPANRelaxed(t *testing.T) {
	t.Run("Luhn-invalid accepted", func(t *testing.T) {
		got, err := MaskPANRelaxed{}.Redact(&ir.Column{Name: "pan"}, "4111111111111112")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "411111XXXXXX1112" {
			t.Errorf("got %q; want 411111XXXXXX1112", got)
		}
	})
	t.Run("Luhn-valid still works", func(t *testing.T) {
		got, err := MaskPANRelaxed{}.Redact(&ir.Column{Name: "pan"}, "4111111111111111")
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
		_, err := MaskPANRelaxed{}.Redact(&ir.Column{Name: "pan"}, "41111111111") // 11 digits
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
			got, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, c.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
	t.Run("nil passthrough", func(t *testing.T) {
		got, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, nil)
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
			_, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, c.input)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), c.errSub)
			}
		})
	}

	t.Run("non-string refused", func(t *testing.T) {
		_, err := MaskEmail{}.Redact(&ir.Column{Name: "email"}, 42)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Errorf("expected unsupported-type error; got %v", err)
		}
	})
}
