// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestDecimalDigitsBeyondScale grades the digit count against hand-derived
// values: significant fractional digits past the scale, trailing zeros
// ignored, exponent forms honoured, non-finite text left to the server.
func TestDecimalDigitsBeyondScale(t *testing.T) {
	cases := []struct {
		s     string
		scale int
		want  int
	}{
		{"0.1234567890123456789012345678901", 30, 1},
		{"0.123456789012345678901234567890", 30, 0},
		{"1.50000000000000000000000000000000000", 30, 0},
		{"-0.99999999999999999999999999999999", 30, 2},
		{"12345678901234567890123456789012345.1", 30, 0},
		{"123456789012345678901234567890123456", 30, 0},
		{"3.14159265358979323846264338327950288419716939937510", 30, 19},
		{"0", 30, 0},
		{"-0.000000000000000000000000000001", 30, 0},
		{"0.0000000000000000000000000000001", 30, 1},
		{"+1.25", 1, 1},
		{"1.25", 2, 0},
		{"100", 0, 0},
		{"1.5e-40", 30, 11},
		{"15e-31", 30, 1},
		{"1000e-3", 0, 0},
		{"1.23e2", 0, 0},
		{"1.234e2", 0, 1},
		{"NaN", 30, 0},
		{"Infinity", 30, 0},
		{"-Infinity", 30, 0},
		{"", 30, 0},
		{"1.2.3", 0, 0},
		{"1e", 0, 0},
	}
	for _, c := range cases {
		if got := decimalDigitsBeyondScale(c.s, c.scale); got != c.want {
			t.Errorf("decimalDigitsBeyondScale(%q, %d) = %d, want %d", c.s, c.scale, got, c.want)
		}
	}
}

// TestPrepareValue_RefusesDecimalScaleLoss pins the value guard on the one
// funnel every MySQL write lane uses: the unconstrained mapping (scale 30),
// a constrained DECIMAL, a DOMAIN over DECIMAL, and the controls that must
// pass (at-scale, trailing zeros, non-decimal columns, a nil descriptor).
func TestPrepareValue_RefusesDecimalScaleLoss(t *testing.T) {
	over := "0.1234567890123456789012345678901"
	refuse := []struct {
		name string
		col  *ir.Column
		v    any
	}{
		{"unconstrained", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, over},
		{"constrained", &ir.Column{Name: "v", Type: ir.Decimal{Precision: 10, Scale: 2}}, "1.234"},
		{"domain", &ir.Column{Name: "v", Type: ir.Domain{Name: "money", BaseType: ir.Decimal{Unconstrained: true}}}, over},
		{"rounds-up-to-integer", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, "-0.99999999999999999999999999999999"},
	}
	for _, c := range refuse {
		_, err := prepareValue(c.v, c.col)
		if err == nil {
			t.Errorf("%s: %v was accepted; the target would round it at exit 0", c.name, c.v)
			continue
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeValueUnrepresentable {
			t.Errorf("%s: refusal is not coded %s: %v", c.name, sluicecode.CodeValueUnrepresentable, err)
		}
		if !strings.Contains(err.Error(), decimalScaleExceededMarker) || !strings.Contains(err.Error(), `"v"`) {
			t.Errorf("%s: refusal does not name the marker and column: %v", c.name, err)
		}
	}
	pass := []struct {
		name string
		col  *ir.Column
		v    any
	}{
		{"at-scale", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, "0.123456789012345678901234567890"},
		{"trailing-zeros", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, "1.50000000000000000000000000000000000"},
		{"integer-overflow-left-to-server", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, "123456789012345678901234567890123456"},
		{"nan-left-to-server", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, "NaN"},
		{"text-column", &ir.Column{Name: "v", Type: ir.Text{}}, over},
		{"nil-descriptor", nil, over},
		{"null", &ir.Column{Name: "v", Type: ir.Decimal{Unconstrained: true}}, nil},
	}
	for _, c := range pass {
		if _, err := prepareValue(c.v, c.col); err != nil {
			t.Errorf("%s: %v was refused, want accepted: %v", c.name, c.v, err)
		}
	}
}

// isCoded reports whether err carries code.
func isCoded(err error, code sluicecode.Code) bool {
	ce, ok := sluicecode.FromError(err)
	return ok && ce.Code == code
}

// TestEmitColumnDef_RefusesDecimalDefaultScaleLoss pins the DEFAULT guard on
// the emit every CREATE TABLE and forwarded ADD COLUMN goes through.
func TestEmitColumnDef_RefusesDecimalDefaultScaleLoss(t *testing.T) {
	col := &ir.Column{
		Name: "v", Type: ir.Decimal{Unconstrained: true}, Nullable: true,
		Default: ir.DefaultLiteral{Value: "0.1234567890123456789012345678901"},
	}
	_, err := stdEmitter.emitColumnDef("t", col)
	if err == nil {
		t.Fatal("an over-scale DECIMAL DEFAULT was emitted; the target would store it rounded")
	}
	if !strings.Contains(err.Error(), decimalScaleExceededMarker) || !isCoded(err, sluicecode.CodeValueUnrepresentable) {
		t.Errorf("refusal = %v; want %s with %s", err, decimalScaleExceededMarker, sluicecode.CodeValueUnrepresentable)
	}
	for _, ok := range []string{"0.123456789012345678901234567890", "1.5000000000000000000000000000000000"} {
		col.Default = ir.DefaultLiteral{Value: ok}
		got, err := stdEmitter.emitColumnDef("t", col)
		if err != nil {
			t.Errorf("DEFAULT %s refused: %v", ok, err)
			continue
		}
		if !strings.Contains(got, "DEFAULT '"+ok+"'") {
			t.Errorf("DEFAULT %s emitted as %q", ok, got)
		}
	}
}
