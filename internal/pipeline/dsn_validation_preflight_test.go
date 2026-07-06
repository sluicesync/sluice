// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// dsnValidatorEngine is a fake ir.Engine (via embedded stubEngine) that
// also implements ir.DSNValidator, refusing any DSN equal to badDSN.
type dsnValidatorEngine struct {
	stubEngine
	badDSN string
}

func (e dsnValidatorEngine) ValidateDSN(dsn string) error {
	if dsn == e.badDSN {
		return errors.New("host is a PlanetScale endpoint")
	}
	return nil
}

// TestPreflightDSNValidation pins the driver/host mismatch preflight:
// an engine implementing ir.DSNValidator that refuses its DSN yields a
// CodedError with the DriverHostMismatch code and the correct role
// prefix; an engine that doesn't implement the surface is a no-op; and
// the source is checked before the target.
func TestPreflightDSNValidation(t *testing.T) {
	t.Run("source refusal is coded + role-prefixed", func(t *testing.T) {
		src := dsnValidatorEngine{badDSN: "bad-src"}
		err := preflightDSNValidation(src, "bad-src", stubEngine{}, "ok-dst")
		ce := assertDriverHostMismatch(t, err)
		if ce.Hint != "pass --source-driver planetscale" {
			t.Errorf("Hint = %q; want the source-driver hint", ce.Hint)
		}
		if got := ce.Error(); got[:len("source: ")] != "source: " {
			t.Errorf("message = %q; want a %q role prefix", got, "source: ")
		}
	})

	t.Run("target refusal is coded + role-prefixed", func(t *testing.T) {
		dst := dsnValidatorEngine{badDSN: "bad-dst"}
		err := preflightDSNValidation(stubEngine{}, "ok-src", dst, "bad-dst")
		ce := assertDriverHostMismatch(t, err)
		if ce.Hint != "pass --target-driver planetscale" {
			t.Errorf("Hint = %q; want the target-driver hint", ce.Hint)
		}
		if got := ce.Error(); got[:len("target: ")] != "target: " {
			t.Errorf("message = %q; want a %q role prefix", got, "target: ")
		}
	})

	t.Run("engine without the surface is a no-op", func(t *testing.T) {
		if err := preflightDSNValidation(stubEngine{}, "any", stubEngine{}, "any"); err != nil {
			t.Errorf("preflightDSNValidation with no validators = %v; want nil", err)
		}
	})

	t.Run("both valid is nil", func(t *testing.T) {
		src := dsnValidatorEngine{badDSN: "bad-src"}
		dst := dsnValidatorEngine{badDSN: "bad-dst"}
		if err := preflightDSNValidation(src, "ok-src", dst, "ok-dst"); err != nil {
			t.Errorf("preflightDSNValidation with valid DSNs = %v; want nil", err)
		}
	})

	t.Run("source is checked before target", func(t *testing.T) {
		// Both sides refuse; the source's refusal must win so the
		// diagnostic is deterministic.
		src := dsnValidatorEngine{badDSN: "bad"}
		dst := dsnValidatorEngine{badDSN: "bad"}
		err := preflightDSNValidation(src, "bad", dst, "bad")
		ce := assertDriverHostMismatch(t, err)
		if ce.Hint != "pass --source-driver planetscale" {
			t.Errorf("Hint = %q; want the source side to win", ce.Hint)
		}
	})
}

func assertDriverHostMismatch(t *testing.T, err error) *sluicecode.CodedError {
	t.Helper()
	if err == nil {
		t.Fatal("preflightDSNValidation = nil; want a refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a CodedError", err)
	}
	if ce.Code != sluicecode.CodeDriverHostMismatch {
		t.Errorf("Code = %q; want %q", ce.Code, sluicecode.CodeDriverHostMismatch)
	}
	return ce
}

// Compile-time assurance the fake actually satisfies the surface.
var _ ir.DSNValidator = dsnValidatorEngine{}
