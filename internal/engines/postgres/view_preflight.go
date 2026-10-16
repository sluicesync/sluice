// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

var _ ir.ViewEmitPreflighter = Engine{}

// PreflightViews refuses, before any data moves, every view a Postgres target
// must not create (Bug 314): one the reader could not make portable, and one
// whose body is in MySQL's dialect (always backquoted, so its CREATE could
// never succeed here, and the refusal now arrives before the copy rather than
// after it). CreateViews applies the same check as a belt. Postgres needs no
// view-namespace preflight: CREATE OR REPLACE VIEW raises loudly on a name a
// table holds (see [ir.ViewEmitPreflighter]).
func (Engine) PreflightViews(s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: PreflightViews: schema is nil")
	}
	if err := translate.RefuseUnportableViews(s, dialectName); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}
