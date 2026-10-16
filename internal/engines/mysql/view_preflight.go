// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

var _ ir.ViewEmitPreflighter = Engine{}

// PreflightViews refuses, before any data moves, every view a MySQL target
// must not create (Bug 314): one the reader could not make portable, and one
// whose body crossed from a Postgres schema other than public, which named
// that schema until the qualifier was removed and would now create and
// compute differently. CreateViews applies the same check as a belt. MySQL
// needs no view-namespace preflight: CREATE OR REPLACE VIEW raises loudly on
// a name a table holds (see [ir.ViewEmitPreflighter]).
func (Engine) PreflightViews(s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: PreflightViews: schema is nil")
	}
	if err := translate.RefuseUnportableViews(s, dialectName); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}
	return nil
}
