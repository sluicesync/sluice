// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFunctionalPartSupported pins round-8 LOW-1's flavor branch: a
// functional key part can be defined only on vanilla MySQL, which serves the
// hidden column's collation; on PlanetScale and Vitess (and any other flavor)
// it is ir.ErrUniqueKeyDefinitionUnsupported, so the key cannot be accepted,
// never a definition that omits the collation it compares under. A key with
// no functional part is definable on every flavor.
func TestFunctionalPartSupported(t *testing.T) {
	for _, f := range []Flavor{FlavorVanilla, FlavorPlanetScale, FlavorVitess, FlavorMariaDB} {
		if err := functionalPartSupported(f, false); err != nil {
			t.Errorf("%s, no functional part: %v", f, err)
		}
		err := functionalPartSupported(f, true)
		if f == FlavorVanilla {
			if err != nil {
				t.Errorf("vanilla, functional part: %v", err)
			}
			continue
		}
		if !errors.Is(err, ir.ErrUniqueKeyDefinitionUnsupported) {
			t.Errorf("%s, functional part: %v, want ir.ErrUniqueKeyDefinitionUnsupported", f, err)
		}
	}
}
