// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"path/filepath"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// SourceIdentity implements [ir.SourceIdentityDescriber]: the FILE the
// DSN names is the dataset, for all three formats (`csv`, `tsv`,
// `ndjson` — one type, three registrations).
//
// The format itself needs no place here: it is part of the ENGINE NAME,
// which the identity already carries, so `./data.csv` read as `csv` and
// the same bytes read as `tsv` are two identities without this field
// saying so.
//
// No I/O, per the describer's contract: the path is normalised and
// returned. A path that is not a readable flat file is refused loudly at
// open by [Engine.validateSource], with its own wrong-driver diagnosis;
// this door reports identity, not readability.
func (Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	path := strings.TrimSpace(dsn)
	if path == "" {
		return ir.SourceIdentity{}
	}
	return ir.SourceIdentity{Database: filepath.Clean(path)}
}

// Pinned beside the method; the registry-derived roster
// (docsync.TestEverySourceEngineDescribesItsIdentity) enforces coverage.
var _ ir.SourceIdentityDescriber = Engine{}
