// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"path/filepath"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// SourceIdentity implements [ir.SourceIdentityDescriber]: the DUMP
// DIRECTORY the DSN names is the dataset.
//
// SCOPE, stated so the name is not read as broader than the truth: this
// reports the DIRECTORY, not the MySQL database name inside it. The
// database name is not in the DSN at all — it is derived from the dump
// FILENAMES, which costs a directory read ([openDumpDir], which also
// refuses a directory carrying more than one database). The describer's
// contract is no-I/O, and it is called on a run that may be about to
// refuse, so it answers with what the DSN literally carries.
//
// That is enough for the door it serves: two `migrate --resume` runs
// pointed at DIFFERENT dumps name different directories and are refused,
// which is the silent-adoption case (audit 2026-09-15 F-1 — before this
// existed, every mydumper source rendered ONE identity). The residual is
// narrow and in the safe direction: two dumps of different databases
// staged into the SAME directory path, one after the other, compare
// equal — a shape that also destroys the first dump's files, so the
// second run has no copy of the first to adopt.
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
