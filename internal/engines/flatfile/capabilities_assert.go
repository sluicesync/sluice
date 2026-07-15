// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declaration of the one interface this package's concrete type
// implements (the sqlite/d1/mydumper convention). The reader surfaces are
// NOT pinned here because this engine has none of its own: OpenSchemaReader/
// OpenRowReader return the sqlite package's staged readers, whose optional
// surfaces (ir.InferredTypeValidator behind --infer-types, ir.Verifier
// behind verify --depth count, the batched-read family) are pinned in
// sqlite/capabilities_assert.go — a break there is what would silently
// degrade THIS engine too.
var _ ir.Engine = Engine{}
