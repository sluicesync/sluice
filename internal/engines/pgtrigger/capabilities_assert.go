// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declarations of the ir interfaces this engine's
// concrete types intentionally implement.
//
// Why this file exists: the orchestrator discovers optional surfaces
// by runtime type-assertion, so a method-set break doesn't fail the
// build — the assertion quietly stops matching and the pipeline
// silently downgrades to the fallback path. The blank-var assertions
// below turn that silent downgrade into a compile error here.
//
// The trigger engine's surface is intentionally NARROW. It composes
// [postgres.Engine] by delegation (NOT embedding-with-promotion —
// see the Engine type's doc comment), precisely so it does NOT
// satisfy the slot-flavoured optional openers the orchestrator
// type-asserts on: [ir.SlotManagerOpener], [ir.CDCReaderWithSlotOpener],
// [ir.SnapshotStreamWithSlotOpener]. A slot-less managed-PG tier is
// the engine's reason to exist; inheriting those surfaces would
// silently route operators through slot management the server forbids.
// Do NOT "fix" a missing-interface compile error by widening this
// engine — that narrowness is load-bearing.
var (
	_ ir.Engine            = Engine{}
	_ ir.ConnectionLabeler = Engine{}
	_ ir.CDCReader         = (*CDCReader)(nil)
)
