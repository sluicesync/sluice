// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package d1trigger

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declaration of the ir interface this engine's concrete type
// intentionally implements. The orchestrator discovers optional surfaces by
// runtime type-assertion, so a method-set break wouldn't fail the build — the
// assertion would quietly stop matching and the pipeline would silently
// downgrade. This blank-var assertion turns that silent downgrade into a compile
// error here.
//
// The engine's surface is intentionally NARROW: it composes the `d1` engine by
// delegation (see the Engine doc) precisely so it does NOT inherit any optional
// opener the orchestrator type-asserts on that D1 cannot honour (the writer /
// target surfaces). D1-trigger is a CDC SOURCE only; do NOT "fix" a
// missing-interface error by widening this engine — that narrowness is
// load-bearing. The CDC reader type itself lives in the sqlite-trigger package
// (the shared implementation), where its [ir.CDCReader] conformance is pinned.
var _ ir.Engine = Engine{}
