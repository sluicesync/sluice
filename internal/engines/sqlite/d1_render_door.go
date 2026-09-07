// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
)

// verifyD1RenderFidelity is the render-fidelity door for the D1 READER lane —
// the `--source-driver d1` bulk copy, `migrate --stage-local`, and the
// `d1-trigger` cold start, which share one paginator.
//
// # Why this lane needed its own door (audit LA-5)
//
// [CapturedValueExpr] renders a REAL through `format('%!.20g', …)`, and the
// `!` alternate-form-2 flag is load-bearing: an engine that ignores it clamps
// to 16 significant digits, which silently alters every REAL at exit 0. That
// was a CRITICAL (v0.131.2), and the fix for it was PROBED at runtime rather
// than assumed — because "no known SQLite release ignores `!`" is a fact about
// builds sluice cannot enumerate.
//
// The probe reached the TRIGGER readers. It did not reach this one, and
// [CapturedValueExpr]'s own doc is where that gap hid: it says "on D1 the
// probed engine IS the engine that fires the triggers, which closes the
// premise there at runtime". That sentence is true and it is about TRIGGERS. A
// `migrate` has no triggers, opens no CDC stream, and therefore ran no probe —
// while projecting the identical expression through [buildD1Projection]
// against an engine Cloudflare controls and can change under a running
// install. The guard's scope was narrower than the class its rationale named,
// which is the shape this repo keeps paying for.
//
// # Why it is safe to refuse here
//
// MEASURED against live Cloudflare D1 on 2026-09-07, with a discriminating
// control rather than a bare green: the production expression rendered
// `0.300000000000000044` (round-trips bit-exact), while the same format
// WITHOUT the `!` flag rendered `0.3` — the 16-digit clamp. So D1 honours the
// flag today, this door does not fire on a working D1, and the control proves
// the probe can still tell the two apart. (D1 refuses `sqlite_version()`, so
// the render itself is the only available evidence about that engine — which
// is precisely why the premise is probed rather than reasoned about.)
//
// It fails CLOSED, matching the trigger lane's door: a probe that cannot run
// is not permission to read. Reading without the premise verified is the thing
// this exists to prevent.
func verifyD1RenderFidelity(ctx context.Context, c *d1Client) error {
	rows, err := c.queryRows(ctx, RealRenderProbeSQL())
	if err != nil {
		return fmt.Errorf("d1: cannot probe the source's REAL render fidelity (%w); "+
			"refusing to read without verifying the format('%%!.20g') render", err)
	}
	if len(rows) != 1 {
		return fmt.Errorf("d1: the REAL render-fidelity probe returned %d rows, want exactly 1; "+
			"refusing to read without verifying the format('%%!.20g') render", len(rows))
	}
	raw, ok := rows[0]["p"]
	if !ok || len(raw) == 0 {
		return fmt.Errorf("d1: the REAL render-fidelity probe returned no %q column; "+
			"refusing to read without verifying the format('%%!.20g') render", "p")
	}
	// The render arrives as a JSON string (the CASE arm is format(), which
	// yields TEXT). Anything else — a JSON number, null — means the arm under
	// test did not run, so it is refused rather than coerced: a number would
	// already have been through a double and could not evidence the render.
	var rendered string
	if err := json.Unmarshal(raw, &rendered); err != nil {
		return fmt.Errorf("d1: the REAL render-fidelity probe returned %s, not the expected text render; "+
			"refusing to read without verifying the format('%%!.20g') render", raw)
	}
	return VerifyRealRenderProbe(rendered)
}
