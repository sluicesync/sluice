// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// engineOptions holds the per-instance CLI-flag overrides that were formerly
// process-wide MUTABLE package globals set by the CLI (audit task 2.5 /
// finding A-4). They live ON the [Engine] value so a fleet `sync run` can
// carry distinct values per sync — one global no longer spans every sync in
// the process. Each is applied via the [Engine.With*] builders BEFORE the CLI
// hands the engine to the orchestrator (the [ir.ConnectionLabeler] template).
//
// Zero-value contract (the v0.99.51 trap): EVERY field's zero value reproduces
// today's default behaviour, so every construction that never sets it — tests,
// broker/chain paths, `engines.Get` callers that don't apply the flags — stays
// byte-identical to the pre-task-2.5 global-default behaviour:
//
//   - sqlMode nil        → defaultStrictSQLMode (the Bug 102/103 strict-by-
//     default). A pointer, so the zero value can't be
//     confused with an explicit "" (fall through to the
//     server default — the legacy-data escape hatch).
//   - zeroDate inherit   → resolves to zeroDateRefuse (loud) at decode, exactly
//     as the former zeroDatePolicy global default did.
//   - {vstream,}CopyTableParallelism 0 → UNSET: fall back to the DSN param then
//     the engine default. A value > 0 WINS over the DSN,
//     exactly as the former atomic-int overrides did.
//   - preserveSkew false → UNSET: fall back to the DSN param then the relaxed
//     MinimizeSkew-off default (ADR-0120). Opt-out-named so
//     the safe/common (relaxed) behaviour is the zero value.
type engineOptions struct {
	// sqlMode is the operator's --mysql-sql-mode override for the session
	// sql_mode sluice forces on every MySQL connection ([openDB]). nil = unset.
	sqlMode *string

	// zeroDate is the operator's --zero-date default policy (ADR-0127) a reader
	// falls back to when its own per-source mode is zeroDateInherit.
	zeroDate zeroDateMode

	// vstreamCopyTableParallelism / copyTableParallelism are the read-axis CLI
	// overrides (ADR-0118 finding 4) consulted ahead of the source DSN param.
	vstreamCopyTableParallelism int
	copyTableParallelism        int

	// preserveSkew is the --vstream-preserve-skew opt-out (ADR-0120): true
	// forces the old MinimizeSkew hold, winning over the DSN param.
	preserveSkew bool
}

// WithSQLMode returns a copy of the engine whose MySQL connections force the
// operator's --mysql-sql-mode value (task 2.5, replacing SetSessionSQLMode). An
// empty string is the explicit "fall through to the server default" escape hatch
// for legacy MySQL data; any other value is forced verbatim. The per-connection
// DSN `?sql_mode=` param still wins over this value ([openDB]). Returns a
// configured copy — the registry's engine value stays override-free — mirroring
// [ir.ConnectionLabeler].
func (e Engine) WithSQLMode(mode string) ir.Engine {
	e.opts.sqlMode = &mode
	return e
}

// WithZeroDate returns a copy of the engine carrying the operator's --zero-date
// default policy (ADR-0127; task 2.5, replacing SetZeroDateMode). It validates
// the value (kong already enum-checks it; this re-checks defensively) and refuses
// loudly on a bad one. An empty string keeps the loud refuse default.
func (e Engine) WithZeroDate(mode string) (ir.Engine, error) {
	m, err := parseZeroDateMode(mode)
	if err != nil {
		return nil, fmt.Errorf("mysql: invalid --zero-date %q (%w)", mode, err)
	}
	e.opts.zeroDate = m
	return e, nil
}

// WithVStreamCopyTableParallelism returns a copy of the engine recording the
// operator's explicit --vstream-copy-table-parallelism value (ADR-0118 finding
// 4; task 2.5, replacing SetVStreamCopyTableParallelismOverride). A value > 0
// wins over the source DSN's vstream_copy_table_parallelism param; 0 (the
// default) leaves the DSN-then-default behaviour byte-identical.
//
// Returns the concrete [Engine] (not ir.Engine) so the CLI can chain the three
// MySQL-only read-axis builders; the caller assigns the result to its ir.Engine.
func (e Engine) WithVStreamCopyTableParallelism(n int) Engine {
	if n > 0 {
		e.opts.vstreamCopyTableParallelism = n
	}
	return e
}

// WithCopyTableParallelism returns a copy of the engine recording the operator's
// explicit --copy-table-parallelism value (ADR-0118 finding 4; task 2.5,
// replacing SetNativeCopyTableParallelismOverride). A value > 0 wins over the
// source DSN's copy_table_parallelism param; 0 (the default) leaves the
// DSN-then-default behaviour byte-identical.
func (e Engine) WithCopyTableParallelism(n int) Engine {
	if n > 0 {
		e.opts.copyTableParallelism = n
	}
	return e
}

// WithVStreamPreserveSkew returns a copy of the engine recording the operator's
// --vstream-preserve-skew value (ADR-0120; task 2.5, replacing
// SetVStreamPreserveSkewOverride). true wins over the source DSN's
// vstream_preserve_skew param and restores the old MinimizeSkew=true behaviour;
// false (the default) leaves the new relaxed MinimizeSkew=false default. Only a
// true value is recorded, so a non-CLI caller never inverts the DSN-then-default
// behaviour (write-once-true, matching the former global's semantics).
func (e Engine) WithVStreamPreserveSkew(preserve bool) Engine {
	if preserve {
		e.opts.preserveSkew = true
	}
	return e
}

// resolveReaderZeroDate collapses a reader's per-source DSN mode against the
// engine's --zero-date default: the DSN param wins when set, else the engine
// default (which is zeroDateInherit for an override-free engine → the reader
// keeps inherit and [applyZeroDatePolicy] resolves it to the loud refuse
// default). This is the single fold every reader-construction site funnels the
// DSN mode through so the engine default reaches readers without a global.
func (e Engine) resolveReaderZeroDate(dsnMode zeroDateMode) zeroDateMode {
	return foldZeroDate(dsnMode, e.opts.zeroDate)
}

// foldZeroDate returns dsnMode when it is an explicit per-source override, else
// the engine default engineMode. Both may be zeroDateInherit (an override-free
// engine / a DSN without zero_date), in which case the reader keeps inherit and
// [applyZeroDatePolicy] resolves it to zeroDateRefuse.
func foldZeroDate(dsnMode, engineMode zeroDateMode) zeroDateMode {
	if dsnMode != zeroDateInherit {
		return dsnMode
	}
	return engineMode
}
