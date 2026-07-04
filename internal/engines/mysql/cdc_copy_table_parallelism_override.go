// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ADR-0118 finding 4: the two cold-copy READ-axis knobs
// (vstream_copy_table_parallelism / copy_table_parallelism) are DSN-plumbable
// AND promoted to first-class `sync start` CLI flags. Precedence is explicit
// CLI flag > DSN param > engine default. Unlike the sql_mode DSN param — where
// the DSN wins over the CLI — here the CLI flag WINS over the DSN, because the
// operator typed it on the command line for this run specifically; the DSN form
// remains the lower-precedence default.
//
// The CLI override was formerly a process-wide MUTABLE package global set once at
// startup (SetVStreamCopyTableParallelismOverride /
// SetNativeCopyTableParallelismOverride). Task 2.5 (finding A-4) moves it onto the
// per-instance [Engine] value (engineOptions.{vstream,}CopyTableParallelism, set
// via [Engine.WithVStreamCopyTableParallelism] / [Engine.WithCopyTableParallelism]),
// so a fleet `sync run` can carry distinct values per sync. The resolution logic
// below is unchanged from the global era — an override > 0 wins over the DSN param;
// 0 (the zero value, unset) falls through to the DSN param then the engine default
// — only the SOURCE of the override moved from the global to the passed value.
//
// The resolver helpers below take the override as an explicit int argument so the
// From-DSN readers stay pure functions of (engine override, cfg); every caller is
// an [Engine] method (or a helper an Engine method threads the value into), so the
// per-instance override reaches them without a package global.
