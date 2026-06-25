// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "sync/atomic"

// ADR-0118 finding 4: the two cold-copy READ-axis knobs
// (vstream_copy_table_parallelism / copy_table_parallelism) were DSN-only;
// the ADR promotes each to a first-class `sync start` CLI flag while keeping
// the DSN form working verbatim. Precedence is explicit CLI flag > DSN param
// > engine default (1 = serial).
//
// The CLI override is threaded into the engine the same way the operator's
// --mysql-sql-mode / --zero-date policies are (a package-level setter called
// from the composition root before any connection opens; see connect.go's
// SetSessionSQLMode and value_decode.go's SetZeroDateMode). Unlike the
// sql_mode setter — where the DSN wins over the global — here the CLI flag
// WINS over the DSN, because the operator typed it on the command line for
// this run specifically; the DSN form remains the lower-precedence default.
//
// Zero-value-safety (the v0.99.51 trap): the override defaults to 0 = unset =
// "fall back to the DSN param", so every constructor / test / non-CLI caller
// that never sets it keeps the existing DSN-then-default behaviour byte for
// byte. Only a value > 0 (an explicitly-set CLI flag) overrides the DSN.
//
// atomic.Int32 because the setter is called once at startup from main-flow
// and the readers run on the engine's connection-open path; the value never
// changes after startup, but the atomic keeps the happens-before edge clean.
var (
	vstreamCopyTableParallelismOverride atomic.Int32
	nativeCopyTableParallelismOverride  atomic.Int32
)

// SetVStreamCopyTableParallelismOverride records the operator's explicit
// --vstream-copy-table-parallelism CLI value (ADR-0118 finding 4). A value
// > 0 wins over the source DSN's vstream_copy_table_parallelism param; 0 (the
// default) means "unset — fall back to the DSN param, then the engine
// default". Call once at startup before any engine opens a connection.
func SetVStreamCopyTableParallelismOverride(n int) {
	if n > 0 {
		vstreamCopyTableParallelismOverride.Store(int32(n))
	}
}

// SetNativeCopyTableParallelismOverride records the operator's explicit
// --copy-table-parallelism CLI value (ADR-0118 finding 4). A value > 0 wins
// over the source DSN's copy_table_parallelism param; 0 (the default) means
// "unset — fall back to the DSN param, then the engine default". Call once at
// startup before any engine opens a connection.
func SetNativeCopyTableParallelismOverride(n int) {
	if n > 0 {
		nativeCopyTableParallelismOverride.Store(int32(n))
	}
}
