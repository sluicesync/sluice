// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"sync"

	mysql "github.com/go-sql-driver/mysql"
)

// A per-SERVER memo for `lower_case_table_names`, because the two callers of
// [Engine.lowerCaseTableNames] are both per-NAMESPACE and one of them landed
// on a fan-out (perf-parity sweep 2026-08-07).
//
// # The cost this removes
//
// [Engine.FoldNamespace] asks once per source namespace and
// [Engine.PreflightTableNameFold] once per database, so a multi-database
// fan-out at 200 databases opened roughly 400 short-lived connect / handshake /
// read-one-variable / close cycles before a single row moved. Each is cheap;
// 400 serial round trips on a WAN target is not, and none of them could return
// a different answer.
//
// # Why one answer per server is CORRECT, and where that premise is checked
//
// `lower_case_table_names` is a GLOBAL, and it is fixed when the data directory
// is initialised: MySQL 8 makes it read-only at runtime and refuses to start
// against a data directory initialised under a different value. So every
// database on one server folds identically, and it cannot change under a
// running process.
//
// That is an environmental fact holding up a caching decision, so it gets a
// check rather than a sentence (CLAUDE.md's premise-naming step):
// TestLowerCaseTableNamesIsReadOnlyAtRuntime (integration, real MySQL and
// MariaDB) asserts the server REFUSES `SET GLOBAL lower_case_table_names`. If
// a future server made it settable, that test fails and this memo is what has
// to change.
//
// # Scope, stated so it cannot be read as broader
//
// The key is the server's NETWORK IDENTITY (net + address), not the DSN: the
// two callers pass different DSNs to the same server on purpose — the fan-out
// deliberately passes the server DSN to the fold preflight while the fold
// helper may see a database-scoped one — and keying on the DSN string would
// miss exactly the sharing this exists for. Credentials are not part of the key
// and are not stored.
//
// It memoises SUCCESSES only. A read that failed is not an answer, and caching
// it would make one transient connection failure sticky for the process.
//
// It is process-wide rather than per-Engine because the registry holds Engine
// VALUES (`var _ ir.TableNameFoldPreflighter = Engine{}`) that are copied on
// every lookup, so a field on the struct would memoise nothing for the callers
// that matter. A plain mutex is enough: this is a once-per-database call on a
// preflight path, never a hot loop.
var lctMemo = struct {
	mu       sync.Mutex
	byServer map[string]int
}{}

// lctMemoKey identifies the SERVER a config points at. Two DSNs differing only
// in their database component (or in any connection parameter) share a key,
// which is the whole point.
func lctMemoKey(cfg *mysql.Config) string {
	return cfg.Net + "|" + cfg.Addr
}

// lookupLCT returns the memoised setting for a server, if one was read.
func lookupLCT(key string) (int, bool) {
	lctMemo.mu.Lock()
	defer lctMemo.mu.Unlock()
	lct, ok := lctMemo.byServer[key]
	return lct, ok
}

// rememberLCT memoises a successfully read setting.
func rememberLCT(key string, lct int) {
	lctMemo.mu.Lock()
	defer lctMemo.mu.Unlock()
	if lctMemo.byServer == nil {
		lctMemo.byServer = map[string]int{}
	}
	lctMemo.byServer[key] = lct
}
