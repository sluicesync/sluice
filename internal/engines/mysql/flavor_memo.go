// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strconv"
	"sync"

	mysql "github.com/go-sql-driver/mysql"
)

// A per-(server, flavor) memo for [Engine.checkServerFlavor], so the
// probe can run at EVERY connection-opening door instead of the two it
// used to.
//
// # Why every door
//
// The probe carries the MariaDB driver steer and the Vitess-under-vanilla
// REFUSAL, and the second is a silent-loss guard. Living at
// OpenSchemaReader and OpenSchemaWriter alone made its coverage depend on
// an unwritten claim — that every path reaching data opens one of those
// first. Bug 280 (v0.148.2 regression cycle) measured that claim failing:
// migrate opens the migration-state store at phase 1.75, ahead of both,
// and that store's own SQL is flavor-specific, so the run died on
// `Error 1064 … near 'AS new ON DUPLICATE KEY UPDATE'` — sluice's own
// statement — with the steer that names the right driver never reached.
//
// # Why a memo makes that affordable
//
// The row doors open per worker on the parallel copy path, so probing
// them unmemoised would add a `SELECT VERSION()` per worker. The answer
// cannot change under a running process for a given server: the version
// string is fixed for the server's lifetime, and the flavor is the
// operator's own flag. This mirrors [lctMemo] exactly, including its two
// rules — key on the server's NETWORK IDENTITY rather than the DSN (the
// doors legitimately pass differently-scoped DSNs to one server), and
// store no credentials.
//
// # What is cached, and what is not
//
// The VERDICT: nil, or the refusal. Both are deterministic for a
// (server, flavor) pair, and caching the refusal is the point — a
// refusing configuration must refuse at every door, not only the first.
// A probe that could not RUN is not a verdict and is never cached; the
// caller's own posture for that case is unchanged.
//
// The MariaDB steer WARN rides the same memo, so it is emitted once per
// (server, flavor) rather than once per door — which is what an operator
// wants from a steer that names one flag.
var flavorMemo = struct {
	mu       sync.Mutex
	byServer map[string]error
}{}

// flavorMemoKey identifies a (server, flavor) pair. Two DSNs differing
// only in their database component share a key, which is the point.
func flavorMemoKey(cfg *mysql.Config, flavor Flavor) string {
	return cfg.Net + "|" + cfg.Addr + "|" + strconv.Itoa(int(flavor))
}

// lookupFlavorVerdict reports whether a verdict was memoised for a
// (server, flavor) pair, and what it was.
func lookupFlavorVerdict(key string) (found bool, verdict error) {
	flavorMemo.mu.Lock()
	defer flavorMemo.mu.Unlock()
	verdict, found = flavorMemo.byServer[key]
	return found, verdict
}

// rememberFlavorVerdict memoises a verdict that was actually reached.
func rememberFlavorVerdict(key string, verdict error) {
	flavorMemo.mu.Lock()
	defer flavorMemo.mu.Unlock()
	if flavorMemo.byServer == nil {
		flavorMemo.byServer = map[string]error{}
	}
	flavorMemo.byServer[key] = verdict
}
