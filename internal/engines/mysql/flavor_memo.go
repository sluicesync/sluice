// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strconv"
	"strings"
	"sync"
	"time"

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
// # Why a memo makes that affordable, and what bounds it
//
// The row doors open per worker on the parallel copy path, so probing
// them unmemoised would add a `SELECT VERSION()` per worker. Those
// workers open within seconds of each other, which is the whole window
// the memo needs. It is keyed on the server's NETWORK IDENTITY rather
// than the DSN (the doors legitimately pass differently-scoped DSNs to
// one server) and stores no credentials — the two rules [lctMemo]
// follows too.
//
// The first cut said the answer "cannot change under a running process".
// That is true of a server and false of an ADDRESS: `sluice sync` runs
// for days and `metrics-watch` indefinitely, and a DSN behind DNS or a
// load balancer can begin answering a different server — a vtgate
// promoted behind an address that used to answer vanilla MySQL, say —
// which is precisely the substitution the lineage arc exists to notice.
// A verdict cached forever would then hold a stale nil and the refusal
// would never fire again for the life of the process (audit 2026-09-15
// A0915-MYSQL-MEDIUM-2). So the memo is bounded two ways, and both are pinned:
//
//   - An entry EXPIRES after [flavorMemoTTL]: a door opened after the
//     window re-probes, so a substitution is seen at the next door open
//     at most one TTL late. Within a run's fan-out the entry is shared
//     exactly as before (TestFlavorMemo_EntriesExpire).
//   - The one site in this engine that OBSERVES a substitution — the
//     binlog reader's lineage verdict, `ir.ErrPositionForeignLineage` —
//     forgets the server's entries outright ([forgetFlavorVerdicts]),
//     so the re-copy the pipeline runs next re-probes at its first door
//     rather than waiting out the TTL
//     (TestVerifyPositionResumable_ForeignVerdictForgetsTheFlavorMemo).
//     The VStream flavors skip the probe entirely and memoise nothing,
//     so their lineage verdict has nothing to forget.
//
// What a stale entry can still cost, stated: a substitution the lineage
// door does not see (no CDC position in play — a plain `migrate`, or a
// sync's live-add copy) is re-probed no later than the next door open
// after the TTL. That is the bound, not a guarantee of zero staleness;
// the memo trades one probe per worker for a window of at most
// flavorMemoTTL, and the window is written here so it can be argued with.
//
// The size is capped at [flavorMemoMaxEntries] (oldest evicted) so a
// process that opens doors on many servers — a metrics-watch fleet — does
// not grow it without bound.
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
// (server, flavor, TTL window) rather than once per door — which is what
// an operator wants from a steer that names one flag.
var flavorMemo = struct {
	mu       sync.Mutex
	byServer map[string]flavorMemoEntry
}{}

// flavorMemoEntry is one memoised verdict and when it was reached.
type flavorMemoEntry struct {
	verdict  error
	probedAt time.Time
}

// flavorMemoTTL bounds how long a verdict is trusted without re-probing.
// Long enough that every worker of one copy fan-out shares one probe;
// short enough that a days-long sync notices a server substitution behind
// its address at the next door it opens after the window.
const flavorMemoTTL = 5 * time.Minute

// flavorMemoMaxEntries caps the memo; the oldest entry is evicted past it.
const flavorMemoMaxEntries = 64

// flavorMemoServerKey identifies a server by network identity — two DSNs
// differing only in their database component share a key, which is the
// point. It is the prefix every flavor's key for that server shares, so
// [forgetFlavorVerdicts] can drop them together.
func flavorMemoServerKey(cfg *mysql.Config) string {
	return cfg.Net + "|" + cfg.Addr
}

// flavorMemoKey identifies a (server, flavor) pair.
func flavorMemoKey(cfg *mysql.Config, flavor Flavor) string {
	return flavorMemoServerKey(cfg) + "|" + strconv.Itoa(int(flavor))
}

// lookupFlavorVerdict reports whether a live verdict was memoised for a
// (server, flavor) pair, and what it was. An expired entry is dropped and
// reported absent, so the caller re-probes.
func lookupFlavorVerdict(key string) (found bool, verdict error) {
	flavorMemo.mu.Lock()
	defer flavorMemo.mu.Unlock()
	e, found := flavorMemo.byServer[key]
	if !found {
		return false, nil
	}
	if time.Since(e.probedAt) > flavorMemoTTL {
		delete(flavorMemo.byServer, key)
		return false, nil
	}
	return true, e.verdict
}

// rememberFlavorVerdict memoises a verdict that was actually reached,
// evicting expired entries and — past the cap — the oldest live one.
func rememberFlavorVerdict(key string, verdict error) {
	flavorMemo.mu.Lock()
	defer flavorMemo.mu.Unlock()
	if flavorMemo.byServer == nil {
		flavorMemo.byServer = map[string]flavorMemoEntry{}
	}
	now := time.Now()
	for k, e := range flavorMemo.byServer {
		if now.Sub(e.probedAt) > flavorMemoTTL {
			delete(flavorMemo.byServer, k)
		}
	}
	for len(flavorMemo.byServer) >= flavorMemoMaxEntries {
		oldestKey, oldest := "", now
		for k, e := range flavorMemo.byServer {
			if oldestKey == "" || e.probedAt.Before(oldest) {
				oldestKey, oldest = k, e.probedAt
			}
		}
		delete(flavorMemo.byServer, oldestKey)
	}
	flavorMemo.byServer[key] = flavorMemoEntry{verdict: verdict, probedAt: now}
}

// forgetFlavorVerdicts drops every flavor's memoised verdict for one
// server, identified by [flavorMemoServerKey]. Called where the engine
// has POSITIVE evidence the server behind an address is not the one it
// probed — the foreign-lineage verdict — so the next door re-probes at
// once. An empty key (a reader built without one, in tests) is a no-op.
func forgetFlavorVerdicts(serverKey string) {
	if serverKey == "" {
		return
	}
	flavorMemo.mu.Lock()
	defer flavorMemo.mu.Unlock()
	for k := range flavorMemo.byServer {
		if strings.HasPrefix(k, serverKey+"|") {
			delete(flavorMemo.byServer, k)
		}
	}
}
