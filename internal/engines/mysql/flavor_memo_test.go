// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// THE FLAVOR MEMO IS BOUNDED, AND THE ONE SITE THAT SEES A SERVER
// SUBSTITUTION FORGETS IT.
//
// Audit 2026-09-15 A0915-MYSQL-MEDIUM-2: flavor_memo.go is process-global and caches a
// silent-loss guard's verdict (refuseVitessUnderNonVStreamFlavor) on the
// premise that "the answer cannot change under a running process". True
// of a server, false of an ADDRESS a days-long sync keeps opening doors
// on: a vtgate promoted behind an address that used to answer vanilla
// MySQL would keep the cached nil forever. The memo now expires entries,
// caps its size, and is forgotten outright by the lineage verdict — the
// engine's one observation of a substitution. Each of the three is pinned
// here; the doc comment on the memo states the residual window.
//
// These tests own the package-level memo, so they do not run in parallel
// with each other or with the door tests that also reset it.

func resetFlavorMemo(t *testing.T) {
	t.Helper()
	flavorMemo.mu.Lock()
	flavorMemo.byServer = nil
	flavorMemo.mu.Unlock()
	t.Cleanup(func() {
		flavorMemo.mu.Lock()
		flavorMemo.byServer = nil
		flavorMemo.mu.Unlock()
	})
}

func backdateFlavorMemoEntry(t *testing.T, key string, by time.Duration) {
	t.Helper()
	flavorMemo.mu.Lock()
	defer flavorMemo.mu.Unlock()
	e, ok := flavorMemo.byServer[key]
	if !ok {
		t.Fatalf("no memo entry under %q to backdate", key)
	}
	e.probedAt = e.probedAt.Add(-by)
	flavorMemo.byServer[key] = e
}

func TestFlavorMemo_EntriesExpire(t *testing.T) {
	resetFlavorMemo(t)
	cfg := &mysql.Config{Net: "tcp", Addr: "db.example:3306", DBName: "app"}
	key := flavorMemoKey(cfg, FlavorVanilla)
	refusal := errors.New("vitess under vanilla")

	rememberFlavorVerdict(key, refusal)
	if found, verdict := lookupFlavorVerdict(key); !found || !errors.Is(verdict, refusal) {
		t.Fatalf("a fresh verdict was not served back (found=%v verdict=%v)", found, verdict)
	}
	// Inside the window the fan-out shares one probe — the memo's reason
	// for existing. Just short of the TTL still hits.
	backdateFlavorMemoEntry(t, key, flavorMemoTTL-time.Second)
	if found, _ := lookupFlavorVerdict(key); !found {
		t.Fatalf("a verdict %s old was dropped; the TTL is %s", flavorMemoTTL-time.Second, flavorMemoTTL)
	}
	// Past it, the next door re-probes: absent, and the entry is gone so a
	// refusal reached long ago cannot be served after the server changed.
	backdateFlavorMemoEntry(t, key, 2*time.Second)
	if found, verdict := lookupFlavorVerdict(key); found {
		t.Fatalf("an EXPIRED verdict was served (%v) — a server substitution behind this address would keep "+
			"the stale verdict for the life of the process (audit 2026-09-15 A0915-MYSQL-MEDIUM-2)", verdict)
	}
	flavorMemo.mu.Lock()
	_, still := flavorMemo.byServer[key]
	flavorMemo.mu.Unlock()
	if still {
		t.Error("the expired entry was reported absent but left in the map")
	}
}

func TestFlavorMemo_ForgetByServerDropsEveryFlavorForThatServerOnly(t *testing.T) {
	resetFlavorMemo(t)
	replaced := &mysql.Config{Net: "tcp", Addr: "db.example:3306"}
	other := &mysql.Config{Net: "tcp", Addr: "db.example:3307"}
	for _, flavor := range []Flavor{FlavorVanilla, FlavorMariaDB} {
		rememberFlavorVerdict(flavorMemoKey(replaced, flavor), nil)
		rememberFlavorVerdict(flavorMemoKey(other, flavor), nil)
	}

	forgetFlavorVerdicts(flavorMemoServerKey(replaced))

	for _, flavor := range []Flavor{FlavorVanilla, FlavorMariaDB} {
		if found, _ := lookupFlavorVerdict(flavorMemoKey(replaced, flavor)); found {
			t.Errorf("flavor %d's verdict for the replaced server survived forgetFlavorVerdicts", flavor)
		}
		if found, _ := lookupFlavorVerdict(flavorMemoKey(other, flavor)); !found {
			t.Errorf("flavor %d's verdict for an UNRELATED server was dropped — the forget is keyed on one "+
				"server's network identity", flavor)
		}
	}
	// A reader built without a DSN carries no key; forgetting "" must not
	// sweep the whole memo.
	forgetFlavorVerdicts("")
	if found, _ := lookupFlavorVerdict(flavorMemoKey(other, FlavorVanilla)); !found {
		t.Error("forgetFlavorVerdicts(\"\") emptied the memo; it must be a no-op")
	}
}

func TestFlavorMemo_IsBoundedAndEvictsTheOldest(t *testing.T) {
	resetFlavorMemo(t)
	for i := 0; i < flavorMemoMaxEntries+1; i++ {
		key := flavorMemoKey(&mysql.Config{Net: "tcp", Addr: "host" + strconv.Itoa(i) + ":3306"}, FlavorVanilla)
		rememberFlavorVerdict(key, nil)
		// Make insertion order the age order regardless of clock
		// resolution: each earlier entry is a little older.
		backdateFlavorMemoEntry(t, key, time.Duration(flavorMemoMaxEntries-i)*time.Millisecond)
	}
	flavorMemo.mu.Lock()
	n := len(flavorMemo.byServer)
	flavorMemo.mu.Unlock()
	if n != flavorMemoMaxEntries {
		t.Fatalf("memo holds %d entries after %d inserts; the cap is %d", n, flavorMemoMaxEntries+1, flavorMemoMaxEntries)
	}
	if found, _ := lookupFlavorVerdict(flavorMemoKey(&mysql.Config{Net: "tcp", Addr: "host0:3306"}, FlavorVanilla)); found {
		t.Error("the OLDEST entry survived the cap; eviction is not oldest-first")
	}
	if found, _ := lookupFlavorVerdict(flavorMemoKey(&mysql.Config{Net: "tcp", Addr: "host1:3306"}, FlavorVanilla)); !found {
		t.Error("the second-oldest entry was evicted; only one eviction should have been needed")
	}
}

// TestVerifyPositionResumable_ForeignVerdictForgetsTheFlavorMemo pins the
// hook: a binlog reader that reaches ir.ErrPositionForeignLineage — from
// either of its two callers, the warm-resume open and the reactive door's
// VerifyLineage — forgets the flavor verdicts memoised for its server. The
// non-foreign shapes (a reset server with the SAME uuid, an unreadable
// witness) must leave the memo alone, or every resume would defeat it.
func TestVerifyPositionResumable_ForeignVerdictForgetsTheFlavorMemo(t *testing.T) {
	resetFlavorMemo(t)
	ctx := context.Background()
	gtidPos := func(set string) ir.Position {
		p, err := encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return p
	}
	const resume = "aaaaaaaa-0000-0000-0000-000000000001:1-15"
	server := flavorMemoServerKey(&mysql.Config{Net: "tcp", Addr: "db.example:3306"})
	remember := func() {
		rememberFlavorVerdict(server+"|"+strconv.Itoa(int(FlavorVanilla)), nil)
		rememberFlavorVerdict(server+"|"+strconv.Itoa(int(FlavorMariaDB)), nil)
	}
	held := func() bool {
		a, _ := lookupFlavorVerdict(server + "|" + strconv.Itoa(int(FlavorVanilla)))
		b, _ := lookupFlavorVerdict(server + "|" + strconv.Itoa(int(FlavorMariaDB)))
		return a && b
	}

	t.Run("foreign lineage forgets the server's verdicts", func(t *testing.T) {
		remember()
		r := &CDCReader{
			db:               newLineageFakeDB(t, "contained=0|executed=bbbbbbbb-0000-0000-0000-000000000002:1-8|uuid=bbbbbbbb-0000-0000-0000-000000000002"),
			flavor:           FlavorVanilla,
			flavorMemoServer: server,
		}
		if err := r.VerifyLineage(ctx, gtidPos(resume)); !errors.Is(err, ir.ErrPositionForeignLineage) {
			t.Fatalf("VerifyLineage = %v; want ErrPositionForeignLineage", err)
		}
		if held() {
			t.Fatal("the flavor memo still holds this server's verdicts after a FOREIGN-lineage verdict — the " +
				"re-copy the pipeline runs next would trust a verdict probed on a different server (audit 2026-09-15 A0915-MYSQL-MEDIUM-2)")
		}
	})
	t.Run("a reset server with the same uuid leaves the memo alone", func(t *testing.T) {
		remember()
		r := &CDCReader{
			db:               newLineageFakeDB(t, "contained=0|executed=|uuid=aaaaaaaa-0000-0000-0000-000000000001"),
			flavor:           FlavorVanilla,
			flavorMemoServer: server,
		}
		if err := r.VerifyLineage(ctx, gtidPos(resume)); err != nil {
			t.Fatalf("VerifyLineage = %v; want nil", err)
		}
		if !held() {
			t.Fatal("a non-foreign verdict emptied the memo; only positive evidence of a substitution may")
		}
	})
	t.Run("an unreadable witness leaves the memo alone", func(t *testing.T) {
		remember()
		r := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed="), flavor: FlavorVanilla, flavorMemoServer: server}
		if err := r.VerifyLineage(ctx, gtidPos(resume)); err != nil {
			t.Fatalf("VerifyLineage = %v; want nil", err)
		}
		if !held() {
			t.Fatal("an unreadable witness emptied the memo; it is not evidence of anything")
		}
	})
}
