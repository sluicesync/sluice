// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
)

// WHICH ENGINES ARM A SOCKET WRITE DEADLINE, kept honest against the registry
// (roadmap item 146 / Bug 229).
//
// The defect: a cold copy whose TARGET connection died mid-`LOAD DATA` parked
// in net.(*conn).Write forever — no error, no exit code, no log line, zero
// rows. A socket write with no deadline blocks for as long as the peer
// refuses to drain, and a peer that is gone never drains and never resets.
//
// This gate exists because the fix's most likely failure mode is the one this
// project keeps paying for: a deadline that lands on MySQL's `LOAD DATA` and
// nowhere else. The two mechanisms are deliberately different per driver —
// [mysql.Config.WriteTimeout] where the driver has a first-class field,
// [netdeadline.Dialer] where it does not — so the roster records WHICH, not
// merely THAT.
//
// # Scope of this gate, stated so it cannot be read as broader
//
// It proves every registered engine either arms a deadline or carries a
// written reason it needs none, and (in the walker below) that no socket dial
// site in internal/engines composes only half the transport policy. It does
// NOT prove a deadline actually surfaces a dead peer — that is
// TestWrapConn_RealSocketWriteToANonDrainingPeerTimesOut (internal/netdeadline,
// real sockets) and TestLoadDataWriteDeadline_DeadSocketFailsLoudly
// (integration, a real MySQL behind a blackholing proxy). Nor does it reach
// the READ side, which deliberately has no deadline (see the netdeadline
// package doc for why, and what covers it instead).
var writeDeadlineMechanism = map[string]string{
	"mysql":            "mysql.Config.WriteTimeout, set for every parse entry point in engines/mysql/connect.go finishParseDSN; the binlog syncer takes netdeadline.Dialer separately (it does not read that cfg)",
	"planetscale":      "same engine code as mysql — a Flavor, not a separate connection path",
	"vitess":           "same engine code as mysql — a Flavor, not a separate connection path",
	"mariadb":          "same engine code as mysql — a Flavor, not a separate connection path",
	"postgres":         "netdeadline.Dialer on all three pgx funnels (openPgxDBAs, openPgxDBDescribeExec, the CDC replication conn)",
	"postgres-trigger": "no connection funnel of its own — every pool comes from postgres.OpenPgxDB, which composes netdeadline.Dialer",
}

// writeDeadlineExempt records, per engine, why it needs no socket write
// deadline. Every entry must name a reason the class is UNREACHABLE there —
// "unlikely" is not a reason.
var writeDeadlineExempt = map[string]string{
	"sqlite":         "no socket: modernc SQLite writes to a local file through the OS filesystem layer, which has no receive window to close",
	"sqlite-trigger": "same local-file transport as sqlite",
	"d1":             "HTTP, not a raw socket: the Cloudflare D1 client bounds the WHOLE request with http.Client.Timeout (d1.go / d1_conn.go), which already covers a peer that stops draining",
	"d1-trigger":     "same bounded HTTP client as d1",
	"csv":            "filesystem source — no network transport",
	"tsv":            "filesystem source — the same flatfile engine as csv",
	"ndjson":         "filesystem source — the same flatfile engine as csv",
	"mydumper":       "filesystem source — a dump directory is read, never written over a socket",
}

func TestEverySocketEngineArmsAWriteDeadline(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor, same shape and the same reason as the other
	// registry rosters in this package: an empty registry passes everything.
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list in this package has drifted from cmd/sluice — "+
			"the roster below would under-report", len(names), names)
	}

	for _, name := range names {
		mech, hasMech := writeDeadlineMechanism[name]
		reason, exempt := writeDeadlineExempt[name]
		switch {
		case hasMech && exempt:
			t.Errorf("engine %q is listed BOTH as arming a deadline (%s) and as exempt (%s) — one of the two entries is stale",
				name, mech, reason)
		case hasMech:
			if strings.TrimSpace(mech) == "" {
				t.Errorf("engine %q has an empty mechanism entry; name the mechanism, not the fact", name)
			}
		case exempt:
			if strings.TrimSpace(reason) == "" {
				t.Errorf("engine %q has an empty exemption reason; name why the class is unreachable", name)
			}
		default:
			t.Errorf("engine %q appears in neither writeDeadlineMechanism nor writeDeadlineExempt. A new engine that "+
				"opens a socket and arms no write deadline hangs forever when its peer dies mid-write (Bug 229). "+
				"Record which mechanism it uses, or why it needs none.", name)
		}
	}

	// Stale-entry hygiene, both maps: an entry for an engine that no longer
	// registers is a claim nobody checks.
	known := map[string]bool{}
	for _, n := range names {
		known[n] = true
	}
	for _, m := range []map[string]string{writeDeadlineMechanism, writeDeadlineExempt} {
		for name := range m {
			if !known[name] {
				t.Errorf("roster names %q, which is not a registered engine — drop or rename the stale entry", name)
			}
		}
	}
}

// The dial-site walker. The roster above is a list; this is the mechanism
// that catches a NEW site the list has not heard of yet.
//
// Every socket-dialing funnel in internal/engines assigns a dial hook to a
// driver's DialFunc / Dialer field. Each such assignment must read
// `netdeadline.Dialer()` — the one spelling that composes BOTH halves of
// sluice's transport policy (keep-alive for an idle socket, the write
// deadline for one with a write in flight). A site that assigns
// `netkeepalive.Dialer().DialContext` directly has half the policy, which is
// exactly the state every connection was in before item 146.
func TestEveryEngineDialSiteComposesTheFullTransportPolicy(t *testing.T) {
	const want = "netdeadline.Dialer()"

	// dialSiteExempt records, per "file.go:line-ish site", why a dial
	// assignment legitimately does not use netdeadline.Dialer. Keyed by file
	// so a moved line does not silently drop the entry.
	dialSiteExempt := map[string]string{}

	fset := token.NewFileSet()
	sites := 0
	var offenders []string

	err := filepath.WalkDir(filepath.Join("..", "engines"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		check := func(field string, val ast.Expr) {
			if field != "DialFunc" && field != "Dialer" {
				return
			}
			sites++
			got := exprText(fset, val)
			if got == want {
				return
			}
			base := filepath.Base(path)
			if _, ok := dialSiteExempt[base]; ok {
				return
			}
			pos := fset.Position(val.Pos())
			offenders = append(offenders, fmt.Sprintf("%s:%d  %s = %s", base, pos.Line, field, got))
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.KeyValueExpr:
				if k, ok := node.Key.(*ast.Ident); ok {
					check(k.Name, node.Value)
				}
			case *ast.AssignStmt:
				for i, lhs := range node.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok || i >= len(node.Rhs) {
						continue
					}
					check(sel.Sel.Name, node.Rhs[i])
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/engines: %v", err)
	}

	// Anti-vacuity floor: four production dial sites are known to exist (the
	// MySQL binlog syncer plus Postgres's three pgx funnels). A walker that
	// stopped matching would otherwise be green for exactly the defect it was
	// built for.
	if sites < 4 {
		t.Fatalf("found %d dial-hook assignment(s) under internal/engines; floor 4 (mysql binlog syncer + the three "+
			"postgres pgx funnels) — the walk is vacuous, re-point it", sites)
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("%d dial site(s) do not compose the full transport policy:\n  %s\n\n"+
			"Each must read %s, which is keep-alive AND the item-146 per-write deadline. A site carrying only "+
			"netkeepalive.Dialer().DialContext blocks forever on a peer that stopped draining — keep-alive probes are "+
			"only sent on an IDLE socket, and a socket with a write in flight is not idle (Bug 229).",
			len(offenders), strings.Join(offenders, "\n  "), want)
	}
}

// exprText renders an expression back to source-ish text for the two shapes
// this walker cares about (`pkg.Fn()` and `pkg.Fn().Method`), and to a
// stable placeholder for anything else — enough to compare against the one
// accepted spelling and to name an offender usefully.
func exprText(fset *token.FileSet, e ast.Expr) string {
	switch v := e.(type) {
	case *ast.CallExpr:
		return exprText(fset, v.Fun) + "()"
	case *ast.SelectorExpr:
		return exprText(fset, v.X) + "." + v.Sel.Name
	case *ast.Ident:
		return v.Name
	default:
		return fmt.Sprintf("<%T>", e)
	}
}
