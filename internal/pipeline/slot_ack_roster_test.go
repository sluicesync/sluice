// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// GC-41's pipeline half. A Postgres CDC reader never acks its slot past what
// its consumer has RELEASED ([slotAckReleaser]); the consumer releases only
// positions it holds durably. So every pipeline call site that starts a CDC
// stream must hand the reader to one of the two release mechanisms, or that
// stream's slot never advances (unbounded source WAL retention — silent, the
// shape of GC-41's lane-path defect):
//
//   - the streamer mechanism: the site calls captureSlotAckReleaser, and the
//     apply-phase sidecar (startSlotAckCeiling, wired in
//     phaseStartApplySidecars) releases the TARGET's persisted position;
//   - the backup-chain mechanism: the site's file calls releaseChainAckTo
//     after each durable manifest commit.
//
// The universe is DERIVED: every `.StreamChanges(` call in the pipeline's
// production files. A new site fails here until someone classifies it, and the
// classification is then CHECKED against the AST rather than trusted. Reach:
// internal/pipeline and internal/pipeline/backup production files; a CDC
// consumer outside those directories is not seen (there is none today — the CLI
// reaches CDC only through these orchestrators).

type slotAckMechanism int

const (
	// slotAckStreamer: the enclosing function calls captureSlotAckReleaser.
	slotAckStreamer slotAckMechanism = iota
	// slotAckChain: the enclosing file calls releaseChainAckTo.
	slotAckChain
)

// slotAckReleaseRoster classifies every StreamChanges call site. Key:
// "<relpath>::<enclosingFunc>".
var slotAckReleaseRoster = map[string]slotAckMechanism{
	"streamer_coldstart.go::(*Streamer).coldStartBeginCDC":     slotAckStreamer, // single-namespace cold start → CDC handoff.
	"streamer_warm_resume.go::(*Streamer).warmResume":          slotAckStreamer, // warm resume; also the stopped-cold-start resume path's CDC open.
	"streamer_multidb.go::(*Streamer).coldStartMultiDatabase":  slotAckStreamer, // multi-schema (PG) / multi-database (MySQL) cold start.
	"streamer_multidb.go::(*Streamer).warmResumeMultiDatabase": slotAckStreamer, // its warm resume.
	"incremental.go::(*IncrementalBackup).Run":                 slotAckChain,    // `backup incremental`: releases the committed EndPosition.
	"stream.go::(*BackupStream).Run":                           slotAckChain,    // `backup stream`: the transient-retry reopen; releases each rollover.
	"stream.go::(*BackupStream).newRolloverLoop":               slotAckChain,    // `backup stream`: the first open.
}

// slotAckRosterScanDirs are the directories whose production files are walked,
// relative to the pipeline package.
var slotAckRosterScanDirs = []string{".", "backup"}

func TestSlotAckReleaseRoster_EveryStreamChangesSiteReleases(t *testing.T) {
	sites, fileCalls, funcCalls := discoverSlotAckSites(t)

	// Anti-vacuity: the walk must find the known universe, or a broken
	// matcher passes by finding nothing.
	if len(sites) < 7 {
		t.Fatalf("anti-vacuity: found %d StreamChanges call sites; want >= 7 — the AST matcher is broken", len(sites))
	}

	var unclassified []string
	for key, pos := range sites {
		mech, ok := slotAckReleaseRoster[key]
		if !ok {
			unclassified = append(unclassified, key+"  ("+pos+")")
			continue
		}
		file, _, _ := strings.Cut(key, "::")
		switch mech {
		case slotAckStreamer:
			if !funcCalls[key]["captureSlotAckReleaser"] {
				t.Errorf("%s is classified as a streamer site but never calls captureSlotAckReleaser — its "+
					"reader's slot would never be released and would retain source WAL for the life of the "+
					"stream (GC-41)", key)
			}
			// A warm resume meets the reader's SLOT-ACKED-PAST-TARGET-POSITION
			// door; the operator's acknowledgement must reach it on every
			// such path, or --accept-slot-acked-past-position works on one
			// resume path and silently not on another.
			if strings.Contains(strings.ToLower(key), "warmresume") && !funcCalls[key]["wireSlotAckedPastAcceptance"] {
				t.Errorf("%s is a warm-resume site but never calls wireSlotAckedPastAcceptance — the operator's "+
					"--accept-slot-acked-past-position cannot reach its reader (GC-41 MEDIUM-1)", key)
			}
		case slotAckChain:
			if !fileCalls[file]["releaseChainAckTo"] {
				t.Errorf("%s is classified as a backup-chain site but %s never calls releaseChainAckTo — "+
					"the chain's slot would never advance (GC-41)", key, file)
			}
		}
	}
	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("StreamChanges call site(s) not classified in slotAckReleaseRoster:\n  %s\n\n"+
			"A Postgres reader never acks its slot past what is released to it (GC-41). Classify each site "+
			"by the mechanism that releases DURABLE positions to its reader — slotAckStreamer (call "+
			"captureSlotAckReleaser; the apply-phase sidecar releases the target's persisted position) or "+
			"slotAckChain (release each committed window via releaseChainAckTo).",
			strings.Join(unclassified, "\n  "))
	}

	for key := range slotAckReleaseRoster {
		if _, ok := sites[key]; !ok {
			t.Errorf("stale roster entry %q: no StreamChanges call there any more — remove it or fix the key", key)
		}
	}

	// The streamer mechanism is two halves: capture at the site, and the
	// sidecar started in the apply phase. A capture nobody consumes releases
	// nothing.
	if !funcCalls["streamer_run_phases.go::(*Streamer).phaseStartApplySidecars"]["startSlotAckCeiling"] {
		t.Error("phaseStartApplySidecars no longer starts startSlotAckCeiling: every captured slot-ack " +
			"releaser is inert and a Postgres source's slot never advances on a sync (GC-41)")
	}
}

// discoverSlotAckSites returns every StreamChanges call site ("<relpath>::<func>"
// → position), plus, per file and per function, the set of called selector /
// identifier names — the evidence the classifications are checked against.
func discoverSlotAckSites(t *testing.T) (sites map[string]string, fileCalls, funcCalls map[string]map[string]bool) {
	t.Helper()
	sites = map[string]string{}
	fileCalls = map[string]map[string]bool{}
	funcCalls = map[string]map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range slotAckRosterScanDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir %q: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			rel := name
			if dir != "." {
				rel = filepath.ToSlash(filepath.Join(dir, name))
			}
			f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatalf("parse %q: %v", name, err)
			}
			fileCalls[rel] = map[string]bool{}
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				key := rel + "::" + funcDeclName(fd)
				calls := map[string]bool{}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					var callee string
					switch fn := call.Fun.(type) {
					case *ast.SelectorExpr:
						callee = fn.Sel.Name
					case *ast.Ident:
						callee = fn.Name
					}
					if callee == "" {
						return true
					}
					calls[callee] = true
					fileCalls[rel][callee] = true
					if callee == "StreamChanges" {
						if _, dup := sites[key]; !dup {
							sites[key] = fset.Position(call.Pos()).String()
						}
					}
					return true
				})
				funcCalls[key] = calls
			}
		}
	}
	return sites, fileCalls, funcCalls
}
