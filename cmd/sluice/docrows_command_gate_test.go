// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The published-remedy half of the runtime-message CLI-surface gate (Bug 230).
//
// hint_command_gate_test.go grades the hint REGISTRY: each per-command text is
// held to the flags its command accepts, and the neutral text to every
// command. That is what fixed Bug 230 at runtime. The PUBLISHED row for the
// same code — internal/sluicecode/docrows.go, mirrored byte-for-byte into
// docs/operator/error-codes.md — kept the pre-fix remedy ("continue with
// `--resume`") for ~40 more releases, because the registry gate's universe
// was the registry, and climsggate grades invocations, not bare prose flags
// (gap census 2026-09-22, GC-18). An operator of three of the four commands
// that raise SLUICE-E-BULKCOPY-TABLE-FAILED looked the code up and was
// handed a flag that exits 80.
//
// # How the raising-command set is derived
//
// A doc row is read by whoever looks the code up, so its remedy has to be
// runnable by every command that can raise the code. That set is stated
// nowhere as a list, and this gate does not invent one: it takes the
// registry's per-command overrides ([migcore.HintTexts]) as the KNOWN raisers
// — a code with overrides for `migrate`, `sync start`, `sync run` and
// `schema add-table` is raised by at least those four — and grades every
// other code's flags for existence only. The reach is therefore exactly the
// registry's per-command knowledge, no wider; a code raised directly by an
// engine with no registry entry is graded only against the union of all
// flags, which catches a phantom flag and nothing else. Stated so this file's
// name cannot be read as broader than the truth.
//
// # The three arms
//
//   - SPAN: a flag inside the same backticked span as a sluice invocation
//     (`sluice sync decommission --stream-id <id> --yes`) is graded against
//     that command. Exact, no heuristic.
//   - SENTENCE: a bare flag is graded against the nearest LEAF command
//     mentioned earlier in the same sentence ("On `sluice migrate`, use
//     `--resume` …"), which is how one remedy names different flags for
//     different commands. Sentences end at ". " and "; ". This is a reading
//     heuristic; a remedy it misreads is reworded into the SPAN form
//     (`migrate --resume`), which is clearer for the operator anyway.
//   - SET: a bare flag with no command in its sentence, on a code with
//     registry overrides, must exist on EVERY known raiser; otherwise it must
//     exist on SOME command.
//
// A flag inside a backticked invocation of ANOTHER binary (`pg_dump
// --section=post-data`) is that tool's and is not graded; another tool's flag
// named in bare prose (`--log-replica-updates` is mysqld's) is listed in
// foreignToolFlags with the tool named. The docs flag gate
// (docs_flag_names_test.go) shares that map and fails an entry no doc still
// mentions, so it cannot rot in either direction.
package main

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/climsggate"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// foreignToolFlags maps a flag named in bare prose to the tool that owns it.
// Shared with TestDocsNameOnlyRealFlags, which enforces that every entry is
// still mentioned somewhere in the operator docs.
var foreignToolFlags = map[string]string{
	"binlog-do-db":                  "mysqld",
	"binlog-ignore-db":              "mysqld",
	"log-replica-updates":           "mysqld",
	"server-id":                     "mysqld",
	"default-authentication-plugin": "mysqld",
	"enable-binlog-dump":            "vttablet",
	"binlog-dump-authorized-users":  "vttablet",
	"dbhost":                        "bucardo",
	"dbport":                        "bucardo",
	"dbuser":                        "bucardo",
	"follow":                        "pgcopydb (a `--follow`-style continuous mode, named as a comparison)",
	"table-jobs":                    "pgcopydb",
	"index-jobs":                    "pgcopydb",
	"split-tables-larger-than":      "pgcopydb",
	"wheres":                        "pscale database dump",
	"role":                          "pscale password create",
	"build-empty-files":             "mydumper",
	"tab":                           "mysqldump",
	"compress":                      "pg_dump",
	"database-flags":                "gcloud sql instances patch",
	"retained-transaction-log-days": "gcloud sql instances patch",
	"server-ca-mode":                "gcloud sql instances patch",
	"enable-bin-log":                "gcloud sql instances patch",
	"no-enable-bin-log":             "gcloud sql instances patch",
	"name":                          "az mysql flexible-server parameter set (the `--name … --value …` pair, quoted as a fragment)",
	"value":                         "az mysql flexible-server parameter set",
	"enable-rbac-authorization":     "az keyvault create",
	"rotation-period":               "gcloud kms keys create",
}

var (
	// remedySpanRe matches a backticked span; the command-context and
	// foreign-tool rules both key on a span's first word.
	remedySpanRe = regexp.MustCompile("`([^`]+)`")
	// remedySentenceRe splits a remedy into the sentences the SENTENCE arm
	// scopes to.
	remedySentenceRe = regexp.MustCompile(`\. |; `)
)

func TestDocRowRemediesNameOnlyFlagsTheirRaisingCommandsAccept(t *testing.T) {
	commands, flags, anyFlag := cliSurface(t)
	topLevel, leaf := commandTree(commands)

	raising := map[sluicecode.Code]map[string]bool{}
	for _, ht := range migcore.HintTexts() {
		if ht.Command == "" {
			continue
		}
		if raising[ht.Code] == nil {
			raising[ht.Code] = map[string]bool{}
		}
		raising[ht.Code][string(ht.Command)] = true
	}

	rows := sluicecode.DocRows()
	codes := make([]string, 0, len(rows))
	for c := range rows {
		codes = append(codes, string(c))
	}
	sort.Strings(codes)

	var graded, spanGraded, setGraded int
	for _, code := range codes {
		set := raising[sluicecode.Code(code)]
		remedy := rows[sluicecode.Code(code)].Remedy
		grade := func(name, context string) {
			graded++
			switch {
			case context != "":
				if !flags[context][name] {
					t.Errorf("%s: remedy names --%s after mentioning `sluice %s` in the same sentence, which has no such flag.\n  remedy: %s",
						code, name, context, remedy)
				}
			case len(set) > 0:
				setGraded++
				var missing []string
				for cmd := range set {
					if !flags[cmd][name] {
						missing = append(missing, "`sluice "+cmd+"`")
					}
				}
				if len(missing) > 0 {
					sort.Strings(missing)
					t.Errorf("%s: remedy names --%s with no command in its sentence, but the code is raised by %s, which %s no such flag. "+
						"Name the command the flag belongs to (\"On `sluice migrate`, …\" or `migrate --resume`) or describe the remedy in words.\n  remedy: %s",
						code, name, strings.Join(missing, ", "), plural(len(missing), "has", "have"), remedy)
				}
			default:
				if _, foreign := foreignToolFlags[name]; !foreign && !anyFlag[name] {
					t.Errorf("%s: remedy names --%s, which is not a flag on any sluice command.\n  remedy: %s", code, name, remedy)
				}
			}
		}

		// Walk the spans in order; the prose between them is where a
		// sentence can end, so it is split there and never inside a span
		// (an invocation's `--source=...` carries ". " too).
		context := ""
		gradeProse := func(text string) {
			for i, piece := range remedySentenceRe.Split(text, -1) {
				if i > 0 {
					context = ""
				}
				for _, name := range climsggate.BareFlags(piece) {
					grade(name, context)
				}
			}
		}
		pos := 0
		for _, loc := range remedySpanRe.FindAllStringSubmatchIndex(remedy, -1) {
			gradeProse(remedy[pos:loc[0]])
			span := remedy[loc[2]:loc[3]]
			pos = loc[1]
			cmd := commandPathOfSpan(span, commands, topLevel)
			switch {
			case cmd != "":
				// An invocation: its own flags are graded against it
				// exactly, and it becomes the sentence's context if it is
				// a leaf (a group like `sync` carries no command flags
				// and would only produce noise).
				for _, name := range climsggate.BareFlags(span) {
					spanGraded++
					grade(name, cmd)
				}
				if leaf[cmd] {
					context = cmd
				}
				continue
			case isForeignInvocation(span):
				continue
			}
			for _, name := range climsggate.BareFlags(span) {
				grade(name, context)
			}
		}
		gradeProse(remedy[pos:])
	}

	// Anti-vacuity floors, sized under today's counts (measured 2026-09-22
	// and logged below: 118 rows, 199 graded flag tokens, 38 inside an
	// invocation span, 4 against a registry raising set — the SET arm is
	// small by construction, because a remedy for a per-command code names
	// its commands and so is mostly graded by the SENTENCE arm) so growth
	// never trips them while a tokenizer or walk that stopped seeing the
	// rows does.
	t.Logf("graded %d flag tokens across %d rows: %d inside invocation spans, %d against a registry raising set", graded, len(rows), spanGraded, setGraded)
	switch {
	case len(rows) < 80:
		t.Fatalf("sluicecode.DocRows() returned only %d rows (floor 80) — the mirror walk is broken", len(rows))
	case graded < 40:
		t.Fatalf("only %d bare --flag tokens graded across %d remedies (floor 40) — the tokenizer is probably broken", graded, len(rows))
	case spanGraded < 10:
		t.Fatalf("only %d flags graded inside an invocation span (floor 10) — the span resolver stopped matching", spanGraded)
	case setGraded < 3:
		t.Fatalf("only %d flags graded against a registry raising set (floor 3) — the per-command derivation from migcore.HintTexts stopped working, and the SET arm is vacuous", setGraded)
	}
}

// commandTree returns the top-level command words and the set of LEAF
// command paths (those with no subcommand), both derived from cliSurface's
// command map.
func commandTree(commands map[string]bool) (topLevel, leaf map[string]bool) {
	topLevel = map[string]bool{}
	leaf = map[string]bool{}
	for cmd := range commands {
		topLevel[strings.Fields(cmd)[0]] = true
		leaf[cmd] = true
	}
	for cmd := range commands {
		for parent := range commands {
			if strings.HasPrefix(cmd, parent+" ") {
				delete(leaf, parent)
			}
		}
	}
	return topLevel, leaf
}

// commandPathOfSpan resolves a backticked span to the kong command path it
// invokes — `sluice sync start --x` and the bare `sync start` both resolve to
// "sync start". Prose that merely begins with a command word does not
// resolve unless the whole leading path is a real command.
func commandPathOfSpan(span string, commands, topLevel map[string]bool) string {
	words := strings.Fields(span)
	if len(words) > 0 && words[0] == "sluice" {
		words = words[1:]
	}
	if len(words) == 0 || !topLevel[words[0]] {
		return ""
	}
	best := ""
	for i := 1; i <= len(words) && i <= 3; i++ {
		if strings.HasPrefix(words[i-1], "-") {
			break
		}
		if p := strings.Join(words[:i], " "); commands[p] {
			best = p
		}
	}
	return best
}

// isForeignInvocation reports whether a span is another binary's command line
// (`pg_dump --section=post-data`): a leading word that is not a flag, and at
// least one flag after it.
func isForeignInvocation(span string) bool {
	words := strings.Fields(span)
	return len(words) > 1 && !strings.HasPrefix(words[0], "-") && strings.Contains(span, " --")
}
