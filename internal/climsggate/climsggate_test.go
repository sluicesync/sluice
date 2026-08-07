// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package climsggate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testSurface is a miniature of the real kong surface — enough shape to
// exercise every rule without depending on which flags sluice happens to
// carry this week.
func testSurface() Config {
	commands := map[string]bool{
		"migrate": true, "restore": true, "verify": true, "trigger": true,
		"trigger setup": true, "schema": true, "schema preview": true,
		"sync": true, "sync start": true, "sync stop": true,
	}
	flags := map[string]map[string]bool{
		"":               {"log-level": true},
		"migrate":        {"log-level": true, "resume": true, "where": true, "exclude-table": true},
		"restore":        {"log-level": true, "include-table": true},
		"verify":         {"log-level": true, "depth": true},
		"trigger":        {"log-level": true},
		"trigger setup":  {"log-level": true, "tables": true},
		"schema":         {"log-level": true},
		"schema preview": {"log-level": true, "type-override": true},
		"sync":           {"log-level": true},
		"sync start":     {"log-level": true, "stream-id": true, "where": true, "restart-from-scratch": true},
		"sync stop":      {"log-level": true, "wait": true},
	}
	anyFlag := map[string]bool{}
	for _, set := range flags {
		for k := range set {
			anyFlag[k] = true
		}
	}
	return Config{Commands: commands, Flags: flags, AnyFlag: anyFlag}
}

// TestScanSpans_FalsePositiveRule is the pin on the half of this gate that
// decides whether it is usable at all. A gate with no exemption list earns
// that posture only by not firing on prose, and every "must not fire" case
// below is a shape taken from the tree the gate ships against.
func TestScanSpans_FalsePositiveRule(t *testing.T) {
	cfg := testSurface()

	cases := []struct {
		name string
		text string
		// wantFlags are the flag tokens the scanner must attribute to a
		// command; wantPaths the command paths it must resolve; wantUnknown
		// the subcommand words it must report.
		wantPaths   []string
		wantFlags   []string
		wantUnknown []string
	}{
		// --- must fire ---------------------------------------------------
		{
			name:      "the motivating instance",
			text:      "(3) `sluice sync start --resume` to continue from the last applied LSN.",
			wantPaths: []string{"sync start"},
			wantFlags: []string{"--resume"},
		},
		{
			name:      "single-quoted invocation",
			text:      "then resume via 'sluice sync start --resume'. Set --schema-changes=refuse to keep it.",
			wantPaths: []string{"sync start"},
			wantFlags: []string{"--resume"},
		},
		{
			name:      "binary name implied, the kong help-string form",
			text:      "use the drained model: 'sync stop --wait' -> schema migrate -> 'sync start --resume'.",
			wantPaths: []string{"sync stop", "sync start"},
			wantFlags: []string{"--wait", "--resume"},
		},
		{
			name:        "unknown subcommand, proven an invocation by its flag",
			text:        "run `sluice schema migrate-shape-a --type-override x`",
			wantPaths:   []string{"schema"},
			wantFlags:   []string{"--type-override"},
			wantUnknown: []string{"migrate-shape-a"},
		},
		{
			name:      "a flag that is real on a SIBLING command still resolves per-command",
			text:      "then `sluice migrate --schema-already-applied` for the data",
			wantPaths: []string{"migrate"},
			wantFlags: []string{"--schema-already-applied"},
		},

		// --- must NOT fire -----------------------------------------------
		{
			name: "prose whose next word is not a command",
			text: "digits; sluice will repair it via a post-copy exact SQL re-read before CDC begins",
		},
		{
			name: "prose after a real command word, no flag, quoted (a SQL message)",
			text: "MESSAGE = 'sluice trigger engine is partially uninstalled (x missing); DDL blocked'",
			// `trigger` resolves, but with no flag in the span there is no
			// evidence this is an invocation, so "engine" is NOT reported.
			wantPaths: []string{"trigger"},
		},
		{
			name:      "a progress header",
			text:      "sluice migrate - complete",
			wantPaths: []string{"migrate"},
		},
		{
			name: "a flag named in PROSE after the invocation closes",
			text: "(3) `sluice sync start --stream-id X` to continue. " +
				"For ADD COLUMN only, opt-in via --forward-schema-add-column (ADR-0058).",
			wantPaths: []string{"sync start"},
			// --forward-schema-add-column is outside the backticks: a
			// REFERENCE, not an invocation. This is the shape that lets a
			// message say a flag was REMOVED without tripping the gate.
			wantFlags: []string{"--stream-id"},
		},
		{
			name: "a closing backtick glued to a possessive must not leak into the next group",
			text: "alter its shape to match `sluice schema preview`'s output; " +
				"`--type-override x` is unrelated prose",
			wantPaths: []string{"schema preview"},
			// This exact text produced three phantom findings while the span
			// boundary was searched token-wise instead of character-wise.
		},
		{
			name:      "a bare -- separator and a short flag are not flags",
			text:      "`sluice migrate -- -n --exclude-table=t`",
			wantPaths: []string{"migrate"},
			wantFlags: []string{"--exclude-table=t"[:len("--exclude-table")]},
		},
		{
			name: "a quoted region that is not a command at all",
			text: "Format: 'TABLE.COLUMN=TYPE', e.g. 'users.id=binary_uuid' with --type-override",
		},
		{
			name: "the word sluice inside an identifier",
			text: "the target's sluice_cdc_state row and sluicesync.dev/sluice/internal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spans := scanSpans(tc.text, cfg)

			var paths, flags, unknown []string
			for _, sp := range spans {
				paths = append(paths, sp.path)
				flags = append(flags, sp.flags...)
				if sp.unknown != "" {
					unknown = append(unknown, sp.unknown)
				}
			}
			assertSet(t, "paths", paths, tc.wantPaths)
			assertSet(t, "flags", flags, tc.wantFlags)
			assertSet(t, "unknown subcommands", unknown, tc.wantUnknown)
		})
	}
}

func assertSet(t *testing.T, label string, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("%s = %v; want %v", label, got, want)
	}
}

// TestCollectLiterals_StitchesConcatenations pins that a command line split
// across `+` operands is still seen whole, and that a non-literal operand
// becomes a boundary rather than fusing two tokens into one.
func TestCollectLiterals_StitchesConcatenations(t *testing.T) {
	dir := t.TempDir()
	src := "package p\n\nimport \"fmt\"\n\nfunc f(id string) string {\n" +
		"\treturn fmt.Sprintf(\"resume via 'sluice sync start --resume \" +\n" +
		"\t\t\"--stream-id \" + id + \"'\")\n}\n"
	write(t, dir, "a.go", src)

	cfg := testSurface()
	cfg.Roots = []string{dir}
	findings, spans, _, flagChecks, err := analyze(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if spans != 1 {
		t.Fatalf("spans = %d; want 1 (the concatenation must be stitched into ONE span)", spans)
	}
	if flagChecks != 2 {
		t.Fatalf("flagChecks = %d; want 2 (--resume and --stream-id, both across operand boundaries)", flagChecks)
	}
	if len(findings) != 1 || !strings.Contains(findings[0].why, "--resume") {
		t.Fatalf("findings = %+v; want exactly the --resume phantom", findings)
	}
}

// TestAnalyze_AntiVacuityFloorsAreReachable pins that the floors report the
// real counts, so a walker that goes quiet trips them rather than passing.
func TestAnalyze_AntiVacuityFloorsAreReachable(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "b.go", "package p\n\nconst m = \"only prose here, no commands\"\n")

	cfg := testSurface()
	cfg.Roots = []string{dir}
	_, spans, commands, flagChecks, err := analyze(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if spans != 0 || commands != 0 || flagChecks != 0 {
		t.Fatalf("a prose-only tree produced spans=%d commands=%d flags=%d; want all zero — "+
			"the floors are what turn that into a failure at the Assert layer", spans, commands, flagChecks)
	}
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
