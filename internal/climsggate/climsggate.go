// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package climsggate is the gate for one class of operator-facing defect:
// a message sluice EMITS AT RUNTIME names a command, subcommand or flag the
// binary does not have.
//
// # Why this exists
//
// `sluice sync start --resume` appeared in nine production string literals —
// a schema-race recovery hint the Postgres CDC reader appends to every
// incompatible-DDL refusal, the schema-forward refusal ADR-0157 pipes into
// webhook/Slack/email drift notifications, two Shape A drained-model hints,
// the add-table completion log, and two kong help strings. `sync start` has
// no `--resume` and never had one; it warm-resumes by DEFAULT when
// re-invoked with the same `--stream-id`. So sluice paged an operator
// mid-incident and handed them a command line that exits 80 with
// `unknown flag --resume`.
//
// The doc instances of the same slip were swept twice (roadmap item 53 in
// 2026-07, an ADR pass in 2026-08) and the code instances survived both,
// because nothing could see them: a Go string literal is not prose a
// docs-drift pass reads, and the compiler has no opinion about its contents.
//
// # Why this gate can be fail-by-default with NO exemption list
//
// The same walk over `docs/` would need one. An ADR legitimately describes a
// surface that is planned and unbuilt — ADR-0054's `sluice schema
// migrate-shape-a` is correctly labelled Phase 3+ — so a docs gate would
// need a curated allowlist, which is the anti-pattern this project keeps
// paying for.
//
// A RUNTIME message has no such case. There is no reason for the binary to
// tell an operator to run something the binary does not implement. So this
// gate carries no exemptions, and an addition to it is always a fix.
//
// # What it grades, and what it deliberately does not (the false-positive rule)
//
// A string literal is not a command line, and most of the tree's mentions of
// `sluice` are prose. Three shapes must not fire:
//
//   - prose that happens to name a command — "sluice will repair it",
//     "sluice trigger engine is partially uninstalled";
//   - a flag-like token that is not a flag reference — a bare `--`
//     separator, a `-n` short form, a `%s` placeholder;
//   - a message that deliberately names a REMOVED flag to tell the operator
//     it is gone.
//
// The rule that separates them is ANCHORING plus DELIMITERS, and it is the
// load-bearing design choice here:
//
//  1. A span is graded only if it starts at the literal word `sluice` AND
//     its first following word resolves to a real kong command. "sluice will
//     repair it" resolves nothing and is dropped whole.
//  2. If the `sluice` was opened by a quote or backtick — the way this
//     codebase writes copy-pasteable commands — the span ENDS at the
//     author's own closing delimiter. That is what keeps
//     schemaRaceRecoveryHint's trailing prose mention of
//     `--forward-schema-add-column` out of the `sluice sync start …` span:
//     a flag named in prose, outside an invocation, is a flag REFERENCE, and
//     a reference is exactly how you tell an operator a flag was removed.
//     Grading only invocations is what makes the no-exemptions posture
//     honest.
//  3. An undelimited span ends at the first token that neither extends the
//     command path nor is a flag or a value, so prose after a bare mention
//     is never swept in.
//
// Unknown SUBCOMMANDS are graded more narrowly still: only inside a
// delimited span, and only when the span also carries a flag. A flag is the
// proof that the author wrote an invocation rather than a sentence — without
// it, "sluice trigger engine is partially uninstalled" and "sluice schema
// migrate-shape-a" are the same shape and no rule can tell them apart.
//
// # Stated limits
//
//   - Short flags (`-n`) are not graded; only `--long` forms.
//   - A flag named in PROSE (outside a delimited invocation) is not graded.
//     That is deliberate — see (2) above — and it is where a removed-flag
//     notice belongs.
//   - A span whose command path is interpolated (`fmt.Sprintf("sluice %s
//     --x")`) cannot be resolved to one command, so its flags are graded
//     name-only against the union of every flag. That is weaker than the
//     (command, flag) keying the rest of the gate uses, and it is reported
//     as such.
//   - The gate grades what a literal SAYS, not whether the code path that
//     emits it is reachable.
package climsggate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Config describes one instantiation of the gate.
type Config struct {
	// Roots are directory trees to walk, recursively.
	Roots []string

	// Commands is every command PATH the binary accepts, space-joined:
	// "sync", "sync start", "backup verify". Derived from kong's model,
	// never from a hand-kept list.
	Commands map[string]bool

	// Flags maps a command path to every long flag name (without the `--`)
	// valid AT that path, including flags inherited from ancestor nodes and
	// the global set. The empty key holds the globals alone.
	Flags map[string]map[string]bool

	// AnyFlag is the union of every long flag name on any command. Used
	// only for spans whose command path is interpolated at runtime.
	AnyFlag map[string]bool

	// SkipDir reports whether a directory should not be walked (vendored
	// trees, worktrees, testdata).
	SkipDir func(path string) bool

	// MinSpans, MinCommandsNamed and MinFlagChecks are the anti-vacuity
	// floors. A walker that stops matching the real shape — a tokenizer
	// regression, a root that moved — goes quiet rather than red, so the
	// gate fails when it finds implausibly little.
	MinSpans         int
	MinCommandsNamed int
	MinFlagChecks    int
}

// finding is one graded span that named something the binary does not have.
type finding struct {
	pos  string
	span string
	why  string
}

// dynMark stands in for a non-literal operand of a string concatenation, so
// `"sluice sync start --stream-id " + id` keeps its token boundaries without
// pretending to know what `id` holds.
const dynMark = "\x00"

var (
	reCommandWord = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	reLongFlag    = regexp.MustCompile(`^--[a-z][a-z0-9-]+$`)
)

// Assert runs the gate.
func Assert(t *testing.T, cfg Config) {
	t.Helper()

	findings, spans, named, flagChecks, err := analyze(cfg)
	if err != nil {
		t.Fatalf("climsggate: %v", err)
	}

	switch {
	case spans < cfg.MinSpans:
		t.Fatalf("found only %d graded `sluice …` spans in Go string literals (floor %d) — the walker has "+
			"stopped matching the real shape and would now pass forever; re-point it", spans, cfg.MinSpans)
	case named < cfg.MinCommandsNamed:
		t.Fatalf("graded spans named only %d distinct commands (floor %d) — the command resolver is probably "+
			"broken; re-point it", named, cfg.MinCommandsNamed)
	case flagChecks < cfg.MinFlagChecks:
		t.Fatalf("graded only %d --flag tokens (floor %d) — the flag tokenizer is probably broken, which is "+
			"exactly the half of this gate that matters; re-point it", flagChecks, cfg.MinFlagChecks)
	}

	if len(findings) == 0 {
		return
	}

	var b strings.Builder
	for _, f := range findings {
		b.WriteString("\n  " + f.pos + "\n      message: " + f.span + "\n      problem: " + f.why)
	}
	t.Errorf("%d runtime message(s) name a CLI surface this binary does not have — an operator who follows "+
		"one gets `unknown flag` / `unexpected argument` and a nonzero exit, usually mid-incident:%s\n\n"+
		"There is no exemption list and there is not meant to be one: a runtime message has no business "+
		"naming an unbuilt command. Correct the message. If the flag is genuinely gone and the message is "+
		"TELLING the operator so, write it as prose outside the quoted invocation — this gate grades "+
		"invocations, not references.",
		len(findings), b.String())
}

// analyze is Assert's engine, split out so the walker's own tests can drive
// it against a fixture tree and assert on the findings — a gate whose only
// caller is a t.Errorf is a gate nobody can mutation-run.
func analyze(cfg Config) (out []finding, spans, commandsNamed, flagChecks int, err error) {
	named := map[string]bool{}
	var findings []finding

	for _, root := range cfg.Roots {
		werr := walkRoot(root, cfg, func(pos token.Position, text string) {
			for _, sp := range scanSpans(text, cfg) {
				spans++
				named[sp.path] = true
				at := filepath.ToSlash(pos.Filename) + ":" + strconv.Itoa(pos.Line)

				if sp.unknown != "" {
					findings = append(findings, finding{
						pos:  at,
						span: sp.render(),
						why:  "`sluice " + sp.path + " " + sp.unknown + "` — " + strconv.Quote(sp.unknown) + " is not a subcommand of `" + sp.path + "`",
					})
				}
				for _, f := range sp.flags {
					flagChecks++
					name := strings.TrimPrefix(f, "--")
					if sp.dynamicPath {
						if cfg.AnyFlag[name] {
							continue
						}
						findings = append(findings, finding{
							pos:  at,
							span: sp.render(),
							why:  "`" + f + "` is not a flag on ANY command (this span's command path is interpolated, so it is graded name-only)",
						})
						continue
					}
					if cfg.Flags[sp.path][name] {
						continue
					}
					findings = append(findings, finding{
						pos:  at,
						span: sp.render(),
						why:  "`" + f + "` is not a flag on `sluice " + sp.path + "`" + otherHomes(f, cfg),
					})
				}
			}
		})
		if werr != nil {
			return nil, 0, 0, 0, werr
		}
	}

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].pos != findings[j].pos {
			return findings[i].pos < findings[j].pos
		}
		return findings[i].why < findings[j].why
	})
	return findings, spans, len(named), flagChecks, nil
}

// otherHomes names the commands that DO have the flag, because the slip is
// almost always a real flag on a sibling command (`--resume` is real on
// `migrate` and `restore`, which is where the phantom came from).
func otherHomes(flag string, cfg Config) string {
	name := strings.TrimPrefix(flag, "--")
	var homes []string
	for path, set := range cfg.Flags {
		if path == "" || !set[name] {
			continue
		}
		// Report only the shallowest owners, so an inherited flag does not
		// list every descendant.
		homes = append(homes, path)
	}
	if len(homes) == 0 {
		return " (and on no other command either — it is not a flag at all)"
	}
	sort.Strings(homes)
	trimmed := homes[:0]
	for _, h := range homes {
		covered := false
		for _, other := range trimmed {
			if strings.HasPrefix(h, other+" ") {
				covered = true
				break
			}
		}
		if !covered {
			trimmed = append(trimmed, h)
		}
	}
	return " — it IS a flag on: " + strings.Join(trimmed, ", ")
}

// walkRoot parses every non-vendored Go file under root and hands each
// string literal (concatenations stitched) to visit.
func walkRoot(root string, cfg Config, visit func(token.Position, string)) error {
	fset := token.NewFileSet()
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if cfg.SkipDir != nil && cfg.SkipDir(path) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// _test.go is the ONE place this gate deliberately narrows, and it
		// is narrowed for a reason rather than for convenience: a test file
		// emits nothing to an operator, and — decisively — a gate's own
		// fixtures must be able to SPELL a phantom flag. Walking test files
		// would force an exemption list into a design whose whole claim is
		// that it does not need one. The cost is real and is stated here so
		// nobody reads the gate's name as broader than the truth: a wrong
		// command line living only in a test's doc string is not caught.
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			return perr
		}
		for _, lit := range collectLiterals(fset, f) {
			visit(lit.pos, lit.text)
		}
		return nil
	})
}

type literal struct {
	text string
	pos  token.Position
}

// collectLiterals returns every string literal in f, with `+` concatenations
// stitched into one text so a command line split across source lines is
// still seen whole. Non-literal operands become dynMark, which preserves
// token boundaries without inventing a value.
func collectLiterals(fset *token.FileSet, f *ast.File) []literal {
	var out []literal

	var inspect func(n ast.Node) bool
	inspect = func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.BinaryExpr:
			if e.Op != token.ADD {
				return true
			}
			var residual []ast.Expr
			text, sawString := flatten(e, &residual)
			if !sawString {
				return true
			}
			out = append(out, literal{text: text, pos: fset.Position(e.Pos())})
			// The concatenation is consumed as a unit; walk only the
			// non-literal operands so a Sprintf nested inside it is not lost.
			for _, r := range residual {
				ast.Inspect(r, inspect)
			}
			return false
		case *ast.BasicLit:
			if e.Kind != token.STRING {
				return true
			}
			if s, err := strconv.Unquote(e.Value); err == nil {
				out = append(out, literal{text: s, pos: fset.Position(e.Pos())})
			}
			return false
		}
		return true
	}
	ast.Inspect(f, inspect)
	return out
}

// flatten renders a `+` chain to text, collecting non-literal operands into
// residual. The bool reports whether any operand was a string literal.
func flatten(e ast.Expr, residual *[]ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.ParenExpr:
		return flatten(v.X, residual)
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			*residual = append(*residual, v)
			return " " + dynMark + " ", false
		}
		l, lok := flatten(v.X, residual)
		r, rok := flatten(v.Y, residual)
		return l + r, lok || rok
	case *ast.BasicLit:
		if v.Kind == token.STRING {
			if s, err := strconv.Unquote(v.Value); err == nil {
				return s, true
			}
		}
		return " " + dynMark + " ", false
	default:
		*residual = append(*residual, e)
		return " " + dynMark + " ", false
	}
}

// span is one graded `sluice …` invocation found in a literal.
type span struct {
	path        string
	words       []string
	flags       []string
	unknown     string
	dynamicPath bool
	delimited   bool
	// implied marks a span written without the binary name, the way kong
	// help strings do ('sync start --resume'). Reported as such so the
	// reader can find the text.
	implied bool
}

func (s span) render() string {
	name := "sluice"
	if s.implied {
		name = "(sluice)"
	}
	parts := append([]string{name, s.path}, s.flags...)
	return strings.Join(parts, " ")
}

// scanSpans finds every graded span in one literal's text.
//
// Two passes, because a runtime message writes an invocation two ways:
//
//	Pass 1 anchors on the binary name — `sluice sync start --resume`.
//	Pass 2 catches the form kong help strings use, where the binary name is
//	       implied by context: "… use the drained model: 'sync stop --wait'
//	       -> schema migrate -> 'sync start --resume'." Those are graded only
//	       when the delimited region BEGINS with a real top-level command and
//	       CARRIES a flag — two conditions together, because either alone
//	       matches ordinary quoted prose.
func scanSpans(text string, cfg Config) []span {
	var out []span

	for _, at := range wordIndexes(text, "sluice") {
		body, delimited := spanBody(text, at+len("sluice"), openerAt(text, at))
		if sp := gradeSpan(body, delimited, cfg); sp != nil {
			out = append(out, *sp)
		}
	}

	for _, region := range delimitedRegions(text) {
		first := firstWord(region)
		if first == "sluice" || !cfg.Commands[first] || !strings.Contains(region, "--") {
			continue
		}
		if sp := gradeSpan(region, true, cfg); sp != nil {
			sp.implied = true
			out = append(out, *sp)
		}
	}
	return out
}

// openerAt reports the quote-like character immediately preceding index i,
// which is what makes a span delimited.
func openerAt(text string, i int) byte {
	if i == 0 {
		return 0
	}
	switch c := text[i-1]; c {
	case '`', '\'', '"':
		return c
	}
	return 0
}

// spanBody returns the text a span covers, starting just after the binary
// name. A delimited span ends at the author's own closing delimiter — the
// boundary that keeps a following prose flag mention out of the invocation.
// The search is over CHARACTERS, not tokens: `sluice schema preview`'s output
// closes its backtick mid-token, and a token-wise search once ran that span
// on into the NEXT backticked group and reported three phantom findings.
// If the delimiter never closes, the span is treated as undelimited rather
// than swallowing the rest of the literal.
func spanBody(text string, from int, opener byte) (string, bool) {
	if opener == 0 {
		return text[from:], false
	}
	if k := strings.IndexByte(text[from:], opener); k >= 0 {
		return text[from : from+k], true
	}
	return text[from:], false
}

// gradeSpan resolves a span's command path and collects its flags. It
// returns nil when nothing resolves to a command at all — which is how prose
// that merely says the word "sluice" ("sluice will repair it") is dropped.
func gradeSpan(body string, delimited bool, cfg Config) *span {
	sp := span{delimited: delimited}
	for _, tok := range strings.Fields(body) {
		core := trimToken(tok)
		switch {
		case core == "":
			// Pure punctuation between tokens; carries nothing.
		case strings.Contains(core, dynMark):
			// Only an interpolation in the COMMAND-PATH region makes the path
			// unresolvable. Past the first flag the path is settled and the
			// dynamic operand is a flag VALUE — `"sluice sync start
			// --stream-id " + id` must still grade --stream-id against
			// `sync start`, not name-only against every flag in the binary.
			if len(sp.flags) == 0 {
				sp.dynamicPath = true
			}
		case flagName(core) != "":
			sp.flags = append(sp.flags, flagName(core))
		case reCommandWord.MatchString(core):
			cand := strings.TrimSpace(sp.path + " " + core)
			switch {
			case len(sp.flags) > 0 || sp.unknown != "":
				// Past the command path already; a bare word here is an
				// argument or prose, not a subcommand.
			case cfg.Commands[cand]:
				sp.path = cand
			case !delimited:
				// Undelimited: prose has resumed. Stop rather than reading
				// the rest of the sentence as arguments.
				return finish(sp)
			default:
				sp.unknown = core
			}
			sp.words = append(sp.words, core)
		default:
			if !delimited && !isValueToken(core) {
				return finish(sp)
			}
		}
	}
	return finish(sp)
}

// finish drops a span that resolved no command, and suppresses the
// unknown-subcommand finding for a span that carries no flag — without one
// there is no evidence the author wrote an invocation rather than a
// sentence, and "sluice trigger engine is partially uninstalled" is
// indistinguishable from "sluice schema migrate-shape-a".
func finish(sp span) *span {
	if sp.path == "" {
		return nil
	}
	if len(sp.flags) == 0 {
		sp.unknown = ""
	}
	return &sp
}

// wordIndexes returns the byte offsets at which word occurs as a whole word.
func wordIndexes(text, word string) []int {
	var out []int
	for i := 0; ; {
		k := strings.Index(text[i:], word)
		if k < 0 {
			return out
		}
		at := i + k
		if !isWordByte(byteAt(text, at-1)) && !isWordByte(byteAt(text, at+len(word))) {
			out = append(out, at)
		}
		i = at + len(word)
	}
}

// delimitedRegions returns the text inside each backtick/quote pair.
//
// Regions are allowed to NEST and overlap — scanning does not jump past a
// region it just emitted. That matters for the shape this pass exists for: a
// kong help string is itself a `"…"` region, so consuming it whole would
// hide every 'sync start --resume' quoted INSIDE it, which is exactly where
// two of the instances lived. Overlapping regions are cheap because pass 2
// grades a region only when it both begins with a real command and carries a
// flag.
func delimitedRegions(text string) []string {
	var out []string
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '`', '\'', '"':
			if k := strings.IndexByte(text[i+1:], text[i]); k >= 0 {
				out = append(out, text[i+1:i+1+k])
			}
		}
	}
	return out
}

func firstWord(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}
	return trimToken(fields[0])
}

func byteAt(s string, i int) byte {
	if i < 0 || i >= len(s) {
		return 0
	}
	return s[i]
}

func isWordByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

const (
	openDelims  = "`'\"([{"
	closeDelims = "`'\")]}.,;:!?"
)

// trimToken strips surrounding punctuation from one whitespace-delimited
// token.
func trimToken(tok string) string {
	i := 0
	for i < len(tok) && strings.IndexByte(openDelims, tok[i]) >= 0 {
		i++
	}
	j := len(tok)
	for j > i && strings.IndexByte(closeDelims, tok[j-1]) >= 0 {
		j--
	}
	return tok[i:j]
}

// flagName returns the long-flag name a token spells, or "" when the token
// is not a long flag. `--exclude-table=<parent>` and `--type-override
// users.id=binary_uuid` are flag references with their value attached, so
// the name is taken up to the first `=`; a bare `--` and a short `-n` are
// not flags.
func flagName(core string) string {
	name := core
	if i := strings.IndexByte(name, '='); i >= 0 {
		name = name[:i]
	}
	if !reLongFlag.MatchString(name) {
		return ""
	}
	return name
}

// isValueToken reports whether a token is an argument or placeholder rather
// than prose — `<ID>`, `NAME=VALUE`, `db.users`, `%s`, `--` on its own.
func isValueToken(core string) bool {
	if core == "" || core == "--" {
		return true
	}
	if strings.ContainsAny(core, "=/%<>\\") {
		return true
	}
	c := core[0]
	return c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-'
}
