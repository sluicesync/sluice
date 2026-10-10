// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package vttestserver

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestLayoutStaysBelowTheEphemeralRange pins the constants themselves: every
// port the layout may own sits below the default ephemeral floor, and the
// derived listeners are where vttestserver puts them relative to the base.
func TestLayoutStaysBelowTheEphemeralRange(t *testing.T) {
	if last := BasePort + PortSpan - 1; last >= EphemeralFloor {
		t.Fatalf("vttestserver layout %d-%d reaches into the ephemeral range (floor %d)", BasePort, last, EphemeralFloor)
	}
	if GRPCPort != BasePort+1 || MySQLPort != BasePort+3 {
		t.Fatalf("GRPCPort=%d MySQLPort=%d; vttestserver puts them at base+1 and base+3 (base %d)", GRPCPort, MySQLPort, BasePort)
	}
	if got := ContainerPort(MySQLPort); got != strconv.Itoa(BasePort+3)+"/tcp" {
		t.Fatalf("ContainerPort(MySQLPort) = %q", got)
	}
}

// TestCheckOutsideEphemeral covers the premise check in both directions: the
// Linux default and a lowered-but-clear range pass, a range that reaches the
// layout (or only its last port) refuses, and unparseable input refuses
// rather than passing.
func TestCheckOutsideEphemeral(t *testing.T) {
	last := BasePort + PortSpan - 1
	cases := []struct {
		name, proc string
		wantErr    bool
	}{
		{"linux default", "32768\t60999\n", false},
		{"lowered but clear of the layout", strconv.Itoa(last+1) + " 60999", false},
		{"entirely below the layout", "1024 " + strconv.Itoa(BasePort-1), false},
		{"covers the base", "1024 60999", true},
		{"touches only the last owned port", strconv.Itoa(last) + " 60999", true},
		{"touches only the base from below", "1024 " + strconv.Itoa(BasePort), true},
		{"empty", "", true},
		{"one number", "32768", true},
		{"not numbers", "a b", true},
		{"descending", "60999 32768", true},
	}
	for _, c := range cases {
		err := CheckOutsideEphemeral(c.proc)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: CheckOutsideEphemeral(%q) = %v, wantErr %v", c.name, c.proc, err, c.wantErr)
		}
	}
}

// containerPortLiteral matches a testcontainers port spelling, "33577/tcp".
var containerPortLiteral = regexp.MustCompile(`^(\d+)(/(tcp|udp))?$`)

// TestEveryVTTestServerHarnessUsesTheSharedLayout walks every Go file in the
// module (build tags do not matter to the parser, so the integration- and
// vstream-tagged harnesses are seen) and, for each file that boots the
// vitess/vttestserver image — identified by a string literal naming it —
// requires that it:
//
//   - takes its base port from this package (references vttestserver.BasePort),
//     so the harnesses cannot drift apart; and
//   - spells no integer or "N/tcp" literal inside the default ephemeral range,
//     so a harness cannot keep the shared import and still hard-code 33574.
//
// Scope, stated: Go files only. No workflow or shell script boots vttestserver
// today (they only pre-pull the image); the Vitess cluster suites boot their
// own docker-compose topology, not this image, and their ports (15xxx) are
// not this gate's concern. Anti-vacuity: at least two booting harnesses must
// be found, which is how many exist.
func TestEveryVTTestServerHarnessUsesTheSharedLayout(t *testing.T) {
	root := moduleRoot(t)
	var harnesses []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if !strings.Contains(string(src), "vitess/vttestserver") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		if path.Dir(filepath.ToSlash(rel)) == "internal/vttestserver" {
			return nil // this gate names the image and the range itself; it boots nothing
		}
		if checkHarness(t, filepath.ToSlash(rel), src) {
			harnesses = append(harnesses, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(harnesses) < 2 {
		t.Fatalf("found %d vttestserver harnesses (%v), want >= 2 — the walk or the image match broke; "+
			"refusing to pass vacuously", len(harnesses), harnesses)
	}
}

// checkHarness reports whether src boots vttestserver (a string literal names
// the image) and, if so, records a failure for each way it escapes the shared
// layout.
func checkHarness(t *testing.T, rel string, src []byte) bool {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		t.Errorf("%s: parse: %v", rel, err)
		return false
	}
	boots, usesBase := false, false
	var bad []string
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.BasicLit:
			var digits string
			switch n.Kind {
			case token.INT:
				digits = n.Value
			case token.STRING:
				s, err := strconv.Unquote(n.Value)
				if err != nil {
					return true
				}
				if strings.HasPrefix(s, "vitess/vttestserver") {
					boots = true
				}
				if m := containerPortLiteral.FindStringSubmatch(s); m != nil {
					digits = m[1]
				}
			}
			if v, err := strconv.Atoi(digits); err == nil && v >= EphemeralFloor && v <= EphemeralCeiling {
				bad = append(bad, fset.Position(n.Pos()).String()+": "+n.Value)
			}
		case *ast.SelectorExpr:
			if x, ok := n.X.(*ast.Ident); ok && x.Name == "vttestserver" && n.Sel.Name == "BasePort" {
				usesBase = true
			}
		}
		return true
	})
	if !boots {
		return false
	}
	if !usesBase {
		t.Errorf("%s boots vitess/vttestserver but never references vttestserver.BasePort — pass the shared "+
			"layout from internal/vttestserver as the image's PORT, so the harnesses cannot drift apart", rel)
	}
	for _, b := range bad {
		t.Errorf("%s: a port-shaped literal inside the ephemeral range %d-%d in a vttestserver harness — an "+
			"outbound connection inside the container can take that port before vtcombo binds it; use the "+
			"internal/vttestserver constants", b, EphemeralFloor, EphemeralCeiling)
	}
	return true
}

// moduleRoot walks up from the test's working directory to the go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod found walking up from the test directory")
		}
		dir = parent
	}
}
