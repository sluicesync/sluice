// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sluicecode

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestWrapAndFromError pins the wrapping contract: FromError extracts
// the CodedError through further fmt.Errorf wrapping, the message is
// byte-identical to the wrapped error's (presentation-free), and the
// chain below stays traversable for errors.Is.
func TestWrapAndFromError(t *testing.T) {
	sentinel := errors.New("sentinel: zero date")
	coded := Wrap(CodeValueZeroDate, "pass --zero-date=null", fmt.Errorf("mysql: column %q: %w", "d", sentinel))
	outer := fmt.Errorf("pipeline: copy table %q: %w", "t", coded)

	ce, ok := FromError(outer)
	if !ok {
		t.Fatal("FromError did not find the CodedError through the outer wrap")
	}
	if ce.Code != CodeValueZeroDate {
		t.Errorf("Code = %q; want %q", ce.Code, CodeValueZeroDate)
	}
	if ce.Hint != "pass --zero-date=null" {
		t.Errorf("Hint = %q; want the construction-site hint", ce.Hint)
	}
	if coded.Error() != ce.Err.Error() {
		t.Errorf("Error() must delegate to the wrapped error: %q vs %q", coded.Error(), ce.Err.Error())
	}
	if !errors.Is(outer, sentinel) {
		t.Error("errors.Is must traverse through the CodedError to the sentinel")
	}
}

// TestWrapNil ensures nil-in-nil-out so construction sites can wrap
// inline without a guard.
func TestWrapNil(t *testing.T) {
	if got := Wrap(CodeValueNULByte, "hint", nil); got != nil {
		t.Errorf("Wrap(_, _, nil) = %v; want nil", got)
	}
}

// TestFromErrorUncoded confirms a plain error chain yields no code.
func TestFromErrorUncoded(t *testing.T) {
	if _, ok := FromError(fmt.Errorf("outer: %w", errors.New("inner"))); ok {
		t.Error("FromError found a CodedError in an uncoded chain")
	}
	if _, ok := FromError(nil); ok {
		t.Error("FromError(nil) reported a CodedError")
	}
}

// TestExitCodeByClass pins the exit-code mapping: refusal-class codes
// exit ExitRefusal, runtime-class codes keep the traditional
// ExitFailure, and an unregistered code degrades to ExitFailure. Every
// REGISTERED code is exercised (not one representative per class) so a
// registry edit that flips a class shows up here.
func TestExitCodeByClass(t *testing.T) {
	for _, c := range All() {
		info, _ := Describe(c)
		want := ExitFailure
		if info.Class == ClassRefusal {
			want = ExitRefusal
		}
		ce := &CodedError{Code: c, Err: errors.New("x")}
		if got := ce.ExitCode(); got != want {
			t.Errorf("%s: ExitCode() = %d; want %d (class %d)", c, got, want, info.Class)
		}
	}
	unregistered := &CodedError{Code: Code("SLUICE-E-NOT-A-CODE"), Err: errors.New("x")}
	if got := unregistered.ExitCode(); got != ExitFailure {
		t.Errorf("unregistered code ExitCode() = %d; want %d", got, ExitFailure)
	}
}

// TestConfigErrorExit pins the config-error exit shape: exit 2, message
// delegation, and Unwrap traversal.
func TestConfigErrorExit(t *testing.T) {
	inner := errors.New("config: load \"x.yaml\": no such file")
	ce := &ConfigError{Err: inner}
	if got := ce.ExitCode(); got != ExitConfig {
		t.Errorf("ExitCode() = %d; want %d", got, ExitConfig)
	}
	if ce.Error() != inner.Error() {
		t.Errorf("Error() = %q; want delegation to %q", ce.Error(), inner.Error())
	}
	if !errors.Is(ce, inner) {
		t.Error("errors.Is must traverse ConfigError.Unwrap")
	}
}

// TestAttrs pins the slog-attr helper: a coded chain yields exactly the
// code and hint attrs, an uncoded chain yields nil (so call sites can
// append unconditionally).
func TestAttrs(t *testing.T) {
	coded := fmt.Errorf("outer: %w", Wrap(CodeValueNULByte, "use --type-override", errors.New("NUL byte")))
	attrs := Attrs(coded)
	if len(attrs) != 2 {
		t.Fatalf("Attrs = %d entries; want 2", len(attrs))
	}
	code, ok := attrs[0].(slog.Attr)
	if !ok || code.Key != "code" || code.Value.String() != string(CodeValueNULByte) {
		t.Errorf("attrs[0] = %v; want code=%s", attrs[0], CodeValueNULByte)
	}
	hint, ok := attrs[1].(slog.Attr)
	if !ok || hint.Key != "hint" || hint.Value.String() != "use --type-override" {
		t.Errorf("attrs[1] = %v; want hint=%q", attrs[1], "use --type-override")
	}
	if got := Attrs(errors.New("plain")); got != nil {
		t.Errorf("Attrs(uncoded) = %v; want nil", got)
	}
}

// TestRegistryDocSync enforces the docs/operator/error-codes.md table
// against the registry IN BOTH DIRECTIONS: every registered code has a
// doc row, and every SLUICE-E-… token in the doc is a registered code.
// This is the run-filter-guard lesson — a doc that must stay in sync
// with code gets a test, not a convention.
func TestRegistryDocSync(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	doc := string(raw)

	documented := map[Code]bool{}
	for _, m := range regexp.MustCompile(`SLUICE-E-[A-Z0-9-]+`).FindAllString(doc, -1) {
		documented[Code(m)] = true
	}

	for _, c := range All() {
		if !documented[c] {
			t.Errorf("registered code %s has no row in %s", c, docPath)
		}
	}
	for c := range documented {
		if _, ok := Describe(c); !ok {
			t.Errorf("%s documents %s, which is not a registered code", docPath, c)
		}
	}
}

// retainedButUnemittedMarker is the sentinel a registry summary carries
// when a code's refusal has been LIFTED but the string is kept registered
// (removing a published catalog code is breaking). It couples the registry
// prose to the doc prose in TestRegistryDocSync_RetainedProse.
const retainedButUnemittedMarker = "RETAINED-BUT-UNEMITTED"

// TestRegistryDocSync_RetainedProse extends the token-only sync check
// (TestRegistryDocSync) to compare row PROSE against the registry — the F7
// (audit 2026-07-17) gate. TestRegistryDocSync passes forever on a row that
// still describes an UNEMITTED code as an active refusal, because it only
// checks the SLUICE-E-… token is present, never that the prose matches the
// shipped status — exactly the drift error-codes.md rows 29-30 exhibited
// (MariaDB CDC "not supported yet" long after CDC shipped). This test pins
// the retained-but-unemitted class: any code whose registry summary carries
// the [retainedButUnemittedMarker] must have a doc row that also flags it as
// retained/no-longer-emitted, so stale ACTIVE-refusal prose fails CI.
func TestRegistryDocSync_RetainedProse(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	// Index each doc row by the first code token on its line (each code
	// occupies exactly one table row).
	codeRe := regexp.MustCompile(`SLUICE-E-[A-Z0-9-]+`)
	rowFor := map[Code]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if m := codeRe.FindString(line); m != "" {
			if _, seen := rowFor[Code(m)]; !seen {
				rowFor[Code(m)] = line
			}
		}
	}

	sawRetained := false
	for c, info := range registry {
		if !strings.Contains(info.Summary, retainedButUnemittedMarker) {
			continue
		}
		sawRetained = true
		row, ok := rowFor[c]
		if !ok {
			t.Errorf("%s is %s in the registry but has no doc row", c, retainedButUnemittedMarker)
			continue
		}
		up := strings.ToUpper(row)
		if !strings.Contains(up, "RETAINED") && !strings.Contains(up, "NO LONGER EMITTED") {
			t.Errorf("%s summary is %s but its error-codes.md row does not flag it retained/no-longer-emitted "+
				"(prose lags the shipped status — the F7 stale-active-refusal class): %s",
				c, retainedButUnemittedMarker, row)
		}
	}
	if !sawRetained {
		t.Logf("no %s codes in the registry (nothing to cross-check) — fine, the guard is a no-op", retainedButUnemittedMarker)
	}
}
