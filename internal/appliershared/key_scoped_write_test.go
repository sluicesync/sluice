// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestRefuseKeyScopedMultiMatch pins the GC-42 refusal's contract: matchable
// by sentinel, terminal (no retry can help), coded, and naming what an
// operator must query — the marker, the table, the key and the count.
func TestRefuseKeyScopedMultiMatch(t *testing.T) {
	err := RefuseKeyScopedMultiMatch("postgres", "delete", "public", "s", ir.Row{"id": int64(2), "note": nil, "blob": strings.Repeat("x", 200)}, 2)
	if !errors.Is(err, ErrKeyScopedWriteMatchedMultipleRows) {
		t.Fatalf("not matchable by the sentinel: %v", err)
	}
	// The fleet supervisor keys on the ir sentinel and logs its text as the
	// marker (TestSupervisor_RefusalsARestartRepeatsAreNotRestarted).
	if !errors.Is(err, ir.ErrKeyScopedWriteMatchedMultipleRows) || ErrKeyScopedWriteMatchedMultipleRows.Error() != KeyScopedWriteMultiMatchMarker {
		t.Fatalf("the sentinel is not ir's, or its text %q is not the marker", ErrKeyScopedWriteMatchedMultipleRows.Error())
	}
	if !ir.IsTerminal(err) {
		t.Fatal("not terminal")
	}
	if c, ok := sluicecode.FromError(err); !ok || c.Code != sluicecode.CodeCDCKeyMatchedMultipleRows || c.Hint == "" {
		t.Fatalf("not coded %s with a hint: %v", sluicecode.CodeCDCKeyMatchedMultipleRows, err)
	}
	msg := err.Error()
	for _, want := range []string{KeyScopedWriteMultiMatchMarker, "public.s", "id=2", "note=NULL", "matched 2 target rows", "would delete"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message lacks %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, strings.Repeat("x", describeRowKeyValueMax+1)) {
		t.Errorf("a wide key value was not truncated: %s", msg)
	}
	if up := RefuseKeyScopedMultiMatch("mysql", "update", "", "t", ir.Row{"u": "x"}, 3).Error(); !strings.Contains(up, "would overwrite") {
		t.Errorf("an UPDATE refusal does not say it would overwrite: %s", up)
	}
}
