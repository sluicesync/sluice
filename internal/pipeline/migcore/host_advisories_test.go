// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// advisorEngine is a minimal ir.Engine + ir.SourceHostAdvisor stub
// that records the arguments the chokepoint hands it.
type advisorEngine struct {
	ir.Engine // nil-panics on any real use — only the advisor surface is exercised

	gotDSN string
	gotCDC bool
	out    []ir.SourceHostAdvisory
}

func (a *advisorEngine) SourceHostAdvisories(dsn string, cdc bool) []ir.SourceHostAdvisory {
	a.gotDSN = dsn
	a.gotCDC = cdc
	return a.out
}

// plainEngine implements only ir.Engine (embedded nil), NOT the
// advisor surface.
type plainEngine struct{ ir.Engine }

// TestWarnSourceHostAdvisories pins the chokepoint: every returned
// advisory is logged at WARN with its message as the record text and
// the hint as a structured attr; the DSN and cdc flag are passed
// through verbatim; an engine without the surface is a silent no-op.
func TestWarnSourceHostAdvisories(t *testing.T) {
	logs := captureSlog(t)

	eng := &advisorEngine{out: []ir.SourceHostAdvisory{
		{Message: "host is a pooler endpoint", Hint: "use the direct endpoint"},
		{Message: "second advisory", Hint: "second hint"},
	}}
	WarnSourceHostAdvisories(context.Background(), eng, "postgres://u@h/db", true)

	if eng.gotDSN != "postgres://u@h/db" || !eng.gotCDC {
		t.Errorf("surface got (dsn=%q, cdc=%v); want the caller's values verbatim", eng.gotDSN, eng.gotCDC)
	}
	out := logs.String()
	for _, want := range []string{
		"level=WARN",
		"host is a pooler endpoint",
		"hint=\"use the direct endpoint\"",
		"second advisory",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in the log; got: %q", want, out)
		}
	}

	// Engine without the surface: nothing logged, nothing panics.
	logs2 := captureSlog(t)
	WarnSourceHostAdvisories(context.Background(), plainEngine{}, "any", false)
	if got := logs2.String(); got != "" {
		t.Errorf("engine without the surface must be a silent no-op; got: %q", got)
	}
}
