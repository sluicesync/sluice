// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUnforwardedChangeError_WrapsTheSentinel: the pipeline persists the
// refusal (so a restart refuses again) by errors.Is on
// ir.ErrUnforwardedSchemaChange, so the sentinel must be reachable through
// the terminal wrapper, the reader classifier and an ordinary %w wrap — and
// the marker must not have changed the operator-facing text, which still
// opens with the engine prefix and the grep-stable marker and now names the
// acknowledgement flag.
func TestUnforwardedChangeError_WrapsTheSentinel(t *testing.T) {
	refusal := unforwardedChangeError("db", "t", []string{`ADD CONSTRAINT "u" UNIQUE (name)`})
	terminal := error(&terminalMySQLError{err: refusal})
	for name, err := range map[string]error{
		"constructor":         refusal,
		"terminal wrapper":    terminal,
		"reader classifier":   classifyReaderError(terminal),
		"pipeline-style wrap": fmt.Errorf("cdc reader: %w", terminal),
	} {
		if !errors.Is(err, ir.ErrUnforwardedSchemaChange) {
			t.Errorf("%s: errors.Is(ir.ErrUnforwardedSchemaChange) = false", name)
		}
	}
	msg := refusal.Error()
	if !strings.HasPrefix(msg, "mysql: cdc: "+unforwardedChangeMarker+" on db.t: ") {
		t.Errorf("marker changed the text: %q", msg)
	}
	if !strings.Contains(msg, "--accept-unforwarded-schema-change") || !strings.Contains(msg, "refuses again") {
		t.Errorf("remedy does not name the acknowledgement flag / the persisted refusal: %q", msg)
	}
}
