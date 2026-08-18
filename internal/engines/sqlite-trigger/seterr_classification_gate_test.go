// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestSetErrSitesClassify is sqlite-trigger's instance of the shared Bug-207
// gate (audit backlog A-4). Per the errclassgate package doc an engine without
// a gate file is itself the finding, and the meta-gate now enforces that; this
// closes sqlite-trigger's half of the reach gap.
//
// Unlike the network-backed CDC readers, sqlite-trigger polls a LOCAL SQLite
// change-log table. A poll fault there is a setup/logic error (the change-log
// table gone, file corruption) or a driver-handled SQLITE_BUSY — none of which
// has a reconnect remedy the streamer could retry INTO, so terminal-by-default
// is correct rather than the Bug-207 hazard it is on a pooled network source.
// The single site is allowlisted with that reason; the value of the gate here
// is that a FUTURE second site (e.g. a network-backed variant that grows a
// transient-worthy poll surface) fails until it is classified or judged.
func TestSetErrSitesClassify(t *testing.T) {
	errclassgate.Assert(t, errclassgate.Config{
		Dir:         ".",
		Method:      "setErr",
		Classifiers: map[string]bool{},
		Allowed: map[string]string{
			`cdc_reader.go:fmt.Errorf("sqlite-trigger: poll: %w", err)`: "polls a LOCAL SQLite change-log table; a poll fault is a setup/logic error or a driver-handled SQLITE_BUSY, none with a reconnect remedy the streamer could retry into — terminal is correct. A network-backed variant that grows a transient poll surface would need its own classifier.",
		},
		MinSites: 1,
	})
}
