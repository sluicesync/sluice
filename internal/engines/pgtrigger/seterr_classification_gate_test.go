// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestSetErrSitesClassify is pgtrigger's instance of the shared Bug-207 gate
// (audit backlog A-4). The errclassgate package doc says every engine
// instantiates it and adding one without a gate file is itself a finding — the
// meta-gate now enforces that mechanically, and this is the file that closes
// pgtrigger's half of the reach gap.
//
// The reader's CDC pump parks two error shapes. The poll fault goes through
// classifyPollError (which rides out the transient SQLSTATEs via
// postgres.IsReadTransientSQLState / triggercdc.ClassifyTransient), and the §7
// observed-DDL condition goes through refuseObservedDDL, a purpose-built
// TERMINAL — the deliberate opposite of an unclassified park, and one the
// transient classifier must never touch. Both are accepted; a future third
// site that parks a raw error fails here.
func TestSetErrSitesClassify(t *testing.T) {
	errclassgate.Assert(t, errclassgate.Config{
		Dir:    ".",
		Method: "setErr",
		Classifiers: map[string]bool{
			"classifyPollError": true,
			// Not a classifier in the transient-vs-terminal sense: a
			// purpose-built refuse-loudly terminal. Accepted as-is because
			// routing observed DDL through the transient classifier would be
			// strictly wrong — see refuseObservedDDL's doc.
			"refuseObservedDDL": true,
		},
		Allowed:  map[string]string{},
		MinSites: 2,
	})
}
