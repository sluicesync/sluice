// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// TestNullHashInputRefusal_NamesTheRemedy pins the text of sample
// verify's NULL-hash-input refusal. Its only real-row cause is a MySQL 8
// JSON value larger than max_allowed_packet (rendered empty, turned NULL
// by mysqlJSONTooLargeArm), so the message must name the row, that
// cause, and the way forward: verify the table again with a larger
// max_allowed_packet. The live refusal is pinned against a real server
// by TestSampleRowHashes_ValueLargerThanMaxAllowedPacket.
func TestNullHashInputRefusal_NamesTheRemedy(t *testing.T) {
	msg := nullHashInputRefusal("docs", "7").Error()
	for _, want := range []string{
		"SampleRowHashes docs: row 7:",
		"refusing rather than hashing fewer columns",
		"JSON value larger than the server's max_allowed_packet",
		"Verify this table with a larger max_allowed_packet",
		"SET GLOBAL max_allowed_packet",
		"sluice verify --include-table docs",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q should contain %q", msg, want)
		}
	}
}
