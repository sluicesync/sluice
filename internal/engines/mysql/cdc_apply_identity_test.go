// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestTransactionIdentity_AnonymousGTIDHasNone pins that an anonymous
// transaction — go-mysql's all-zero-source GTID, the SAME string for every
// anonymous transaction — gets no apply identity rather than one shared by
// all of them, while a real GTID keeps its own.
func TestTransactionIdentity_AnonymousGTIDHasNone(t *testing.T) {
	for gtid, want := range map[string]string{
		"00000000-0000-0000-0000-000000000000:0":  "",
		"00000000-0000-0000-0000-000000000000:17": "",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:23": "3e11fa47-71ca-11e1-9e33-c80aa9429562:23",
		"0-1-42": "0-1-42", // MariaDB domain-server-seq
	} {
		r := &CDCReader{posMode: positionModeGTID, pendingGTID: gtid}
		if got := r.transactionIdentity(nil); got != want {
			t.Errorf("transactionIdentity(%q) = %q, want %q", gtid, got, want)
		}
	}
}
