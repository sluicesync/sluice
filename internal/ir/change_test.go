package ir

import "testing"

// TestChangeVariantsImplementInterface is a compile-time / runtime
// check that every shipping [Change] variant satisfies the sealed
// interface. The point of the test is the assignment list itself —
// adding a new variant without registering it here surfaces as a
// missing entry in the table rather than a downstream type-switch
// silently dropping the new case.
func TestChangeVariantsImplementInterface(t *testing.T) {
	cases := []struct {
		name string
		c    Change
	}{
		{"Insert", Insert{}},
		{"Update", Update{}},
		{"Delete", Delete{}},
		{"Truncate", Truncate{}},
		{"TxBegin", TxBegin{}},
		{"TxCommit", TxCommit{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(_ *testing.T) {
			// Type assertion confirms the variant satisfies Change;
			// the seal method ensures only this package can add
			// variants.
			c.c.isChange()
		})
	}
}

// TestTxBeginQualifiedNameEmpty asserts the boundary events surface
// no table reference. The [BatchedChangeApplier] dispatch path
// switches on type rather than on QualifiedName, but downstream
// observers (logs, metrics) consult the qualified name and need a
// well-defined value.
func TestTxBeginQualifiedNameEmpty(t *testing.T) {
	if got := (TxBegin{}).QualifiedName(); got != "" {
		t.Errorf("TxBegin.QualifiedName() = %q; want empty string", got)
	}
	if got := (TxCommit{}).QualifiedName(); got != "" {
		t.Errorf("TxCommit.QualifiedName() = %q; want empty string", got)
	}
}

// TestTxBeginPosRoundTrip confirms the boundary events carry the
// supplied position through the Pos accessor unchanged. The applier
// uses this position as the persisted source-position when the
// boundary is the last event in a flushed batch.
func TestTxBeginPosRoundTrip(t *testing.T) {
	pos := Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7350"}`}
	if got := (TxBegin{Position: pos}).Pos(); got != pos {
		t.Errorf("TxBegin.Pos() = %#v; want %#v", got, pos)
	}
	if got := (TxCommit{Position: pos}).Pos(); got != pos {
		t.Errorf("TxCommit.Pos() = %#v; want %#v", got, pos)
	}
}
