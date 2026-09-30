// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMarkDatabaseNotFound pins the MySQL half of GC-40 (b): errno 1049 is
// classified [ir.ErrDatabaseNotFound] with its text unchanged, and a missing
// table (1146) or any other error is not.
func TestMarkDatabaseNotFound(t *testing.T) {
	bad := fmt.Errorf("dial: %w", &mysql.MySQLError{Number: 1049, Message: "Unknown database 'nope'"})
	got := markDatabaseNotFound(bad)
	if !ir.IsDatabaseNotFound(got) || got.Error() != bad.Error() {
		t.Errorf("1049: IsDatabaseNotFound=%v text=%q; want true and the original text", ir.IsDatabaseNotFound(got), got.Error())
	}
	for _, err := range []error{
		&mysql.MySQLError{Number: 1146, Message: "Table 'app.t' doesn't exist"},
		&mysql.MySQLError{Number: 1045, Message: "Access denied for user"},
		errors.New("Unknown database 'nope'"),
	} {
		if ir.IsDatabaseNotFound(markDatabaseNotFound(err)) {
			t.Errorf("%v classified as a missing database", err)
		}
	}
}
