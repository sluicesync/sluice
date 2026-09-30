// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "errors"

// SQLStater is the standard-SQLSTATE accessor a driver error may expose.
// Postgres's pgconn.PgError implements it (pinned in the postgres engine's
// capabilities_assert.go, so a driver change that dropped the method fails
// the build rather than silently blinding every classifier that reads it).
// go-sql-driver's MySQLError does not: the MySQL engine marks what a
// classifier needs with [WithMarker] instead.
type SQLStater interface {
	SQLState() string
}

// SQLStateOf returns the SQLSTATE of the first error in err's chain that
// exposes one ([SQLStater]), and whether one did.
func SQLStateOf(err error) (string, bool) {
	var s SQLStater
	if errors.As(err, &s) {
		return s.SQLState(), true
	}
	return "", false
}

// sqlStateInvalidCatalogName is ISO SQL's "invalid catalog name": the
// database a connection names does not exist (Postgres raises it at
// connect as `database "x" does not exist`).
const sqlStateInvalidCatalogName = "3D000"

// ErrDatabaseNotFound classifies a connect failure where the server says the
// database the DSN names does not exist. An engine whose driver error does not
// carry SQLSTATE 3D000 through [SQLStater] marks it with [WithMarker] — MySQL
// errno 1049, whose SQLSTATE is the generic 42000.
var ErrDatabaseNotFound = errors.New("the database named in the DSN does not exist")

// IsDatabaseNotFound reports whether err is the server saying the DSN's
// database does not exist, read from the error's structure — SQLSTATE 3D000
// or the [ErrDatabaseNotFound] marker — never from its text. A missing
// relation (42P01) or column (42703) says "does not exist" too, and is not
// this (GC-40 (b), sluice-testing Bug 292).
func IsDatabaseNotFound(err error) bool {
	if errors.Is(err, ErrDatabaseNotFound) {
		return true
	}
	state, ok := SQLStateOf(err)
	return ok && state == sqlStateInvalidCatalogName
}
