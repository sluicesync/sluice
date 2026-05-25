// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Unit tests for the heartbeat-writer helpers. The MySQL HeartbeatWriter
// methods themselves require a live database; their behaviour is pinned
// by heartbeat_writer_integration_test.go (build tag `integration`).
// Here we exercise the pure helpers that don't need a DB.

import (
	"errors"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/orware/sluice/internal/ir"
)

// TestValidHeartbeatTableName_AllowedNames pins the conservative allowlist:
// alphanumeric + underscore identifiers starting with a letter or
// underscore are accepted. The guard exists because the engine emits
// the name unquoted-internally (backtick-wrapped at the SQL boundary)
// and MySQL backtick-quoting doesn't escape interior backticks; any
// non-alnum/_ char would be a potential injection vector.
func TestValidHeartbeatTableName_AllowedNames(t *testing.T) {
	allowed := []string{
		"sluice_heartbeat",    // default
		"_sluice_heartbeat",   // leading underscore
		"sluice_heartbeat_v2", // trailing digits ok
		"SluiceHeartbeat",     // mixed case
		"a",                   // single char
		"abc123",              // alnum
		"_",                   // single underscore
	}
	for _, name := range allowed {
		t.Run(name, func(t *testing.T) {
			if !validHeartbeatTableName(name) {
				t.Errorf("validHeartbeatTableName(%q) = false; want true", name)
			}
		})
	}
}

// TestValidHeartbeatTableName_RejectedNames pins the loud-failure path:
// anything with a backtick, space, or special char is refused. A
// leading digit is also rejected because it's not a standard MySQL
// unquoted identifier; the guard's "predictable to operators" property
// matters more than handling the edge case.
func TestValidHeartbeatTableName_RejectedNames(t *testing.T) {
	rejected := []string{
		"",                  // empty
		"1invalid",          // leading digit
		"sluice heartbeat",  // space
		"sluice-heartbeat",  // hyphen
		"sluice.heartbeat",  // dot
		"sluice`heartbeat",  // backtick (the injection vector)
		"sluice;DROP",       // semicolon
		"sluice'heartbeat",  // single quote
		"sluice\"heartbeat", // double quote
		"héartbeat",         // non-ASCII
	}
	for _, name := range rejected {
		t.Run(name, func(t *testing.T) {
			if validHeartbeatTableName(name) {
				t.Errorf("validHeartbeatTableName(%q) = true; want false", name)
			}
		})
	}
}

// TestIsMySQLPermissionDenied_Detects pins the privilege-error
// classifier — every code we treat as a permission-denied trigger for
// the F17 WARN-once-skip path.
func TestIsMySQLPermissionDenied_Detects(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"table access denied 1142", &gomysql.MySQLError{Number: 1142, Message: "TABLEACCESS_DENIED"}, true},
		{"db access denied 1044", &gomysql.MySQLError{Number: 1044, Message: "DBACCESS_DENIED"}, true},
		{"unrelated MySQL error", &gomysql.MySQLError{Number: 1062, Message: "duplicate key"}, false},
		{"plain non-mysql error", errors.New("some other error"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMySQLPermissionDenied(tc.err); got != tc.want {
				t.Errorf("isMySQLPermissionDenied(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestHeartbeatPermissionSentinel pins that the IR-level sentinel is
// errors.Is-matchable through a wrapped error — the pipeline wiring
// relies on errors.Is to detect the permission case.
func TestHeartbeatPermissionSentinel(t *testing.T) {
	// Wrap as the engine-side helpers do.
	underlying := &gomysql.MySQLError{Number: 1142, Message: "TABLEACCESS_DENIED for 'sluice_heartbeat'"}
	wrapped := errors.Join(ir.ErrHeartbeatPermission, underlying)
	if !errors.Is(wrapped, ir.ErrHeartbeatPermission) {
		t.Errorf("wrapped permission error should match ir.ErrHeartbeatPermission via errors.Is")
	}
	// And a plain transient error must NOT match.
	transient := errors.New("connection reset by peer")
	if errors.Is(transient, ir.ErrHeartbeatPermission) {
		t.Errorf("transient error must not falsely match ir.ErrHeartbeatPermission")
	}
}
