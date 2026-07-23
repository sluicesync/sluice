// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package docsync holds doc<->doc and doc<->code synchronization gates
// that have no more specific home — tests that parse tracked
// documentation and fail CI when two places that must agree drift
// apart (the run-filter-guard lesson: a doc that must stay in sync
// gets a test, not a convention). The package intentionally exports
// nothing; it exists for its tests.
package docsync
