// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// rotationReasonAge mirrors pipeline-root's stream_rotation.go constant of the
// same name: the lineage CapReason recorded when the --retain-rotate-at age
// threshold caps a segment. It is a frozen cross-version wire string, so this
// test-only mirror cannot drift silently (changing the root value would be a
// backup-format compatibility break flagged by the rotation tests that own it).
// Duplicated here — rather than exported from root — so the carved-out backup
// test tree does not import root's test tree.
const rotationReasonAge = "retain-rotate-at"
