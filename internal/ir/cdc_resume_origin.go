// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// CDCResumeOrigin names where the position a CDC stream resumes from was
// read, so a reader's refusal about that position can name it, and name the
// remedy that fits it (GC-41 (k)). A reader cannot tell on its own: a
// Postgres resume position is the same {slot, lsn} whether `sync` read it
// from the target's control row or `backup stream` read it from a chain
// manifest, and the two have different holders, causes and recoveries.
//
// The zero value is [CDCResumeOriginUnstated], whose text names no holder and
// gives every remedy, so a caller that never states an origin gets a refusal
// that is less specific but never wrong.
type CDCResumeOrigin int

const (
	// CDCResumeOriginUnstated is a caller that did not say.
	CDCResumeOriginUnstated CDCResumeOrigin = iota
	// CDCResumeOriginTargetControlRow is a `sync` warm resume, from the
	// position the target persisted in its control table.
	CDCResumeOriginTargetControlRow
	// CDCResumeOriginChainHandoff is a `sync --position-from-manifest` start,
	// from the end position of the backup chain the target was restored from.
	CDCResumeOriginChainHandoff
	// CDCResumeOriginBackupChain is `backup stream` / `backup incremental`,
	// from the end position of the chain's last committed manifest.
	CDCResumeOriginBackupChain
)
