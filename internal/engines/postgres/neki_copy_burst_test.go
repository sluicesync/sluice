//go:build integration || nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
)

// The apparatus behind the concurrent-COPY premise arm: a COPY that streams a
// sustained burst, announces that the whole burst is on the wire, and then
// holds its slot open until released.
//
// It lives under `integration || nekiverify` rather than with the arm so the
// per-PR run can prove the MECHANISM against an ordinary PostgreSQL container
// ([TestPostgresSuite_NekiCopyBurstApparatus]). That split is not tidiness.
// This probe has now had three apparatus defects — a zero-byte COPY that may
// never have reached a shard, a serial release that spent 87 s on timeouts,
// and a four-second silence read as acceptance — and every one of them was
// found by spending a provisioned cluster. The parts that a router is not
// required to answer should not cost one.
const (
	// nekiCopyBurstRows × nekiCopyBurstPayload ≈ 512 KiB per session. The
	// number that matters is that it is many times pgx's own 64 KiB send
	// frame, so a router that refuses a COPY it has nowhere to put has had
	// every opportunity to say so before the session is counted as accepted.
	//
	// Deliberately not larger: these rows land in the shared fixture table
	// when a session's COPY completes, and the arm's teardown has to delete
	// them.
	nekiCopyBurstRows    = 128
	nekiCopyBurstPayload = 4000

	// nekiCopyProbeMarker prefixes every payload the probe writes, so teardown
	// finds its rows with a prefix match and catches nothing else. The prefix
	// (not equality) is load-bearing since the burst appends a per-row suffix;
	// [TestPostgresSuite_NekiCopyBurstApparatus] pins that the predicate the
	// arm deletes with actually matches what the reader writes.
	nekiCopyProbeMarker = "copy-limit-probe"
)

// holdOneCopy opens a COPY … FROM STDIN into table, streams a sustained burst,
// closes burstDone once every byte of it has been flushed, and then keeps the
// COPY open until release is closed.
//
// The reader is the mechanism: `CopyFrom` does not return until the reader
// reports EOF, so the COPY — and its slot on the cluster — is held for exactly
// as long as the caller wants it. Returning io.EOF after the burst ends the
// COPY cleanly; whether its rows land depends on whether the platform accepted
// it, and the caller's teardown deletes them either way.
func holdOneCopy(ctx context.Context, db *sql.DB, table string, release <-chan struct{},
	burstDone chan<- struct{}, tenant, firstID int,
) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	return conn.Raw(func(driverConn any) error {
		pgConn, perr := pgConnFromDriver(driverConn)
		if perr != nil {
			return perr
		}
		_, cerr := pgConn.CopyFrom(
			ctx,
			newSustainedCopyReader(release, burstDone, tenant, firstID),
			fmt.Sprintf("COPY %s (tenant_id, id, v) FROM STDIN", table),
		)
		if cerr != nil {
			return fmt.Errorf("COPY FROM STDIN: %w", cerr)
		}
		return nil
	})
}

// sustainedCopyReader streams [nekiCopyBurstRows] rows, signals that the whole
// burst has been flushed, then blocks until released and reports EOF.
//
// # Why a burst rather than one row
//
// Two earlier cuts, and the reason each was wrong, because the shape repeats.
//
// The FIRST sent no bytes at all. A COPY that never sends a row never routes
// anything, so it may occupy a protocol slot on the router while never
// engaging the per-shard machinery the limit is about — counting open COPY
// commands and comparing them to a constant about concurrent copies.
//
// The SECOND sent exactly one row and waited four seconds. That reached a
// shard but proved nothing about ACCEPTANCE, because a Neki router does not
// deliver its `53300` until the client sends more: three paid runs counted
// twelve sessions as accepted and then collected eight `53300`s from those
// same sessions on release, in the same second. See
// [nekiConcurrentCopyLimitHoldsOnTheCluster]'s doc for the log extract.
//
// So the burst is sized to outrun any plausible router-side buffering, and
// burstDone is closed on the Read that FOLLOWS the last byte — by which point
// pgx has flushed it, since pgx flushes after every Read that returns data.
// That ordering is what makes "the burst is on the wire" a fact rather than an
// assumption, and it is what
// [TestPostgresSuite_NekiCopyBurstApparatus] pins.
type sustainedCopyReader struct {
	buf       []byte
	off       int
	burstDone chan<- struct{}
	closed    bool
	release   <-chan struct{}
	eof       bool
}

func newSustainedCopyReader(release <-chan struct{}, burstDone chan<- struct{},
	tenant, firstID int,
) *sustainedCopyReader {
	var b bytes.Buffer
	b.Grow(nekiCopyBurstRows * (nekiCopyBurstPayload + 32))
	// A payload with no tab, newline or backslash in it, so the COPY text
	// format needs no escaping and a short read can never split an escape
	// sequence in half.
	filler := strings.Repeat("x", nekiCopyBurstPayload-len(nekiCopyProbeMarker)-8)
	for k := range nekiCopyBurstRows {
		fmt.Fprintf(&b, "%d\t%d\t%s-%06d%s\n", tenant, firstID+k, nekiCopyProbeMarker, k, filler)
	}
	return &sustainedCopyReader{buf: b.Bytes(), burstDone: burstDone, release: release}
}

func (r *sustainedCopyReader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}
	if r.off < len(r.buf) {
		n := copy(p, r.buf[r.off:])
		r.off += n
		return n, nil
	}
	// Every burst byte has been handed to pgx, and flushed by the Send that
	// followed each of those Reads. Announce it once, then park.
	if !r.closed {
		r.closed = true
		if r.burstDone != nil {
			close(r.burstDone)
		}
	}
	<-r.release
	r.eof = true
	return 0, io.EOF
}
