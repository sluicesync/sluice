// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package netdeadline

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// recordingConn is a net.Conn stand-in that records the deadlines set on it
// and blocks in Write until released — the shape a peer that has stopped
// draining presents.
type recordingConn struct {
	net.Conn // nil: any un-overridden method would panic loudly rather than lie

	mu       sync.Mutex
	writeSet []time.Time
	bothSet  []time.Time
	writes   int
}

func newRecordingConn() *recordingConn {
	return &recordingConn{}
}

func (c *recordingConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeSet = append(c.writeSet, t)
	return nil
}

func (c *recordingConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bothSet = append(c.bothSet, t)
	return nil
}

func (c *recordingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return len(b), nil
}

func (c *recordingConn) lastWriteDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writeSet) == 0 {
		return time.Time{}, false
	}
	return c.writeSet[len(c.writeSet)-1], true
}

func TestWrapConn_ArmsADeadlineBeforeEveryWrite(t *testing.T) {
	rc := newRecordingConn()
	c := WrapConn(rc, time.Minute)

	for i := range 3 {
		if _, err := c.Write([]byte("x")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(rc.writeSet) != 3 {
		t.Fatalf("armed %d deadline(s) for 3 writes, want 3 — a write that is not preceded by an arm is the Bug 229 hang",
			len(rc.writeSet))
	}
	if rc.writes != 3 {
		t.Fatalf("underlying writes = %d, want 3", rc.writes)
	}
	for i, d := range rc.writeSet {
		if d.IsZero() {
			t.Errorf("arm %d set the ZERO deadline (= no deadline)", i)
		}
	}
}

// The load-bearing correctness property: the wrapper may only ever SHORTEN
// the deadline. pgconn cancels an in-flight operation by setting a deadline
// in the PAST; a wrapper that pushed that back into the future would
// un-cancel it.
func TestWrapConn_NeverLengthensAnOwnerDeadline(t *testing.T) {
	cases := []struct {
		name        string
		set         func(net.Conn, time.Time) error
		ownerAt     time.Duration // relative to now; negative = already past
		wantShorter bool
	}{
		{
			name:        "SetDeadline in the past (pgconn's cancel) wins",
			set:         func(c net.Conn, t time.Time) error { return c.SetDeadline(t) },
			ownerAt:     -time.Second,
			wantShorter: true,
		},
		{
			name:        "SetWriteDeadline in the past wins",
			set:         func(c net.Conn, t time.Time) error { return c.SetWriteDeadline(t) },
			ownerAt:     -time.Second,
			wantShorter: true,
		},
		{
			name:        "an owner deadline SOONER than ours wins",
			set:         func(c net.Conn, t time.Time) error { return c.SetDeadline(t) },
			ownerAt:     5 * time.Second,
			wantShorter: true,
		},
		{
			name:        "an owner deadline LATER than ours does not (we shorten it)",
			set:         func(c net.Conn, t time.Time) error { return c.SetDeadline(t) },
			ownerAt:     time.Hour,
			wantShorter: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc := newRecordingConn()
			c := WrapConn(rc, 30*time.Second)
			owner := time.Now().Add(tc.ownerAt)
			if err := tc.set(c, owner); err != nil {
				t.Fatalf("set owner deadline: %v", err)
			}
			if _, err := c.Write([]byte("x")); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, ok := rc.lastWriteDeadline()
			if !ok {
				t.Fatal("no deadline armed")
			}
			if tc.wantShorter {
				if !got.Equal(owner) {
					t.Fatalf("armed %v, want the owner's %v — the wrapper LENGTHENED an owner deadline, which un-cancels a pgconn cancellation",
						got, owner)
				}
				return
			}
			if !got.Before(owner) {
				t.Fatalf("armed %v, want something before the owner's far-future %v", got, owner)
			}
		})
	}
}

// Clearing the deadline (pgconn's post-operation restore, SetDeadline(zero))
// must not be read as "the owner wants zero == the epoch == already expired".
func TestWrapConn_ZeroOwnerDeadlineMeansNoneNotExpired(t *testing.T) {
	rc := newRecordingConn()
	c := WrapConn(rc, 30*time.Second)
	if err := c.SetDeadline(time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := c.SetDeadline(time.Time{}); err != nil { // the restore
		t.Fatalf("restore: %v", err)
	}
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, ok := rc.lastWriteDeadline()
	if !ok {
		t.Fatal("no deadline armed")
	}
	if !got.After(time.Now()) {
		t.Fatalf("armed %v (already expired); the zero owner deadline means NONE, not the epoch", got)
	}
}

func TestWrapConn_NoOpOnNonPositiveDeadline(t *testing.T) {
	rc := newRecordingConn()
	if got := WrapConn(rc, 0); got != net.Conn(rc) {
		t.Error("WrapConn(c, 0) wrapped anyway; a zero deadline must be the pre-item-146 identity")
	}
	if got := WrapConn(nil, time.Minute); got != nil {
		t.Error("WrapConn(nil, d) must return nil")
	}
}

func TestDialWith_WrapsTheDialedConn(t *testing.T) {
	rc := newRecordingConn()
	base := func(context.Context, string, string) (net.Conn, error) { return rc, nil }

	c, err := DialWith(base, time.Minute)(context.Background(), "tcp", "example:3306")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, ok := rc.lastWriteDeadline(); !ok {
		t.Fatal("the dialed conn was not wrapped — no deadline armed on write")
	}

	// A dial error passes through untouched (no nil-conn wrap).
	boom := errors.New("dial failed")
	if _, err := DialWith(func(context.Context, string, string) (net.Conn, error) { return nil, boom }, time.Minute)(
		context.Background(), "tcp", "example:3306",
	); !errors.Is(err, boom) {
		t.Fatalf("dial error = %v, want %v", err, boom)
	}

	// Non-positive deadline / nil next return the input hook unchanged.
	if DialWith(nil, time.Minute) != nil {
		t.Error("DialWith(nil, d) must be nil")
	}
}

// The real-socket proof that a blocked write actually unblocks: a listener
// that accepts and never reads, written to until the socket buffers fill.
// Without the wrapper this Write parks forever — which is Bug 229.
func TestWrapConn_RealSocketWriteToANonDrainingPeerTimesOut(t *testing.T) {
	ctx := t.Context()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		accepted <- c // held open, never read from
	}()

	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = raw.Close() }()
	select {
	case c := <-accepted:
		defer func() { _ = c.Close() }()
	case <-time.After(5 * time.Second):
		t.Fatal("listener never accepted")
	}

	conn := WrapConn(raw, 250*time.Millisecond)
	buf := make([]byte, 1<<20)

	done := make(chan error, 1)
	go func() {
		for {
			if _, werr := conn.Write(buf); werr != nil {
				done <- werr
				return
			}
		}
	}()

	select {
	case werr := <-done:
		var ne net.Error
		if !errors.As(werr, &ne) || !ne.Timeout() {
			t.Fatalf("write failed with %v, want a net.Error timeout — the deadline is what must surface a non-draining peer", werr)
		}
	case <-time.After(30 * time.Second):
		// The test's own bound: a test that reproduces a hang by hanging is
		// not a test.
		t.Fatal("write to a non-draining peer did not time out within 30s — the write deadline is not armed (Bug 229)")
	}
}
