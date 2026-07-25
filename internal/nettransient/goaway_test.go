// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package nettransient_test

import (
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/nettransient"
)

// verbatimProductionGoAway is the EXACT error text that took down the
// soak231 soak on 2026-07-24 (roadmap item 79), copied from the sync log
// rather than reconstructed — a paraphrase would not prove the matcher
// handles what the wire actually produces.
const verbatimProductionGoAway = `pipeline: source cdc reader: mysql/vstream: recv: rpc error: ` +
	`code = InvalidArgument desc = protocol error: incomplete envelope: ` +
	`http2: server sent GOAWAY and closed the connection; ` +
	`LastStreamID=1, ErrCode=NO_ERROR, debug="graceful_stop"`

// TestIsGracefulGoAway_PositiveShapes pins that a GOAWAY carrying the
// no-error code is recognised as a graceful drain, across the spellings
// different HTTP/2 and gRPC stacks emit.
func TestIsGracefulGoAway_PositiveShapes(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"verbatim production shape (item 79)", verbatimProductionGoAway},
		{"spaced symbolic form", `http2: server sent GOAWAY; ErrCode = NO_ERROR`},
		{"alternate 'error code' wording", `stream terminated by GOAWAY, error code = NO_ERROR`},
		{"lower-case wire text", `http2: server sent goaway and closed the connection; errcode=no_error`},
		{"no debug field at all", `http2: server sent GOAWAY and closed the connection; LastStreamID=7, ErrCode=NO_ERROR`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !nettransient.IsGracefulGoAway(errors.New(tc.text)) {
				t.Error("IsGracefulGoAway = false; want true — a GOAWAY with NO_ERROR is a graceful drain and must be retriable")
			}
		})
	}
}

// TestIsGracefulGoAway_StaysTerminal is the load-bearing half. A GOAWAY is
// benign ONLY when its error code is benign; every other GOAWAY means the
// peer is rejecting how we speak to it, and retrying identically would mask a
// real fault. This is exactly why the check is a conjunction rather than a
// TextShapes substring entry.
func TestIsGracefulGoAway_StaysTerminal(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"PROTOCOL_ERROR — we are speaking wrongly", `http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=PROTOCOL_ERROR`},
		{"ENHANCE_YOUR_CALM — we are being rate-limited off", `http2: server sent GOAWAY; ErrCode=ENHANCE_YOUR_CALM, debug="too_many_pings"`},
		{"INADEQUATE_SECURITY — TLS posture rejected", `http2: server sent GOAWAY; ErrCode=INADEQUATE_SECURITY`},
		{"INTERNAL_ERROR", `http2: server sent GOAWAY; ErrCode=INTERNAL_ERROR`},
		{"GOAWAY with no readable code — must not be ASSUMED benign", `http2: server sent GOAWAY and closed the connection`},
		{"graceful_stop debug WITHOUT a no-error code — debug text is not load-bearing", `http2: server sent GOAWAY; ErrCode=PROTOCOL_ERROR, debug="graceful_stop"`},
		{"no GOAWAY at all", `rpc error: code = InvalidArgument desc = malformed request`},
		{"the word NO_ERROR without a GOAWAY", `some unrelated failure; ErrCode=NO_ERROR`},
		{"nil", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.text != "" {
				err = errors.New(tc.text)
			}
			if nettransient.IsGracefulGoAway(err) {
				t.Error("IsGracefulGoAway = true; want false — only a GOAWAY carrying NO_ERROR may be treated as a graceful drain")
			}
		})
	}
}

// TestIsGracefulGoAway_UnwrapsChains pins that the predicate sees through the
// wrapping the real call path applies (the pump wraps with `recv: %w`, the
// pipeline wraps again), since that is how it arrives in production.
func TestIsGracefulGoAway_UnwrapsChains(t *testing.T) {
	wrapped := errors.New(verbatimProductionGoAway)
	for i := 0; i < 3; i++ {
		wrapped = errWrap{wrapped}
	}
	if !nettransient.IsGracefulGoAway(wrapped) {
		t.Error("IsGracefulGoAway = false through a wrapped chain; want true")
	}
}

type errWrap struct{ inner error }

func (e errWrap) Error() string { return "layer: " + e.inner.Error() }
func (e errWrap) Unwrap() error { return e.inner }

// TestIsGracefulGoAway_NotInTheGenericMatcher pins the deliberate separation:
// the bare GOAWAY text must NOT be retriable via [nettransient.IsTransientShape],
// because that matcher is a plain substring corpus and would then retry the
// error-carrying GOAWAYs too. The conjunction predicate is the only door.
func TestIsGracefulGoAway_NotInTheGenericMatcher(t *testing.T) {
	hostile := errors.New(`http2: server sent GOAWAY; ErrCode=PROTOCOL_ERROR`)
	if nettransient.IsTransientShape(hostile) {
		t.Error("IsTransientShape matched a PROTOCOL_ERROR GOAWAY — 'goaway' must never enter the substring corpus")
	}
}
