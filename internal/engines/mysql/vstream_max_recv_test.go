// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The VStream gRPC receive ceiling (ticket 36535).
//
// A PlanetScale region move failed its cold copy on a 148 GB chat-message
// table with:
//
//	rpc error: code = ResourceExhausted desc = grpc: received message after
//	decompression larger than max 4194304
//
// 4194304 is 4 MiB — gRPC's stock CLIENT default. sluice builds its VStream
// client by hand (grpc.NewClient with credentials only) rather than through
// Vitess's grpcclient.DialContext, so it inherited that 4 MiB ceiling while
// every Vitess-native client gets 16 MiB. Nothing on the server rejected the
// row; sluice refused to receive it.
//
// VStream ships whole ROWS and cannot split one across messages, so a single
// wide row is enough. The reporting table has a `mediumtext` content column
// (16 MiB max) plus two `json` columns.
//
// Two defects, pinned separately below, because fixing only the first would
// still have left an operator burning 10 reconnects on a deterministic error.

package mysql

import (
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStreamMaxRecvBytesFromDSN(t *testing.T) {
	t.Run("absent uses the default, which is above gRPC's 4 MiB", func(t *testing.T) {
		cfg := &gomysql.Config{Params: map[string]string{}}
		got, err := vstreamMaxRecvBytesFromDSN(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != defaultVStreamMaxRecvBytes {
			t.Errorf("got %d, want %d", got, defaultVStreamMaxRecvBytes)
		}
		// The regression this exists to prevent: silently sitting on gRPC's
		// stock ceiling, which is what shipped.
		const grpcStockDefault = 4 << 20
		if got <= grpcStockDefault {
			t.Errorf("default ceiling is %d, at or below gRPC's stock %d — the ticket-36535 failure returns",
				got, grpcStockDefault)
		}
		// And above every server ceiling we know of, so sluice is never the
		// STRICTER of the two ends: Vitess defaults to 16 MiB and PlanetScale
		// is understood to run 64 MiB. Sitting AT a server ceiling is not
		// enough — a message the server sends at its own limit can still
		// exceed ours once protobuf framing is counted.
		const knownServerCeiling = 64 << 20 // PlanetScale
		if got <= knownServerCeiling {
			t.Errorf("default ceiling is %d, at or below the known server ceiling %d — sluice would be the "+
				"stricter end and could reject a message vtgate was willing to send, which is the "+
				"ticket-36535 failure one size up",
				got, knownServerCeiling)
		}
	})

	t.Run("explicit value is honoured", func(t *testing.T) {
		cfg := &gomysql.Config{Params: map[string]string{"vstream_max_recv_bytes": "1048576"}}
		got, err := vstreamMaxRecvBytesFromDSN(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 1048576 {
			t.Errorf("got %d, want 1048576", got)
		}
	})

	t.Run("malformed and non-positive values refuse loudly", func(t *testing.T) {
		for _, bad := range []string{"abc", "0", "-1", "16MiB"} {
			cfg := &gomysql.Config{Params: map[string]string{"vstream_max_recv_bytes": bad}}
			if _, err := vstreamMaxRecvBytesFromDSN(cfg); err == nil {
				t.Errorf("%q was accepted; an operator setting this knob is working around a specific "+
					"oversized-row failure, and silently ignoring their value puts them back on the default "+
					"with no indication why nothing changed", bad)
			}
		}
	})
}

// TestClassifyReaderError_MessageTooLargeIsTerminal is the second defect. The
// gRPC text carries `code = ResourceExhausted`, which the Vitess retriable
// substring list matches as a throttler/pool transient — correct for those,
// wrong for this one. The same row is re-sent on every reconnect, so the
// operator burned the reconnect budget (10) and then the copy retry budget
// (8) against an error that could never clear.
func TestClassifyReaderError_MessageTooLargeIsTerminal(t *testing.T) {
	// The verbatim shape from the ticket.
	raw := errors.New("rpc error: code = ResourceExhausted desc = grpc: received message after " +
		"decompression larger than max 4194304")

	got := classifyReaderError(raw)
	if got == nil {
		t.Fatal("classified to nil")
	}

	var re ir.RetriableError
	if errors.As(got, &re) && re.Retriable() {
		t.Error("the oversized-message error classified as RETRIABLE. It is deterministic — the same row is " +
			"re-sent on every reconnect — so retrying spins the reconnect and copy budgets and turns an " +
			"actionable failure into a slow one with a misleading trail.")
	}

	// The message must name the remedy, and must not leave the operator
	// believing their server rejected the row.
	msg := got.Error()
	for _, want := range []string{"vstream_max_recv_bytes", "grpc_max_message_size", "NOT retriable"} {
		if !strings.Contains(msg, want) {
			t.Errorf("classified message does not mention %q; got: %s", want, msg)
		}
	}
}

// The carve-out must be narrow: the throttler and pool flavours of
// ResourceExhausted are genuinely transient and must stay retriable, or a
// PlanetScale throttle would become a hard copy failure.
func TestClassifyReaderError_OtherResourceExhaustedStaysRetriable(t *testing.T) {
	// A real VStream recv surfaces a gRPC STATUS, not a bare string — using
	// errors.New here would test a shape the classifier never sees.
	for _, desc := range []string{
		"throttler engaged",
		"pool connection limit exceeded",
	} {
		msg := desc
		got := classifyReaderError(status.Error(codes.ResourceExhausted, desc))
		var re ir.RetriableError
		if !errors.As(got, &re) || !re.Retriable() {
			t.Errorf("%q is no longer retriable — the size carve-out is too broad and a throttle would now "+
				"fail the copy outright; got: %v", msg, got)
		}
	}
}

func TestIsVStreamMessageTooLargeError_BothGRPCWordings(t *testing.T) {
	// gRPC emits two forms depending on whether compression was used.
	for _, msg := range []string{
		"grpc: received message after decompression larger than max 4194304",
		"grpc: received message larger than max (5242880 vs. 4194304)",
	} {
		if !isVStreamMessageTooLargeError(errors.New(msg)) {
			t.Errorf("not matched: %s", msg)
		}
	}
	for _, msg := range []string{
		"rpc error: code = ResourceExhausted desc = throttler engaged",
		"grpc: received message",       // partial, must not match
		"some other larger than max x", // lacks the grpc prefix
	} {
		if isVStreamMessageTooLargeError(errors.New(msg)) {
			t.Errorf("falsely matched: %s", msg)
		}
	}
}
