// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

// Nop is the do-nothing [Sink]. The non-migrate one-shot commands
// (verify, backup, restore) use it on the non-TTY path: their historical
// output is their OWN report (verify renders a text/JSON report to its
// own writer) or their OWN direct slog lines (backup/restore), NOT routed
// through a sink, so the presentation layer must add nothing there —
// keeping their non-TTY output byte-identical to before ADR-0155 phase 2.
// Only the [TTYSink] drives a live view for those commands.
//
// It is distinct from [LogSink] on purpose: LogSink is migrate's
// structured-log sink (it EMITS migrate's `"migration: ..."` records and
// is the [FromContext] default), which the other commands must not adopt
// or they would inject migrate-shaped lines into their own streams.
type Nop struct{}

// compile-time assertion that Nop satisfies Sink.
var _ Sink = Nop{}

func (Nop) PhaseStarted(Phase)                 {}
func (Nop) PhaseCompleted(Phase)               {}
func (Nop) PhaseCompletedEarly(Phase)          {}
func (Nop) TableProgress(string, int64, int64) {}
func (Nop) Warn(string, ...any)                {}
func (Nop) Summary(Result)                     {}
