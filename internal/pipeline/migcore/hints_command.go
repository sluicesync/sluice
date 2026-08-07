// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"sort"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Which COMMAND is being told the remedy (Bug 230).
//
// # Why this exists
//
// The hint registry is phase-scoped, and a phase is shared: the bulk-copy,
// index and constraint phases all run under `migrate`, under BOTH `sync`
// cold-start paths (`sync start` and the fleet's `sync run`), under
// `schema add-table` and — for the DDL phases — under `restore`. One hint
// string therefore reaches operators of five different commands, whose flag
// surfaces are NOT the same.
//
// The bulk-copy catch-all said "use --resume to continue …, or
// --exclude-table=<name> to skip it" to all of them. `sync start` has no
// `--resume` (it warm-resumes by default on re-invocation with the same
// `--stream-id`), `sync run` has neither flag (its table scope is a fleet-config
// key), and `schema add-table` has neither AND copies exactly one table, so
// there is nothing to exclude — its own `--help` says "there is no resume flag"
// while its failure hint told the operator to pass one. Three of the five
// commands that print that line exit 80 `unknown flag` if the operator follows
// it, mid-incident.
//
// # The shape, and why the zero value is the safe one
//
// [errorHint.hint] is the COMMAND-NEUTRAL text — what every command sees
// unless a sharper one is registered — and it must therefore name no flag that
// is not valid everywhere. [errorHint.perCommand] carries the sharper,
// flag-precise text for the commands that have the flags. A command with no
// override degrades to advice that is less specific but never unrunnable,
// which is the [CLAUDE.md] zero-value-safe rule applied to prose: forgetting
// to register an override costs precision, never correctness.
//
// The running command is a PROCESS-level fact — one kong command runs per
// sluice process — so it is set once at the composition root (cmd/sluice's
// main, from kong's own selected node) rather than threaded through the
// hundred-odd [WrapWithHint] call sites. Setting it from kong's model rather
// than from a hand-kept roster is what makes it cover every command, including
// ones that do not exist yet.
//
// TestHintRegistryTextsNameOnlyFlagsTheirCommandAccepts in cmd/sluice is the
// gate: it grades every bare `--flag` in every text this file can emit against
// that command's real kong flag set.

// Command is a kong command PATH — "migrate", "sync start",
// "schema add-table" — spelled exactly the way cmd/sluice's kong model spells
// it, because that is what the gate grades against. The empty Command means
// "not set": every hint falls back to its command-neutral text.
type Command string

// The commands whose runs reach a phase whose hint varies by command. This is
// not a closed roster of what [SetRunningCommand] may hold — it accepts any
// kong path — only the subset the registry currently sharpens for.
const (
	CommandMigrate   Command = "migrate"
	CommandSyncStart Command = "sync start"
	CommandSyncRun   Command = "sync run"
	CommandAddTable  Command = "schema add-table"
)

// runningCommand holds the kong command path of this process's invocation.
// Atomic because a fleet `sync run` reads it from many sync goroutines while
// tests write it; production writes it exactly once, before any pipeline work.
var runningCommand atomic.Value // Command

// SetRunningCommand records the kong command path the operator invoked, so
// hints can name a remedy that command actually accepts. Called once from
// cmd/sluice's composition root with kong's selected-node path; an empty path
// (no subcommand selected) leaves every hint on its neutral text.
//
// Tests that set this must restore it — `defer SetRunningCommand("")` — and
// must not run in parallel with each other, since it is process state.
func SetRunningCommand(path string) { runningCommand.Store(Command(path)) }

// RunningCommand returns the command path [SetRunningCommand] recorded, or ""
// when nothing did (every library caller, and every test that does not opt in).
func RunningCommand() Command {
	c, _ := runningCommand.Load().(Command)
	return c
}

// textFor returns the hint text this entry shows to cmd: the registered
// override when there is one, the command-neutral text otherwise.
func (h errorHint) textFor(cmd Command) string {
	if t, ok := h.perCommand[cmd]; ok {
		return t
	}
	return h.hint
}

// HintText is one operator-facing hint string together with the command it is
// shown to. Exported for the CLI-surface gate in cmd/sluice, which is the only
// package that can see kong's model (cmd/sluice is package main, so nothing
// under internal/ can import it and the grading has to happen there).
type HintText struct {
	// Code is the entry's stable error code, so a finding can name the hint
	// an operator would actually receive.
	Code sluicecode.Code

	// Command is the command this text is shown to. The empty Command marks
	// the command-NEUTRAL text, which every command without an override
	// receives — so it has to be graded against ALL of them.
	Command Command

	// Text is the hint line, exactly as WrapWithHint would append it.
	Text string
}

// HintTexts returns every operator-facing text the hint registry can emit,
// each tagged with the command that receives it. One entry per registry row
// for the neutral text, plus one per registered per-command override.
//
// This is the whole graded surface of the bare-flag gate, and it is DERIVED
// from the registry rather than listed anywhere: a new hint, or a new
// per-command override, is graded the moment it is added.
func HintTexts() []HintText {
	var out []HintText
	for _, h := range hintRegistry {
		out = append(out, HintText{Code: h.code, Text: h.hint})
		cmds := make([]Command, 0, len(h.perCommand))
		for c := range h.perCommand {
			cmds = append(cmds, c)
		}
		sort.Slice(cmds, func(i, j int) bool { return cmds[i] < cmds[j] })
		for _, c := range cmds {
			out = append(out, HintText{Code: h.code, Command: c, Text: h.perCommand[c]})
		}
	}
	// The dynamic AAAA-only classifier (hints_dns.go) builds its text outside
	// the registry, so it would otherwise be the one operator-facing hint the
	// gate never sees. Rendered against a placeholder host — the host name is
	// the only part that varies.
	dns := dnsHintFor("db.example.invalid")
	out = append(out, HintText{Code: dns.code, Text: dns.hint})
	return out
}
