// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

// JSON result envelopes for the primary verbs — `--format json` on
// `migrate`, `sync start`, `backup full`, and `restore` (docs/research/
// ai-friendly-sluice.md recommendation #2, mirroring planetscale/cli#1280's
// typed-envelope + next-steps idea).
//
// Emission contract: in json mode exactly ONE JSON object is written to
// stdout, as the last thing the command writes there, on every exit
// path — completed, refused, and failed. The slog stream stays on
// stderr untouched, and kong's fatal handler in main.go keeps printing
// the human error line to stderr on failure — stdout carries the
// envelope alone, so `sluice migrate --format json | jq .` always
// parses. Text mode is byte-identical to before this layer existed
// (finish is a pass-through).
//
// Status classification: "refused" covers errors returned BEFORE the
// pipeline engaged (CLI-side flag/config validation and preflight
// resolution — sluice declined before touching any data work) AND any
// error whose chain carries a ClassRefusal [sluicecode.CodedError]
// (the loud-failure refusals: populated-target cold-start, extension
// gates, value refusals, …) regardless of engagement — keeping the
// envelope status consistent with the exit-code taxonomy, where a
// refusal exits 3. "failed" covers everything else after markEngaged.
// Uncoded pipeline errors classify by the engagement boundary alone.
//
// Redaction: no envelope field is built from a DSN — engines are names
// only, plan hosts are pre-redacted, next-step suggestions use
// placeholder DSNs. The one path that could echo a credential is the
// error message (drivers and config errors sometimes embed the DSN
// they failed on), so finish scrubs every registered DSN out of the
// rendered JSON, replacing it with the credential-free locator
// [diagnose.RedactDSN] produces — the same redaction contract as the
// crash bundle.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Envelope status values. Stable strings — agents branch on them.
const (
	envelopeStatusCompleted = "completed"
	envelopeStatusRefused   = "refused"
	envelopeStatusFailed    = "failed"
)

// resultEnvelope is the terminal `--format json` object. Fields are
// omitted when empty so each verb emits only what it knows (backup has
// no target engine; sync start has no per-table stats).
type resultEnvelope struct {
	Command        string          `json:"command"`
	Status         string          `json:"status"`
	ElapsedSeconds float64         `json:"elapsed_seconds"`
	SourceEngine   string          `json:"source_engine,omitempty"`
	TargetEngine   string          `json:"target_engine,omitempty"`
	Tables         []envelopeTable `json:"tables,omitempty"`
	Resume         *envelopeResume `json:"resume,omitempty"`
	Error          *envelopeError  `json:"error,omitempty"`
	NextSteps      []string        `json:"next_steps,omitempty"`
}

// envelopeTable is one table's end-of-run stats. Rows is a pointer so
// "unknown" (absent) is distinguishable from a genuine 0 — the same
// no-silent-ambiguity discipline as PlanTable's -1 sentinel.
type envelopeTable struct {
	Schema string `json:"schema,omitempty"`
	Name   string `json:"name"`
	Rows   *int64 `json:"rows,omitempty"`
}

// envelopeResume tells an agent whether (and how) an interrupted run
// of this verb can be resumed.
type envelopeResume struct {
	Supported bool   `json:"supported"`
	Hint      string `json:"hint,omitempty"`
}

// envelopeError carries the failure detail. Message is the same error
// text the human path prints to stderr (post DSN-scrub). The planned
// error-code chunk adds a stable `code` field alongside it.
type envelopeError struct {
	Message string `json:"message"`
	// Code and Hint surface the stable SLUICE-E-* error code and its
	// remedy when the failure carries one (docs/operator/error-codes.md);
	// absent for uncoded errors.
	Code string `json:"code,omitempty"`
	Hint string `json:"hint,omitempty"`
}

// planEnvelope is the `--dry-run --format json` shape: the plan
// replaces the result envelope as the command's single stdout object
// (the plan IS the result of a dry run). Plan is a
// *pipeline.MigrationPlan or *pipeline.StreamPlan.
type planEnvelope struct {
	Command string `json:"command"`
	DryRun  bool   `json:"dry_run"`
	Plan    any    `json:"plan"`
}

// envelopeRun threads the envelope state through one command Run().
// Construct with newEnvelopeRun, feed it facts as they resolve, and
// return env.finish(err) from Run — finish renders the single stdout
// object in json mode and is a pure pass-through in text mode.
type envelopeRun struct {
	command   string
	jsonMode  bool
	start     time.Time
	out       io.Writer // os.Stdout; swapped by tests
	srcEngine string
	dstEngine string
	scrubDSNs []string
	resume    *envelopeResume
	nextSteps []string
	summary   *pipeline.RunSummary
	plan      any
	engaged   bool
}

// newEnvelopeRun starts the envelope clock for command. format is the
// verb's --format flag value (kong's enum already restricts it to
// text|json). The summary collector exists only in json mode so text
// mode pays zero bookkeeping.
func newEnvelopeRun(command, format string) *envelopeRun {
	e := &envelopeRun{
		command:  command,
		jsonMode: strings.EqualFold(strings.TrimSpace(format), "json"),
		start:    time.Now(),
		out:      os.Stdout,
	}
	if e.jsonMode {
		e.summary = &pipeline.RunSummary{}
	}
	return e
}

// setEngines records the resolved engine names (names only — never
// DSNs).
func (e *envelopeRun) setEngines(source, target string) {
	e.srcEngine = source
	e.dstEngine = target
}

// scrub registers DSNs to be redacted out of the rendered envelope.
// Empty strings are ignored.
func (e *envelopeRun) scrub(dsns ...string) {
	for _, d := range dsns {
		if d != "" {
			e.scrubDSNs = append(e.scrubDSNs, d)
		}
	}
}

// setResume declares the verb's resume affordance.
func (e *envelopeRun) setResume(supported bool, hint string) {
	e.resume = &envelopeResume{Supported: supported, Hint: hint}
}

// setNextSteps records the static post-success suggestions (rendered
// only on a completed run). Steps must use placeholder DSNs — never
// real ones.
func (e *envelopeRun) setNextSteps(steps ...string) {
	e.nextSteps = steps
}

// markEngaged flips the refused→failed classification boundary: every
// error returned after this point is a runtime failure, not a
// pre-work refusal. Call it immediately before handing control to the
// pipeline orchestrator.
func (e *envelopeRun) markEngaged() {
	e.engaged = true
}

// captureMigratePlan is the Migrator.PlanSink hookup. Multi-database
// fan-out fires the sink once per database; captures after the first
// merge into it.
func (e *envelopeRun) captureMigratePlan(p *pipeline.MigrationPlan) {
	if prev, ok := e.plan.(*pipeline.MigrationPlan); ok {
		prev.Tables = append(prev.Tables, p.Tables...)
		prev.Views += p.Views
		return
	}
	e.plan = p
}

// captureStreamPlan is the Streamer.PlanSink hookup.
func (e *envelopeRun) captureStreamPlan(p *pipeline.StreamPlan) {
	e.plan = p
}

// finish renders the single stdout JSON object (json mode) and returns
// err unchanged so kong's exit-code path still fires. In text mode it
// is a pure pass-through. A captured dry-run plan replaces the result
// envelope on the success path; a dry-run failure still gets the
// failure envelope so agents always receive exactly one object.
func (e *envelopeRun) finish(err error) error {
	if !e.jsonMode {
		return err
	}
	var doc any
	if err == nil && e.plan != nil {
		doc = planEnvelope{Command: e.command, DryRun: true, Plan: e.plan}
	} else {
		doc = e.buildResult(err)
	}
	// Encode without HTML escaping so a DSN's `&`-joined query params
	// survive verbatim into the rendered text and the scrubText pass
	// below can't be defeated by & escaping.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if merr := enc.Encode(doc); merr != nil {
		// Plain-data marshalling can't realistically fail; if it ever
		// does, be loud on stderr rather than silently swallowing the
		// envelope, and never mask the run's own error.
		fmt.Fprintf(os.Stderr, "sluice: render --format json envelope: %v\n", merr)
		return err
	}
	fmt.Fprint(e.out, e.scrubText(buf.String()))
	return err
}

// buildResult assembles the result envelope for finish.
func (e *envelopeRun) buildResult(err error) resultEnvelope {
	env := resultEnvelope{
		Command:        e.command,
		Status:         envelopeStatusCompleted,
		ElapsedSeconds: time.Since(e.start).Seconds(),
		SourceEngine:   e.srcEngine,
		TargetEngine:   e.dstEngine,
		Resume:         e.resume,
	}
	for _, t := range e.summary.Tables() {
		env.Tables = append(env.Tables, envelopeTable{Schema: t.Schema, Name: t.Name, Rows: t.Rows})
	}
	if err != nil {
		env.Status = envelopeStatusFailed
		if !e.engaged {
			env.Status = envelopeStatusRefused
		}
		env.Error = &envelopeError{Message: e.scrubText(err.Error())}
		// A coded error lifts its stable code + remedy hint into the
		// envelope, and a ClassRefusal reclassifies the status even
		// after engagement — keeping the envelope consistent with the
		// exit-code taxonomy (a refusal exits 3; an agent must never
		// see exit 3 alongside status "failed"). Uncoded errors keep
		// the engagement-boundary classification above.
		if ce, ok := sluicecode.FromError(err); ok {
			env.Error.Code = string(ce.Code)
			env.Error.Hint = e.scrubText(ce.Hint)
			if info, known := sluicecode.Describe(ce.Code); known && info.Class == sluicecode.ClassRefusal {
				env.Status = envelopeStatusRefused
			}
		}
		return env
	}
	env.NextSteps = e.nextSteps
	return env
}

// scrubText replaces every registered DSN in s with the
// credential-free locator [diagnose.RedactDSN] produces — the final
// no-DSN-leaves-the-envelope check.
func (e *envelopeRun) scrubText(s string) string {
	for _, dsn := range e.scrubDSNs {
		if strings.Contains(s, dsn) {
			s = strings.ReplaceAll(s, dsn, diagnose.RedactDSN(dsn))
		}
	}
	return s
}
