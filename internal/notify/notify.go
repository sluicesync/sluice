// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package notify is the engine-neutral notification-sink layer for the
// ADR-0107 item-36 sync-scoped target-metrics alerter. It defines a tiny
// [Notifier] surface plus the two shipped sinks — a generic JSON-POST
// webhook ([WebhookNotifier]) and a Slack incoming-webhook
// ([SlackNotifier]) — and a fan-out [MultiNotifier] that delivers to all
// configured sinks with FAILURE ISOLATION: one dead sink never blocks the
// others.
//
// Deliberately standalone: this package imports NO engine package and NOT
// `internal/ir`. The pipeline alerter maps its ir/telemetry view into a
// plain [Notification] and hands it here, keeping the sink layer generic
// (the same posture the telemetry provider keeps for the metrics seam).
// Outbound notifications are an OPTIONAL, credential-gated, advisory
// integration surface — never on the value path, never able to stall or
// crash a sync (the caller logs+swallows any error this layer returns).
package notify

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Level classifies a notification's severity. The alerter picks the level
// per metric (storage/lag breaches are the more urgent class; cpu/mem are
// warnings) and the Slack sink renders a different emoji per level.
type Level string

const (
	// LevelWarning is the advisory class (e.g. CPU/memory saturation): act
	// soon, but the sync rides through.
	LevelWarning Level = "warning"

	// LevelCritical is the urgent class (e.g. storage approaching capacity,
	// replica lag spiking): an operator likely wants to look now.
	LevelCritical Level = "critical"
)

// Category classifies what KIND of event a notification carries, so the
// sinks can render the right shape. A threshold alert has a numeric
// "V ≥ T" reading; a schema-drift alert (ADR-0157) is a discrete stall
// event with no numeric threshold — just a title + body. The zero value
// ("") is treated as [CategoryThreshold] everywhere, so every existing
// metrics notification (which does not set the field) renders byte-for-byte
// as before.
type Category string

const (
	// CategoryThreshold is the metrics-alerter class: a metric reading
	// breached its configured limit. The default/zero-equivalent — an empty
	// Category is treated as threshold by every sink.
	CategoryThreshold Category = "threshold"

	// CategorySchemaDrift is the ADR-0157 class: a source DDL stalled the
	// sync. No numeric reading; the sinks render Title + Body (the drift
	// detail + recovery steps) without the "V ≥ T" line.
	CategorySchemaDrift Category = "schema-drift"

	// CategorySlotHealth is the ADR-0059 class (roadmap item 64a): the
	// source Postgres replication slot crossed a health threshold — WAL
	// retention pressure against max_slot_wal_keep_size, or prolonged
	// inactivity. No numeric reading; the sinks render Title + Body (the
	// slot facts + remediation) without the "V ≥ T" line.
	CategorySlotHealth Category = "slot-health"
)

// IsSchemaDrift reports whether c is the schema-drift category. Kept as a
// helper (rather than an == comparison at each sink) so the "empty ⇒
// threshold" contract lives in one place: only an explicit
// [CategorySchemaDrift] takes the event-shaped rendering, and every other
// value (including the zero value) renders as a threshold alert.
func (c Category) IsSchemaDrift() bool { return c == CategorySchemaDrift }

// IsSlotHealth reports whether c is the slot-health category. Same posture
// as [Category.IsSchemaDrift]: only the explicit value takes the
// event-shaped rendering.
func (c Category) IsSlotHealth() bool { return c == CategorySlotHealth }

// IsEvent reports whether c is one of the EVENT-shaped categories
// (schema-drift, slot-health): a discrete operational event with no
// numeric "V ≥ T" reading, rendered by the sinks as Title + Body. Every
// other value — including the zero value — renders as a threshold alert,
// so the pre-Category metrics notifications stay byte-for-byte unchanged.
func (c Category) IsEvent() bool { return c.IsSchemaDrift() || c.IsSlotHealth() }

// Notification is one operator-facing alert. It is a plain, engine-neutral
// value: the pipeline alerter fills it from a telemetry snapshot, the
// sinks render it to their wire shape. For [CategoryThreshold] (the
// default) Value/Threshold carry the breached metric reading + the
// configured limit so a sink can compose a readable "0.91 ≥ 0.90" line;
// for [CategorySchemaDrift] there is no numeric reading and the sinks
// render Title + Body only.
type Notification struct {
	Level     Level
	Category  Category
	StreamID  string
	Metric    string
	Title     string
	Body      string
	Value     float64
	Threshold float64
	At        time.Time
}

// Notifier is a single notification sink. Notify delivers one alert; it
// MUST honour ctx for cancellation and bound its own I/O so a hung sink
// can't wedge the alerter. Name identifies the sink in logs.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
	Name() string
}

// MultiNotifier fans one notification out to every configured sink. Its
// Notify NEVER short-circuits: it calls every sink even if an earlier one
// errored, so a single dead webhook can't suppress delivery to a healthy
// one (the load-bearing failure-isolation contract). It returns a joined
// error of every sink failure (nil if all succeeded); the caller (the
// pipeline alerter) logs+swallows it — a notification failure must never
// affect the sync.
type MultiNotifier []Notifier

// NewMultiNotifier builds a MultiNotifier from the supplied sinks, dropping
// any nil entries (so a caller can pass the result of an "if configured"
// chain without guarding each one). Returns nil when no non-nil sink
// remains, so the alerter's "no notifier ⇒ no-op" guard stays exact.
func NewMultiNotifier(sinks ...Notifier) MultiNotifier {
	var out MultiNotifier
	for _, s := range sinks {
		if s != nil {
			out = append(out, s)
		}
	}
	return out
}

// Notify delivers n to every sink, collecting errors without
// short-circuiting. A nil/empty MultiNotifier is a no-op (returns nil).
func (m MultiNotifier) Notify(ctx context.Context, n Notification) error {
	var errs []error
	for _, s := range m {
		if s == nil {
			continue
		}
		if err := s.Notify(ctx, n); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Name reports the aggregate sink name (for symmetry with [Notifier]; the
// alerter logs individual failures via the joined error).
func (m MultiNotifier) Name() string { return "multi" }
