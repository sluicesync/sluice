// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"bytes"
	"fmt"
	"html/template"
	"strconv"
	"strings"
	texttemplate "text/template"
	"time"
)

// Email rendering for the SMTP sink. One [Notification] renders to three
// parts: a one-line subject, an HTML body (the rich part), and a plaintext
// body (the universal fallback). Both bodies carry the SAME facts — which
// rule fired, the metric value vs the threshold, the stream id, and the
// timestamp — so a mail client that shows only text loses no information.
//
// Kept deliberately minimal: this is an ops alert, not marketing. The HTML
// uses inline styles (the only styling broadly honoured across mail
// clients) and a single accent colour keyed off the [Level].

// emailView is the flattened, template-friendly projection of a
// [Notification]. Rendering goes through this struct (not the Notification
// directly) so the templates stay simple and the number/time formatting is
// done once in Go, where it is testable.
type emailView struct {
	Level      string
	LevelLabel string // upper-cased, human ("CRITICAL" / "WARNING")
	Accent     string // hex colour keyed off the level
	StreamID   string
	Metric     string
	Title      string
	Body       string
	Value      string // pre-formatted
	Threshold  string // pre-formatted
	At         string // RFC3339, UTC
}

// emailAccentCritical / emailAccentWarning are the single accent colour per
// level — a restrained red for critical, amber for warning. Used for the
// header bar and the metric value so the severity reads at a glance.
const (
	emailAccentCritical = "#b3261e"
	emailAccentWarning  = "#9a6700"
)

// renderEmail produces the (subject, htmlBody, textBody) for one alert. It is
// the single rendering entry point shared by the live SMTP sink and the
// committed sample-preview generator, so what an operator previews is exactly
// what the relay sends.
//
// The rendering is category-dependent: the event categories have no
// numeric reading, so each uses a distinct subject + templates that state
// the Title and the Body without the "V ≥ T" line and the metric facts —
// schema-drift (ADR-0157) carries the drift detail + recovery steps,
// slot-health (ADR-0059, roadmap item 64a) the slot facts + remediation.
// A threshold alert (empty or CategoryThreshold) renders through the
// original templates, byte-for-byte unchanged.
func renderEmail(n Notification) (subject, htmlBody, textBody string, err error) {
	v := newEmailView(n)
	htmlTmpl, textTmpl := emailHTMLTemplate, emailTextTemplate
	switch {
	case n.Category.IsSchemaDrift():
		htmlTmpl, textTmpl = emailHTMLTemplateSchemaDrift, emailTextTemplateSchemaDrift
	case n.Category.IsSlotHealth():
		htmlTmpl, textTmpl = emailHTMLTemplateSlotHealth, emailTextTemplateSlotHealth
	}
	subject = emailSubject(n)
	var hb bytes.Buffer
	if err := htmlTmpl.Execute(&hb, v); err != nil {
		return "", "", "", fmt.Errorf("render html email: %w", err)
	}
	var tb bytes.Buffer
	if err := textTmpl.Execute(&tb, v); err != nil {
		return "", "", "", fmt.Errorf("render text email: %w", err)
	}
	return subject, hb.String(), tb.String(), nil
}

// emailSubject is the one-line subject. For a threshold alert it is
// "[sluice] <metric> threshold alert: <stream>"; for a schema-drift alert
// it is "[sluice] schema change stalled sync: <stream>" (no metric); for a
// slot-health alert it is "[sluice] replication slot health alert:
// <stream>". The stream segment is dropped when the id is empty so the
// subject never trails a dangling colon.
func emailSubject(n Notification) string {
	if n.Category.IsSchemaDrift() {
		s := "[sluice] schema change stalled sync"
		if n.StreamID != "" {
			s += ": " + n.StreamID
		}
		return s
	}
	if n.Category.IsSlotHealth() {
		s := "[sluice] replication slot health alert"
		if n.StreamID != "" {
			s += ": " + n.StreamID
		}
		return s
	}
	s := fmt.Sprintf("[sluice] %s threshold alert", n.Metric)
	if n.StreamID != "" {
		s += ": " + n.StreamID
	}
	return s
}

func newEmailView(n Notification) emailView {
	accent := emailAccentWarning
	if n.Level == LevelCritical {
		accent = emailAccentCritical
	}
	at := n.At
	if at.IsZero() {
		at = time.Now()
	}
	return emailView{
		Level:      string(n.Level),
		LevelLabel: strings.ToUpper(string(n.Level)),
		Accent:     accent,
		StreamID:   n.StreamID,
		Metric:     n.Metric,
		Title:      n.Title,
		Body:       n.Body,
		Value:      formatMetricNumber(n.Value),
		Threshold:  formatMetricNumber(n.Threshold),
		At:         at.UTC().Format(time.RFC3339),
	}
}

// formatMetricNumber renders a metric reading compactly (4 significant
// digits, no trailing zero noise) — the same precision the shared
// [formatThresholdBody] body uses, so the email value matches the body line.
func formatMetricNumber(f float64) string {
	return strconv.FormatFloat(f, 'g', 4, 64)
}

// emailHTMLTemplate is the rich body: a centred card with a level-coloured
// header, the breached "value ≥ threshold" stated prominently, and a small
// facts table. Inline styles only (mail clients strip <style> blocks).
var emailHTMLTemplate = template.Must(template.New("email.html").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{.Metric}} threshold alert</title>
</head>
<body style="margin:0; padding:0; background-color:#f4f5f7; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif; color:#1f2328;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f5f7; padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="520" cellpadding="0" cellspacing="0" style="max-width:520px; width:100%; background-color:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
<tr>
<td style="background-color:{{.Accent}}; padding:18px 28px;">
<div style="font-size:12px; font-weight:700; letter-spacing:0.12em; color:#ffffff; opacity:0.85;">SLUICE &middot; {{.LevelLabel}} ALERT</div>
<div style="font-size:20px; font-weight:700; color:#ffffff; margin-top:4px;">{{.Title}}</div>
</td>
</tr>
<tr>
<td style="padding:28px;">
<div style="font-size:15px; line-height:1.5; color:#3a3f45;">
The <strong>{{.Metric}}</strong> threshold was breached on stream
<strong>{{.StreamID}}</strong>.
</div>
<div style="margin:22px 0; padding:16px 18px; background-color:#f7f8fa; border-left:4px solid {{.Accent}}; border-radius:4px;">
<span style="font-size:26px; font-weight:700; color:{{.Accent}};">{{.Value}}</span>
<span style="font-size:15px; color:#6a737d;"> &ge; {{.Threshold}} (threshold)</span>
</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px; color:#3a3f45; border-collapse:collapse;">
<tr><td style="padding:6px 0; color:#6a737d; width:130px;">Metric</td><td style="padding:6px 0; font-weight:600;">{{.Metric}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Value</td><td style="padding:6px 0; font-weight:600;">{{.Value}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Threshold</td><td style="padding:6px 0; font-weight:600;">{{.Threshold}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Stream</td><td style="padding:6px 0; font-weight:600;">{{.StreamID}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Severity</td><td style="padding:6px 0; font-weight:600;">{{.LevelLabel}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Time (UTC)</td><td style="padding:6px 0; font-weight:600;">{{.At}}</td></tr>
</table>
</td>
</tr>
<tr>
<td style="padding:16px 28px; background-color:#fafbfc; border-top:1px solid #eaecef; font-size:12px; line-height:1.5; color:#8a929b;">
Advisory alert from sluice. Notifications are failure-isolated &mdash; a delivery
problem never affects the running sync. Re-fires are rate-limited per the
configured cooldown until the metric recovers below its threshold.
</td>
</tr>
</table>
</td></tr>
</table>
</body>
</html>
`))

// emailTextTemplate is the plaintext fallback: the same facts, line-oriented,
// readable in any client that won't render HTML.
var emailTextTemplate = texttemplate.Must(texttemplate.New("email.txt").Parse(`[sluice] {{.LevelLabel}} ALERT — {{.Title}}

The {{.Metric}} threshold was breached on stream {{.StreamID}}.

    {{.Value}} >= {{.Threshold}} (threshold)

  Metric:      {{.Metric}}
  Value:       {{.Value}}
  Threshold:   {{.Threshold}}
  Stream:      {{.StreamID}}
  Severity:    {{.LevelLabel}}
  Time (UTC):  {{.At}}

Advisory alert from sluice. Notifications are failure-isolated — a delivery
problem never affects the running sync. Re-fires are rate-limited per the
configured cooldown until the metric recovers below its threshold.
`))

// emailHTMLTemplateSchemaDrift is the schema-drift (ADR-0157) HTML body: the
// same card chrome as the threshold email, but stating the stall Title and
// the drift/recovery Body instead of the "value ≥ threshold" block + metric
// facts. The Body carries the ADR-0060 drift detail and the recovery steps
// verbatim; it is preformatted text, so it is rendered inside a <pre> so the
// multi-line recovery hint keeps its shape.
var emailHTMLTemplateSchemaDrift = template.Must(template.New("email_schema_drift.html").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Schema change stalled sync</title>
</head>
<body style="margin:0; padding:0; background-color:#f4f5f7; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif; color:#1f2328;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f5f7; padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="520" cellpadding="0" cellspacing="0" style="max-width:520px; width:100%; background-color:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
<tr>
<td style="background-color:{{.Accent}}; padding:18px 28px;">
<div style="font-size:12px; font-weight:700; letter-spacing:0.12em; color:#ffffff; opacity:0.85;">SLUICE &middot; {{.LevelLabel}} ALERT</div>
<div style="font-size:20px; font-weight:700; color:#ffffff; margin-top:4px;">{{.Title}}</div>
</td>
</tr>
<tr>
<td style="padding:28px;">
<div style="font-size:15px; line-height:1.5; color:#3a3f45;">
A source schema change stalled sync <strong>{{.StreamID}}</strong>. No data
was lost — the stream halted at the change boundary and will not advance
until the drift is resolved. Details and recovery steps:
</div>
<div style="margin:22px 0; padding:16px 18px; background-color:#f7f8fa; border-left:4px solid {{.Accent}}; border-radius:4px;">
<pre style="margin:0; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:13px; line-height:1.5; color:#3a3f45; white-space:pre-wrap; word-break:break-word;">{{.Body}}</pre>
</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px; color:#3a3f45; border-collapse:collapse;">
<tr><td style="padding:6px 0; color:#6a737d; width:130px;">Stream</td><td style="padding:6px 0; font-weight:600;">{{.StreamID}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Severity</td><td style="padding:6px 0; font-weight:600;">{{.LevelLabel}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Time (UTC)</td><td style="padding:6px 0; font-weight:600;">{{.At}}</td></tr>
</table>
</td>
</tr>
<tr>
<td style="padding:16px 28px; background-color:#fafbfc; border-top:1px solid #eaecef; font-size:12px; line-height:1.5; color:#8a929b;">
Advisory alert from sluice. Notifications are failure-isolated &mdash; a delivery
problem never affects the running sync. This alert fires once per distinct
schema-change stall.
</td>
</tr>
</table>
</td></tr>
</table>
</body>
</html>
`))

// emailTextTemplateSchemaDrift is the schema-drift plaintext fallback: the
// same facts line-oriented, with the drift/recovery Body reproduced verbatim.
var emailTextTemplateSchemaDrift = texttemplate.Must(texttemplate.New("email_schema_drift.txt").Parse(`[sluice] {{.LevelLabel}} ALERT — {{.Title}}

A source schema change stalled sync {{.StreamID}}. No data was lost — the
stream halted at the change boundary and will not advance until the drift is
resolved. Details and recovery steps:

{{.Body}}

  Stream:      {{.StreamID}}
  Severity:    {{.LevelLabel}}
  Time (UTC):  {{.At}}

Advisory alert from sluice. Notifications are failure-isolated — a delivery
problem never affects the running sync. This alert fires once per distinct
schema-change stall.
`))

// emailHTMLTemplateSlotHealth is the slot-health (ADR-0059, roadmap item
// 64a) HTML body: the same card chrome as the threshold email, but stating
// the crossing Title and the facts/remediation Body instead of the
// "value ≥ threshold" block + metric facts. The Body carries the slot name,
// the retention/inactivity reading, and the remediation verbatim; it is
// preformatted text, so it is rendered inside a <pre> so the multi-line
// remediation keeps its shape.
var emailHTMLTemplateSlotHealth = template.Must(template.New("email_slot_health.html").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Replication slot health alert</title>
</head>
<body style="margin:0; padding:0; background-color:#f4f5f7; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif; color:#1f2328;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f5f7; padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="520" cellpadding="0" cellspacing="0" style="max-width:520px; width:100%; background-color:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
<tr>
<td style="background-color:{{.Accent}}; padding:18px 28px;">
<div style="font-size:12px; font-weight:700; letter-spacing:0.12em; color:#ffffff; opacity:0.85;">SLUICE &middot; {{.LevelLabel}} ALERT</div>
<div style="font-size:20px; font-weight:700; color:#ffffff; margin-top:4px;">{{.Title}}</div>
</td>
</tr>
<tr>
<td style="padding:28px;">
<div style="font-size:15px; line-height:1.5; color:#3a3f45;">
The source replication slot for sync <strong>{{.StreamID}}</strong> crossed a
health threshold. The sync is still running, but if the condition persists
Postgres can invalidate the slot (wal_status &rarr; 'lost') and a fresh
re-snapshot would be the only recovery. Details and remediation:
</div>
<div style="margin:22px 0; padding:16px 18px; background-color:#f7f8fa; border-left:4px solid {{.Accent}}; border-radius:4px;">
<pre style="margin:0; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:13px; line-height:1.5; color:#3a3f45; white-space:pre-wrap; word-break:break-word;">{{.Body}}</pre>
</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px; color:#3a3f45; border-collapse:collapse;">
<tr><td style="padding:6px 0; color:#6a737d; width:130px;">Stream</td><td style="padding:6px 0; font-weight:600;">{{.StreamID}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Severity</td><td style="padding:6px 0; font-weight:600;">{{.LevelLabel}}</td></tr>
<tr><td style="padding:6px 0; color:#6a737d;">Time (UTC)</td><td style="padding:6px 0; font-weight:600;">{{.At}}</td></tr>
</table>
</td>
</tr>
<tr>
<td style="padding:16px 28px; background-color:#fafbfc; border-top:1px solid #eaecef; font-size:12px; line-height:1.5; color:#8a929b;">
Advisory alert from sluice. Notifications are failure-isolated &mdash; a delivery
problem never affects the running sync. Re-fires are rate-limited while the
condition persists and re-arm when it clears.
</td>
</tr>
</table>
</td></tr>
</table>
</body>
</html>
`))

// emailTextTemplateSlotHealth is the slot-health plaintext fallback: the
// same facts line-oriented, with the facts/remediation Body reproduced
// verbatim.
var emailTextTemplateSlotHealth = texttemplate.Must(texttemplate.New("email_slot_health.txt").Parse(`[sluice] {{.LevelLabel}} ALERT — {{.Title}}

The source replication slot for sync {{.StreamID}} crossed a health
threshold. The sync is still running, but if the condition persists Postgres
can invalidate the slot (wal_status -> 'lost') and a fresh re-snapshot would
be the only recovery. Details and remediation:

{{.Body}}

  Stream:      {{.StreamID}}
  Severity:    {{.LevelLabel}}
  Time (UTC):  {{.At}}

Advisory alert from sluice. Notifications are failure-isolated — a delivery
problem never affects the running sync. Re-fires are rate-limited while the
condition persists and re-arm when it clears.
`))
