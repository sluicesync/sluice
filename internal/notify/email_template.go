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
func renderEmail(n Notification) (subject, htmlBody, textBody string, err error) {
	v := newEmailView(n)
	subject = emailSubject(n)
	var hb bytes.Buffer
	if err := emailHTMLTemplate.Execute(&hb, v); err != nil {
		return "", "", "", fmt.Errorf("render html email: %w", err)
	}
	var tb bytes.Buffer
	if err := emailTextTemplate.Execute(&tb, v); err != nil {
		return "", "", "", fmt.Errorf("render text email: %w", err)
	}
	return subject, hb.String(), tb.String(), nil
}

// emailSubject is the one-line subject: "[sluice] <metric> threshold alert:
// <stream>". The stream segment is dropped when the id is empty so the
// subject never trails a dangling colon.
func emailSubject(n Notification) string {
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
