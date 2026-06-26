# Email alert samples

These are **rendered previews** of the emails the SMTP notification sink
(`--notify-smtp-*`, roadmap item 48) sends when a threshold alert fires. Open
the `.html` files in any browser to see what lands in an operator's inbox; the
`.txt` files are the plaintext alternative (what a text-only mail client
shows). Both bodies carry the same facts — which rule fired, the metric value
vs the threshold, the stream id, and the timestamp.

| File | Alert |
|------|-------|
| `storage-util.{html,txt}` | Critical — target storage approaching capacity (ADR-0107) |
| `sync-lag.{html,txt}` | Critical — sluice's own sync lag high (roadmap item 45) |
| `cpu-util.{html,txt}` | Warning — target CPU saturating (ADR-0107) |

These are **generated**, not hand-written, so they never drift from the live
template (`internal/notify/email_template.go`). To regenerate after a template
change, from the repo root:

```sh
SLUICE_WRITE_EMAIL_SAMPLES=docs/operator/email-samples \
  go test -run TestRenderEmailSamples ./internal/notify/...
```

A normal `go test ./...` run renders the template (for coverage) but does
**not** write these files — only the env var above triggers the write.
