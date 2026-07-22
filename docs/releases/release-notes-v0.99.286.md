# sluice v0.99.286

**A continuous `sync` now rides out routine transient source-side errors instead of exiting.** Found by a multi-day soak against real PlanetScale and Cloudflare D1: three ordinary network/upstream blips each terminated a healthy long-running stream. Nothing was ever at risk of data loss — every failure was loud, the CDC position is durable, and every restart warm-resumed cleanly — but a sync that needs a manual restart after each blip isn't operationally usable. If you run continuous sync for days at a time, upgrade.

## Fixed

sluice's pipeline retries a CDC reader failure only when the error is classified as retriable (the `ir.RetriableError` interface). The retry machinery was working; the gap was **classification coverage** on the source-side read. Three real shapes were falling through as terminal — each observed killing a multi-day soak against real infrastructure:

- **PlanetScale / Vitess (VStream).** A long-lived stream drop surfaces as gRPC `code = Internal desc = server closed the stream without sending trailers`. `Internal` is deliberately *not* in the retriable code set — a genuine vtgate fault must stay loud — so the transport-level abnormal-close wordings (`server closed the stream without sending trailers`, `unexpected EOF`, `stream terminated by RST_STREAM`) are now discriminated and classified retriable, while a real `Internal` fault still fails fast.
- **Cloudflare D1 (`d1-trigger` / `sqlite-trigger`).** The D1 HTTP transport had no classification at all, so a `net/http: TLS handshake timeout` — or a Cloudflare-side `HTTP 500` — ended the stream. Transport failures (EOF, connection reset, broken pipe, i/o timeout, TLS handshake timeout, temporary DNS failure) and the transient status family (408, 429, 500, 502, 503, 504) are now retried. A 4xx meaning the *request* is wrong (bad token, missing database, malformed statement) — and a DNS `no such host`, which means a typo'd endpoint — stay terminal, so a real misconfiguration still fails loudly instead of burning the retry budget.
- **Postgres trigger-CDC (`pgtrigger`).** The same structural gap: a bare poll error with no classification. Transport-level transients are now classified.

The D1 and pgtrigger classifiers are one shared, pinned implementation (`internal/engines/internal/triggercdc`) rather than two per-engine copies, so the transient set can't drift apart between the trigger-CDC engines.

## Compatibility

- **No behavior change for any path that was already succeeding, and no widening of what sluice tolerates silently.** Every newly-retried shape previously *terminated the process with a loud error*; it is now retried loudly (logged) instead. Nothing that used to fail loudly now fails quietly.
- **The retry budget is unchanged** (`--apply-retry-attempts`, default 8, with the existing backoff). A genuinely persistent outage still exhausts the budget and fails loudly rather than spinning forever.
- **Known gap (follow-up):** pgtrigger SQLSTATE-level transients — `57P01` (admin shutdown), `57P03` (cannot connect now), `08006` (connection failure) — are *not* yet classified. A Postgres failover during a trigger-CDC poll can still terminate the stream; that path is tracked for a subsequent release.

## Also in this release

- **New operator guide: [staged ("wave") migration](https://github.com/sluicesync/sluice/blob/main/docs/operator/staged-wave-migration.md)** — moving a database a few tables at a time, which mechanism to use on which source engine (they are *not* interchangeable — there's a Postgres caveat), how foreign keys constrain wave ordering, and the one thing sluice deliberately does not do.
- **ADR-0175 through ADR-0178** — Postgres publication scope isolation (`--publication-name` and the clobber refusal), publication row-filter push-down, a publication column-list capability survey (no adoption), and a design record for an analytical target class (`ir.ChangeSink`).

## Who needs this

Anyone running `sluice sync` continuously for more than a few hours — especially against **PlanetScale/Vitess** or **Cloudflare D1**, where the observed failures came from. If your syncs are short-lived, or you only use `sluice migrate`, there's no functional reason to upgrade.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.286
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.286`
