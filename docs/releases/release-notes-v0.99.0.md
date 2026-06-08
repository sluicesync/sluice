# sluice v0.99.0

## v0.99.0 — sluice's new home + an official container image

This is the "new home" release. sluice moved to its own GitHub organization and a vanity module path, and now ships a multi-arch container image. There are **no functional engine changes** from v0.98.1 — the connection-resilience and index-build work shipped in v0.98.0 / v0.98.1.

### New home

- **Repository:** `github.com/orware/sluice` → **`github.com/sluicesync/sluice`**.
- **Module path:** now the vanity path **`sluicesync.dev/sluice`** (served via GitHub Pages, so the import path is decoupled from the host and won't break on any future move).

```bash
go install sluicesync.dev/sluice/cmd/sluice@latest
```

GitHub's transfer redirect keeps the old `github.com/orware/sluice` URLs working, and existing `go get github.com/orware/sluice@<oldtag>` pins still resolve — but new code should use the vanity path. The prebaked CI test images moved to `ghcr.io/sluicesync/sluice-*` as well.

### Added — official runtime container image

`ghcr.io/sluicesync/sluice:0.99.0` and `:latest` — multi-arch (**linux/amd64 + linux/arm64**), a **distroless** image wrapping the static binary (no shell, runs non-root, tiny). Run sluice as a container instead of managing the binary on a host:

- **Kubernetes Deployment** for `sync start`, behind the `/healthz` / `/readyz` / `/metrics` endpoints (`--metrics-listen`)
- **CronJobs** for `backup`, `cutover`, or `matview refresh`
- **One-shot CI step** for `migrate`

The operator guide ([`docs/operator/running-as-a-service.md`](https://github.com/sluicesync/sluice/blob/main/docs/operator/running-as-a-service.md)) ships the compose + k8s manifests. GoReleaser publishes the image on every tagged release.

### Compatibility

- **No CLI, config, IR, or behavior changes.** Existing binaries, configs, and pipelines are unaffected.
- **Old import path keeps working** via GitHub's redirect; the canonical path is now `sluicesync.dev/sluice`.

### Who needs this

- **Anyone scripting against the Go module** should move to `go install sluicesync.dev/sluice/...`.
- **Container / Kubernetes operators** get a first-class image to deploy the continuous-sync daemon and scheduled jobs.
- **Everyone else** can keep using the same binaries and commands.
