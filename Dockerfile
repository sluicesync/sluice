# Runtime image for the sluice binary.
#
# GoReleaser builds the static (CGO_ENABLED=0) binary and COPYs it in —
# there is no in-container compilation here. The base is distroless
# "static": no shell, no package manager, ~2 MB, runs as a non-root user.
#
# Orchestrator health/readiness is via the HTTP endpoints that
# `sluice sync start --metrics-listen ADDR` exposes (/healthz, /readyz,
# /metrics), so the image needs no in-container curl — Kubernetes httpGet
# probes hit them directly. See docs/operator/running-as-a-service.md.
# Digest-pinned (the multi-arch index digest, not a per-platform
# manifest) so two builds of the same sluice tag can't differ because
# the mutable `:nonroot` tag moved underneath them. Dependabot's
# docker ecosystem bumps the digest when upstream publishes.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:b7bb25d9f7c31d2bdd1982feb4dafcaf137703c7075dbe2febb41c24212b946f

COPY sluice /usr/local/bin/sluice

ENTRYPOINT ["/usr/local/bin/sluice"]
