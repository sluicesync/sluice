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
FROM gcr.io/distroless/static-debian12:nonroot

COPY sluice /usr/local/bin/sluice

ENTRYPOINT ["/usr/local/bin/sluice"]
