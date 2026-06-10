#!/usr/bin/env bash
# ci-mirror-pull.sh — pull a stock docker.io image via its GHCR mirror,
# then retag it locally under the stock name (task #36).
#
# Why this exists: three docker.io incidents in 24h (a vttestserver pull
# flake, the postgres:17 cold pull flaking three tag runs, and a full
# registry outage that failed a PR shard on postgres:16 through the whole
# 5-attempt retry budget) showed retry-with-backoff alone cannot remove
# docker.io from CI's critical path. build-prebaked-images.sh's `mirrors`
# engine publishes PURE RETAGS (byte-identical, no content changes) of the
# stock images CI depends on to GHCR; this script is the consumer side.
#
# For each stock ref this script:
#   1. derives the mirror ref via the NAMING RULE below,
#   2. pulls the mirror with bounded retry (ci-ghcr-pull.sh),
#   3. `docker tag`s it back to the stock name — tests keep booting the
#      stock ref (postgres:16 etc.) and testcontainers' PullIfNotPresent
#      hits the local cache instead of docker.io,
#   4. FALLBACK: if the mirror pull fails (GHCR outage, or the mirror has
#      not been published yet — the first-run bootstrap case), it emits a
#      ::warning and pulls the stock ref straight from docker.io, i.e.
#      degrades to the pre-mirror behavior. The mirror must never make
#      availability WORSE than docker.io alone.
#
# NAMING RULE (the single source of truth — build-prebaked-images.sh
# reads it via `--print-ref` so publisher and consumers cannot drift):
#   docker.io/<repo>:<tag>  ->  ${GHCR_NAMESPACE}/sluice-mirror-<basename(repo)>:<tag>
#   e.g. postgres:16                  -> ghcr.io/sluicesync/sluice-mirror-postgres:16
#        mysql:8.0                    -> ghcr.io/sluicesync/sluice-mirror-mysql:8.0
#        vitess/vttestserver:mysql80  -> ghcr.io/sluicesync/sluice-mirror-vttestserver:mysql80
# (The pre-existing vitess/lite mirror predates this scheme and lives at
# ghcr.io/sluicesync/sluice-vitess:<tag>; use --mirror to consume it.)
#
# Usage:
#   ci-mirror-pull.sh <stock-ref> [<stock-ref> ...]
#   ci-mirror-pull.sh --mirror <mirror-ref> <stock-ref>
#       Pull+retag+fallback with an EXPLICIT mirror ref (for mirrors that
#       predate the naming rule, e.g. sluice-vitess for vitess/lite).
#   ci-mirror-pull.sh --print-ref <stock-ref>
#       Print the derived mirror ref and exit (used by the publisher).
#
# Auth: GHCR_USER + GHCR_TOKEN, passed through to ci-ghcr-pull.sh for the
# mirror pull (empty token = anonymous, the fork-PR case). The docker.io
# fallback always pulls anonymously.
set -euo pipefail

GHCR_NAMESPACE="${GHCR_NAMESPACE:-ghcr.io/sluicesync}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# mirror_ref derives the GHCR mirror ref for a stock docker.io ref per
# the naming rule above. The stock ref must carry an explicit tag — a
# bare repo would silently mirror :latest, which is exactly the kind of
# implicit drift this script exists to prevent.
mirror_ref() {
	local stock="$1"
	if [[ "$stock" != *:* ]]; then
		echo "::error::ci-mirror-pull.sh: stock ref '${stock}' has no explicit tag" >&2
		return 2
	fi
	local repo="${stock%:*}"
	local tag="${stock##*:}"
	printf '%s/sluice-mirror-%s:%s\n' "$GHCR_NAMESPACE" "${repo##*/}" "$tag"
}

# pull_via_mirror pulls the mirror (bounded retry, smaller budget than a
# direct pull since a healthy GHCR answers fast and a failure has a
# fallback), retags to the stock name on success, and falls back to a
# direct docker.io pull (full retry budget) on failure.
pull_via_mirror() {
	local mirror="$1"
	local stock="$2"
	if MAX_ATTEMPTS="${MIRROR_MAX_ATTEMPTS:-3}" FAIL_LEVEL=warning bash "$SCRIPT_DIR/ci-ghcr-pull.sh" "$mirror"; then
		docker tag "$mirror" "$stock"
	else
		echo "::warning::GHCR mirror ${mirror} unavailable (outage or not yet published); falling back to docker.io for ${stock}" >&2
		# Clear GHCR_TOKEN so the fallback skips the ghcr.io login — a
		# GHCR outage that broke the mirror pull must not also break
		# the docker.io fallback at the login step. docker.io pulls
		# resolve anonymously either way.
		GHCR_TOKEN='' bash "$SCRIPT_DIR/ci-ghcr-pull.sh" "$stock"
	fi
}

case "${1:-}" in
	--print-ref)
		if [ "$#" -ne 2 ]; then
			echo "::error::usage: ci-mirror-pull.sh --print-ref <stock-ref>" >&2
			exit 2
		fi
		mirror_ref "$2"
		exit 0
		;;
	--mirror)
		if [ "$#" -ne 3 ]; then
			echo "::error::usage: ci-mirror-pull.sh --mirror <mirror-ref> <stock-ref>" >&2
			exit 2
		fi
		pull_via_mirror "$2" "$3"
		exit 0
		;;
	"")
		echo "::error::ci-mirror-pull.sh: no stock image refs given" >&2
		exit 2
		;;
esac

for stock in "$@"; do
	mirror="$(mirror_ref "$stock")"
	pull_via_mirror "$mirror" "$stock"
done
