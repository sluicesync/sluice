#!/usr/bin/env bash
# Bounded-retry GHCR login + pre-pull of CI's pre-baked container images.
#
# Why this exists: the self-hosted runner pool intermittently hits
# `docker login ghcr.io: context deadline exceeded` (and, less often,
# pull i/o timeouts). A single transient blip on the login or pull step
# red-fails the whole integration shard, which then costs a manual
# `gh run rerun --failed` round (~10-15 min). Retrying the login+pull a
# few times with linear backoff self-heals the blip in-line instead.
#
# This does NOT mask a real registry outage: after the retry budget is
# exhausted the script still exits non-zero, so a genuinely unreachable
# GHCR fails the shard with a clear "after N attempts" message.
#
# Usage: ci-ghcr-pull.sh <image-ref> [<image-ref> ...]
# Auth comes from the environment so the token never lands on the
# command line: GHCR_USER (e.g. github.actor) + GHCR_TOKEN (GITHUB_TOKEN).
set -euo pipefail

readonly MAX_ATTEMPTS=5

# retry <human-label> <cmd...> — runs cmd, retrying up to MAX_ATTEMPTS
# with linear backoff (10s, 20s, ...). Returns cmd's success or fails
# loudly after the budget is spent.
retry() {
	local label="$1"
	shift
	local attempt
	for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
		if "$@"; then
			return 0
		fi
		if [ "${attempt}" -eq "${MAX_ATTEMPTS}" ]; then
			echo "::error::${label} failed after ${MAX_ATTEMPTS} attempts" >&2
			return 1
		fi
		local backoff=$((attempt * 10))
		echo "::warning::${label} attempt ${attempt} failed; retrying in ${backoff}s" >&2
		sleep "${backoff}"
	done
}

ghcr_login() {
	echo "${GHCR_TOKEN}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin
}

if [ "$#" -eq 0 ]; then
	echo "::error::ci-ghcr-pull.sh: no image refs given" >&2
	exit 2
fi

retry "ghcr.io login" ghcr_login
for img in "$@"; do
	retry "docker pull ${img}" docker pull "${img}"
done
