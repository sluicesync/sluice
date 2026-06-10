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
# When GHCR_TOKEN is empty — fork PRs run with a read-only GITHUB_TOKEN and
# no access to repo secrets — the login is skipped and the (public) pre-baked
# images are pulled anonymously, so external-contributor PRs pass CI too.
set -euo pipefail

# Overridable so callers with their own fallback (ci-mirror-pull.sh) can
# spend a smaller budget on the first-choice registry before falling back.
readonly MAX_ATTEMPTS="${MAX_ATTEMPTS:-5}"

# Annotation level for the exhausted-retry message. Callers with their
# own fallback (ci-mirror-pull.sh) downgrade this to "warning" so a
# mirror miss that the fallback recovers doesn't leave a spurious
# ::error annotation on a green run. Direct callers stay loud.
readonly FAIL_LEVEL="${FAIL_LEVEL:-error}"

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
			echo "::${FAIL_LEVEL}::${label} failed after ${MAX_ATTEMPTS} attempts" >&2
			return 1
		fi
		local backoff=$((attempt * 10))
		echo "::warning::${label} attempt ${attempt} failed; retrying in ${backoff}s" >&2
		sleep "${backoff}"
	done
}

ghcr_login() {
	echo "${GHCR_TOKEN:-}" | docker login ghcr.io -u "${GHCR_USER:-}" --password-stdin
}

if [ "$#" -eq 0 ]; then
	echo "::error::ci-ghcr-pull.sh: no image refs given" >&2
	exit 2
fi

# Fork PRs run with an empty GITHUB_TOKEN and no access to repo secrets, so
# GHCR_TOKEN is empty there. The pre-baked images are public, so an anonymous
# pull works — skip the login (an empty-password `docker login` would only
# fail) and pull straight through. Same-repo pushes/PRs still authenticate
# (private-image-safe, and authed pulls dodge GHCR's stricter anon rate limit).
if [ -n "${GHCR_TOKEN:-}" ]; then
	retry "ghcr.io login" ghcr_login
else
	echo "::notice::no GHCR_TOKEN (fork PR / anonymous run) — skipping login, pulling public images anonymously" >&2
fi
for img in "$@"; do
	retry "docker pull ${img}" docker pull "${img}"
done
