#!/usr/bin/env bash
# Run the gated Vitess-cluster integration suite across several Vitess
# server versions (roadmap item 1(e): the multi-version matrix).
#
# The vendored client stays vitess.io/vitess v0.24.1. Older servers are
# exercised via newer-client -> older-server skew, the direction Vitess
# supports for rolling upgrades (and the one PlanetScale itself runs).
#
# For each image: docker-pull it (missing tag -> SKIP, not FAIL), set
# VITESS_LITE_IMAGE so the compose file + harness boot on it, run the
# 'integration vitesscluster' suite, and record PASS / FAIL / SKIP. The
# 'latest' canary is allowed to fail; a FAIL on any pinned version fails
# the run.
#
# Manual / rig-driven (heavy: each version boots a real 5-container
# cluster). Intentionally NOT in per-PR CI.
#
# Usage:
#   scripts/vitess-version-matrix.sh
#   RUN=TestVitessCluster_Bug27 scripts/vitess-version-matrix.sh
#   VERSIONS="vitess/lite:v23.0.3 vitess/lite:v24.0.1" scripts/vitess-version-matrix.sh
#
# KEEP THE PINNED MAJORS IN [v21..v24] and BUMP THE MINORS as upstream
# releases (verify tags at https://hub.docker.com/r/vitess/lite/tags).
# v24 must stay in lockstep with the vendored client major.

set -uo pipefail

VERSIONS="${VERSIONS:-vitess/lite:v21.0.6 vitess/lite:v22.0.5 vitess/lite:v23.0.3 vitess/lite:v24.0.1 vitess/lite:latest}"
RUN="${RUN:-TestVitessCluster}"
TIMEOUT="${TIMEOUT:-25m}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "Vitess version matrix - client = vitess.io/vitess v0.24.1, run filter = '$RUN'"
echo

declare -a summary=()
hard_failures=0

for image in $VERSIONS; do
	canary=0
	case "$image" in *:latest) canary=1 ;; esac
	echo "=== $image ==="

	if ! docker pull "$image"; then
		echo "  image not pullable -> SKIP"
		summary+=("$image|SKIP|pull failed (tag missing?)")
		continue
	fi

	start=$(date +%s)
	VITESS_LITE_IMAGE="$image" go test -tags 'integration vitesscluster' \
		-count=1 -timeout "$TIMEOUT" -run "$RUN" ./internal/engines/mysql/...
	code=$?
	elapsed=$(( $(date +%s) - start ))

	if [ "$code" -eq 0 ]; then
		echo "  PASS (${elapsed}s)"
		summary+=("$image|PASS|${elapsed}s")
	elif [ "$canary" -eq 1 ]; then
		echo "  FAIL (canary - non-fatal) (${elapsed}s, exit $code)"
		summary+=("$image|FAIL(canary)|exit $code, ${elapsed}s")
	else
		echo "  FAIL (${elapsed}s, exit $code)"
		summary+=("$image|FAIL|exit $code, ${elapsed}s")
		hard_failures=$((hard_failures + 1))
	fi
done

echo
echo "===== Vitess version matrix summary ====="
printf '%-26s %-14s %s\n' "IMAGE" "RESULT" "NOTE"
for row in "${summary[@]}"; do
	IFS='|' read -r img res note <<<"$row"
	printf '%-26s %-14s %s\n' "$img" "$res" "$note"
done

if [ "$hard_failures" -gt 0 ]; then
	echo "${hard_failures} pinned version(s) FAILED."
	exit 1
fi
echo "All pinned versions passed (skips/canary tolerated)."
exit 0
