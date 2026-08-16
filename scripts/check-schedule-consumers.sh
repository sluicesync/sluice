#!/usr/bin/env bash
# check-schedule-consumers.sh — the scheduled-red-consumer guard (audit H-5).
#
# TESTCI-1 established that a scheduled workflow whose red nothing consumes
# ages silently in an unread Actions tab. report-red.yml is the generalized
# consumer: a `workflow_run` job that files/refreshes/closes a standing issue
# for every scheduled correctness workflow. This guard keeps that consumer's
# workflow list honest — every workflow in .github/workflows/ that carries a
# `schedule:` trigger MUST be either:
#
#   (a) listed by its `name:` under report-red.yml's `on.workflow_run.workflows`, or
#   (b) named in the EXEMPT_WORKFLOWS block below, with a reason.
#
# A new `schedule:` trigger that is neither consumed nor exempted fails CI
# here — not weeks later when its first red goes unfiled. Mirrors the
# check-shard-coverage.sh discipline: derive the universe from the tree, hold
# a hand-maintained companion (report-red.yml's list) to it, fail on drift.
set -euo pipefail
cd "$(dirname "$0")/.."

WF_DIR=.github/workflows
CONSUMER="$WF_DIR/report-red.yml"

# Filenames deliberately NOT consumed, each with a reason. A red here is not a
# correctness regression that needs a standing triage issue.
#   build-prebaked-images.yml — its red is a transient image-build / GHCR
#   registry failure that self-heals on the next scheduled build; the CI jobs
#   that consume those images fall back to building locally, so a stale/absent
#   prebaked image degrades speed, not correctness.
EXEMPT_WORKFLOWS="build-prebaked-images.yml"

if [ ! -f "$CONSUMER" ]; then
	echo "check-schedule-consumers: consumer $CONSUMER is missing — the scheduled-red gate has no home. Failing."
	exit 1
fi

# Discover every scheduled workflow (a `schedule:` key under the `on:` block).
scheduled=""
for f in "$WF_DIR"/*.yml; do
	base=$(basename "$f")
	# The consumer itself is workflow_run-triggered, never scheduled; skip it.
	[ "$base" = "report-red.yml" ] && continue
	if grep -qE '^[[:space:]]+schedule:' "$f"; then
		scheduled="$scheduled $base"
	fi
done

# Anti-vacuity: if we discovered zero scheduled workflows the discovery broke
# (grep pattern rot, dir moved) — a green here would be meaningless.
if [ -z "${scheduled// /}" ]; then
	echo "check-schedule-consumers: discovered ZERO scheduled workflows — discovery broke or none exist. Failing."
	exit 1
fi

# The consumed set: workflow NAMES listed under report-red.yml's
# `on.workflow_run.workflows:`. Extract the block between `workflows:` and the
# next `types:`/`jobs:` key, then strip list-dashes and quotes.
consumed_names=$(awk '
	/^[[:space:]]*workflows:[[:space:]]*$/ {inblock=1; next}
	inblock && /^[[:space:]]*(types|jobs):/ {inblock=0}
	inblock && /^[[:space:]]*-[[:space:]]*/ {
		line=$0
		sub(/^[[:space:]]*-[[:space:]]*/, "", line)
		gsub(/^["'"'"']|["'"'"']$/, "", line)
		print line
	}
' "$CONSUMER")

if [ -z "$consumed_names" ]; then
	echo "check-schedule-consumers: parsed ZERO workflow names from $CONSUMER — the consumer list is empty or the parse broke. Failing."
	exit 1
fi

fail=0
for base in $scheduled; do
	# Exempt filenames pass with their reason (documented above).
	case " $EXEMPT_WORKFLOWS " in
		*" $base "*)
			echo "check-schedule-consumers: $base — EXEMPT (no standing-issue consumer by design)."
			continue
			;;
	esac

	name=$(grep -m1 '^name:' "$WF_DIR/$base" | sed 's/^name:[[:space:]]*//' | sed 's/[[:space:]]*$//')
	if [ -z "$name" ]; then
		echo "check-schedule-consumers: $base has a schedule: trigger but no top-level name: — cannot be consumed by workflow_run. Failing."
		fail=1
		continue
	fi

	if printf '%s\n' "$consumed_names" | grep -qxF "$name"; then
		echo "check-schedule-consumers: $base ('$name') — consumed by report-red.yml. OK."
	else
		echo "check-schedule-consumers: $base ('$name') has a schedule: trigger but is NOT in report-red.yml's workflows list and NOT exempt."
		echo "  Fix: add \"$name\" under report-red.yml's on.workflow_run.workflows, or add $base to EXEMPT_WORKFLOWS with a reason."
		fail=1
	fi
done

if [ "$fail" -ne 0 ]; then
	echo "check-schedule-consumers: FAILED — a scheduled workflow has no red consumer."
	exit 1
fi

echo "check-schedule-consumers: every scheduled workflow is either consumed by report-red.yml or explicitly exempt."
