#!/bin/sh
# check-integration-skips.sh — fail a CI integration shard when a test
# SKIPPED that was expected to RUN (audit 2026-07-23 TEST-7; the
# psverify.yml fail-on-skip belt generalized to the per-PR shards).
#
# Why: nearly every integration boot helper starts with
# testcontainers.SkipIfProviderIsNotHealthy(t), so a docker daemon that
# dies MID-RUN converts the rest of the shard into silent skips and the
# job stays GREEN — vacuous coverage with no signal (the shards ran
# without -v, so the skips were invisible even in the log). The shard
# now runs with -v teeing to a file, and this guard fails on any
# `--- SKIP` whose test name is not on the explicit allowlist below.
#
# ALLOWLIST — anchored extended regexes over the skip's test name
# (subtest path included), one per KNOWN-legitimate skip, each with the
# reason it may skip even on a healthy CI runner. Keep it tight: an
# entry that stops matching any real skip costs nothing (skips are
# exceptional), but every entry is a hole in the vacuous-green belt, so
# new entries need the same scrutiny as a new DEFENSIVE_PACKAGES entry
# in check-shard-coverage.sh.
#
#   - TestMigrate_Corpus_*: the real-world corpus dumps are fetch.sh
#     downloads (not tracked); CI runs without them by design.
#   - TestLoadColumnTypes_Bug97VerbatimEligibleTypes: gated on the
#     PG_PROBE_DSN env var (a live-probe characterization test), never
#     set in shard CI.
#   - TestChainRestore_CrossEnginePostGISNowSupported AND
#     TestChainRestore_CrossEngine_PostGISNowSupported: TWO distinct
#     retired v0.28.0 tombstones with near-identical names — the first
#     is the untagged unit placeholder (chain_restore_test.go), the
#     second the integration-tagged one (chain_restore_cross_
#     integration_test.go). A -tags=integration shard runs BOTH, so
#     both documented unconditional skips appear in shard output
#     (each points at the postgis-tagged suite for the live coverage).
#   - TestStreamer_SchemaForward_DropNotNull_PG: documented PG
#     limitation (pgoutput omits the nullability flag), unconditional.
#   - TestBackup_SignedManifest_DR_RoundTripAndTamper: two data-dependent
#     subtests skip when the incremental produced no change chunks.
#   - TestConnectionSlotClassifier_RealPG53300: skips when the held
#     session drops before the classifier probe (race-dependent, the raw
#     ping assertion has already run by then).
#   - TestStreamer_PG_StreamIDCollisionRefused /
#     TestStreamer_MultiSchema_SlotLossRefusesLoudly: conditional on
#     runtime state that usually holds; skip = precondition unprovable,
#     documented in the tests.
#   - TestLoadData* / TestWriteBatched*: sql_mode/local_infile probe
#     tests that skip when the container refuses the GLOBAL grant or the
#     server doesn't exhibit the seeded clamp (data-dependent).
#   - TestCDCReader_TimestampNonUTCHost: needs host tzdata for
#     America/Los_Angeles.
ALLOWED_SKIPS='
^TestMigrate_Corpus_
^TestLoadColumnTypes_Bug97VerbatimEligibleTypes$
^TestChainRestore_CrossEnginePostGISNowSupported$
^TestChainRestore_CrossEngine_PostGISNowSupported$
^TestStreamer_SchemaForward_DropNotNull_PG$
^TestBackup_SignedManifest_DR_RoundTripAndTamper(/|$)
^TestConnectionSlotClassifier_RealPG53300$
^TestStreamer_PG_StreamIDCollisionRefused$
^TestStreamer_MultiSchema_SlotLossRefusesLoudly$
^TestLoadData
^TestWriteBatched
^TestCDCReader_TimestampNonUTCHost$
'

usage() {
	echo "usage: $0 <go-test-verbose-output-file> [shard-name]" >&2
	exit 2
}

out=${1:-}
shard=${2:-integration}
[ -n "$out" ] || usage
if [ ! -f "$out" ]; then
	echo "::error::check-integration-skips: output file $out does not exist — the test step must tee its -v output there before this guard runs."
	exit 1
fi

# Vacuous-input guard: a go-test -v log always carries PASS/ok markers;
# a file without any means the tee wiring broke (or the run produced
# nothing), and passing on it would be exactly the silent-green this
# guard exists to kill.
if ! grep -qE '^(--- PASS: |ok[[:space:]])' "$out"; then
	echo "::error::check-integration-skips: $out contains no '--- PASS:'/'ok' markers — not a go test -v log; fix the tee wiring in ci.yml before trusting this guard."
	exit 1
fi

skips=$(grep -E '^[[:space:]]*--- SKIP: ' "$out" | awk '{print $3}' | sort -u)
if [ -z "$skips" ]; then
	echo "check-integration-skips ($shard): no skipped tests."
	exit 0
fi

status=0
count=0
for name in $skips; do
	count=$((count + 1))
	allowed=0
	for re in $ALLOWED_SKIPS; do
		if printf '%s\n' "$name" | grep -qE "$re"; then
			allowed=1
		fi
	done
	if [ "$allowed" -eq 0 ]; then
		echo "::error::shard $shard: test $name SKIPPED but is not on the known-skip allowlist — in CI a skip usually means the container provider died mid-run and the remaining coverage silently vanished (audit 2026-07-23 TEST-7). If this skip is genuinely legitimate on a healthy runner, add it to ALLOWED_SKIPS in scripts/check-integration-skips.sh with a reason."
		status=1
	fi
done

if [ "$status" -eq 0 ]; then
	echo "check-integration-skips ($shard): $count skipped test(s), all on the known-skip allowlist."
fi
exit $status
