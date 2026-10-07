#!/usr/bin/env bash
# bdriver.sh PAIR [N] [REPS] [WORKLOADS] — the ADR-0191 brokerbench matrix of
# one pair: every workload × arm through bcell.sh (one captured chain per
# workload and arm, REPS replays per apply mode). ARMS (default "base head")
# names the binaries, each FB_BIN_<ARM>.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAIR=$1 N=${2:-2000} REPS=${3:-3} WORKLOADS=${4:-"A B D K"}
ARMS=${ARMS:-"base head"}
for W in $WORKLOADS; do
  for a in $ARMS; do
    bash "$HERE/bcell.sh" "$PAIR" "$W" "$a" "$N" "$REPS" 2>&1 | grep -E '^[a-z0-9]+_' || true
  done
done
