#!/usr/bin/env bash
# driver.sh PAIR LAT [REPS] [WORKLOADS] — every arm × workload × rep of one
# pair at one latency, through cell.sh.
#
# ARMS (default: the ADR-0190 amendment D acceptance arms) is a space-
# separated list of "ARM CFG" pairs; each ARM needs FB_BIN_<ARM> set (see
# cell.sh). N, the backlog size, comes from nfor: sized so each cell runs
# roughly 15-30 s at the arm's expected rate.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAIR=$1 LAT=$2 REPS=${3:-3} WORKLOADS=${4:-"A B C D"}
ARMS=${ARMS:-"base:serial base:eol base:lanes fold:eol fold:lanes"}
nfor() { # WORKLOAD CFG
  local k="$1/$2"
  if [ "$LAT" = 0 ]; then
    case $k in
      A/eol | D/eol | D/lanes | */serial) echo 4000 ;;
      C/eol) echo 16000 ;;
      *) echo 30000 ;;
    esac
  else
    case $k in
      A/eol | D/eol | D/lanes | */serial) echo 800 ;;
      C/eol) echo 3000 ;;
      *) echo 20000 ;;
    esac
  fi
}
for rep in $(seq 1 "$REPS"); do
  for W in $WORKLOADS; do
    for arm in $ARMS; do
      a=${arm%%:*} cfg=${arm##*:}
      N=$(nfor "$W" "$cfg")
      TIMEOUT=${TIMEOUT:-600s} bash "$HERE/cell.sh" "$PAIR" "$W" "$a" "$cfg" "$LAT" "$N" "$rep" 2>&1 | tail -1
    done
  done
done
