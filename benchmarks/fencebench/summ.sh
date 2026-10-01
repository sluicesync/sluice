#!/usr/bin/env bash
# summ.sh [RESULTS] — median, min and max transactions/s per
# pair_workload_latency|arm-cfg over the reps in a results file (default
# out/results.txt), with a count of cells whose verify was not OK.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
F=${1:-$HERE/out/results.txt}
awk '
{
  split($1, a, "_"); key = a[1] "_" a[2] "_" a[5] "|" a[3] "-" a[4]
  r = $0; sub(/.*txps=/, "", r); sub(/ .*/, "", r)
  if ($0 !~ /VERIFY OK/) bad[key]++
  n[key]++; vals[key, n[key]] = r + 0; keys[key] = 1
}
END {
  for (k in keys) {
    m = n[k]; for (i = 1; i <= m; i++) s[i] = vals[k, i]
    for (i = 1; i <= m; i++) for (j = i + 1; j <= m; j++) if (s[j] < s[i]) { t = s[i]; s[i] = s[j]; s[j] = t }
    med = (m % 2) ? s[(m + 1) / 2] : (s[m / 2] + s[m / 2 + 1]) / 2
    printf "%s n=%d median=%.1f min=%.1f max=%.1f verify_fail=%d\n", k, m, med, s[1], s[m], bad[k] + 0
  }
}' "$F" | sort
