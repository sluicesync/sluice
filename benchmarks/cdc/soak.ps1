# soak.ps1 — CDC steady-state MEMORY soak (Windows / PowerShell, host-binary RSS).
#
# Sibling of cdc-bench.sh: where that validates zero-LOSS, this measures the
# long-running MEMORY profile of `sluice sync` in CDC follow mode — does RSS
# plateau (no leak / no unbounded sawtooth) and does --max-memory (GOMEMLIMIT)
# bound it? It launches the sluice binary on the HOST (so true Windows RSS is
# read via Get-Process WorkingSet64), drives a continuous INSERT/UPDATE/DELETE
# writer in the source container, and samples RSS + Go heap/GC from sluice's own
# /metrics endpoint every IntervalSec, writing a CSV. Run it once per config and
# diff the CSVs (see cdc-soak.md for the analysis recipe + results on record).
#
# Prereqs: the bench-cdc-src/bench-cdc-dst cluster is up (bash cdc-up.sh) and a
# sluice binary is built (go build -o sluice.exe ./cmd/sluice). Host ports
# 5453 (src) / 5454 (dst) per cdc-up.sh.
#
# Usage:
#   pwsh benchmarks/cdc/soak.ps1 -Binary .\sluice.exe -Label A-unbounded -DurationSec 420
#   pwsh benchmarks/cdc/soak.ps1 -Binary .\sluice.exe -Label B-cap32 -MaxMemory 32MiB -DurationSec 420
#   # Throttled target (fill the buffer): point -DstPort at a toxiproxy bandwidth proxy.
#   pwsh benchmarks/cdc/soak.ps1 -Binary .\sluice.exe -Label C-throttled -DstPort 5455 -DurationSec 420
param(
  [Parameter(Mandatory=$true)][string]$Binary,
  [Parameter(Mandatory=$true)][string]$Label,
  [string]$MaxMemory = "",                    # e.g. "768MiB"; empty = GOMEMLIMIT off
  [long]$MaxBufferBytes = 536870912,          # 512 MiB raw buffered-value cap
  [int]$DurationSec = 420,
  [int]$IntervalSec = 5,
  [int]$SrcPort = 5453,
  [int]$DstPort = 5454,
  [string]$OutDir = "$PSScriptRoot\results",
  [string]$Docker = "C:\Program Files\Rancher Desktop\resources\resources\win32\bin\docker.exe"
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
$SB = (Resolve-Path $Binary).Path
$SRC = "postgres://postgres:bench@localhost:$SrcPort/benchdb?sslmode=disable"
$DST = "postgres://postgres:bench@localhost:$DstPort/benchdb?sslmode=disable"
$STREAM = "soak"; $METRICS = "127.0.0.1:9090"; $PPROF = "127.0.0.1:6060"
$csv = Join-Path $OutDir "soak-$Label.csv"
$log = Join-Path $OutDir "soak-$Label.sync.log"
$errlog = Join-Path $OutDir "soak-$Label.sync.err"

Write-Host "### soak '$Label'  max-memory='$MaxMemory'  buffer=$MaxBufferBytes  dst-port=$DstPort  dur=${DurationSec}s"
Get-Process ([System.IO.Path]::GetFileNameWithoutExtension($SB)) -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

# 1. Fresh target schema + drop the slot so each run cold-starts clean.
& $Docker exec bench-cdc-dst psql -U postgres -d benchdb -q -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO postgres;" | Out-Null
& $Docker exec bench-cdc-src psql -U postgres -d benchdb -q -c "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='sluice_slot' AND active_pid IS NOT NULL;" 2>$null | Out-Null
Start-Sleep -Seconds 1
& $Docker exec bench-cdc-src psql -U postgres -d benchdb -q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='sluice_slot';" 2>$null | Out-Null

# 2. Launch the host sluice binary (PassThru → PID for RSS sampling).
$a = @("sync","start","--source-driver=postgres","--source=$SRC",
  "--target-driver=postgres","--target=$DST","--stream-id=$STREAM",
  "--max-buffer-bytes=$MaxBufferBytes","--apply-batch-size=auto",
  "--metrics-listen=$METRICS","--pprof-listen=$PPROF")
if ($MaxMemory -ne "") { $a += "--max-memory=$MaxMemory" }
$proc = Start-Process -FilePath $SB -ArgumentList $a -PassThru -RedirectStandardOutput $log -RedirectStandardError $errlog -WindowStyle Hidden
Write-Host "sluice pid=$($proc.Id)"

# 3. Continuous writer in the source container (self-terminating). 12 tables,
#    seed high-water ~2,000,000.
$wd = $DurationSec + 10
$writer = @"
SEED=2000000; NT=12; DUR=$wd; start=`$(date +%s); it=0
while [ `$(( `$(date +%s) - start )) -lt `$DUR ]; do
  t=`$(( it % NT )); tbl=`$(printf 'cdc_%02d' `$t)
  lo=`$(( SEED + 1 + it*200 )); hi=`$(( lo + 199 ))
  uwlo=`$(( (it*137) % (SEED-500) + 1 )); uwhi=`$(( uwlo + 99 ))
  dwlo=`$(( (it*911) % (SEED-50) + 1 )); dwhi=`$(( dwlo + 9 ))
  psql -U postgres -d benchdb -qtA >/dev/null 2>&1 <<SQL
INSERT INTO `${tbl} (id,user_id,amount,event_type,payload,created_at,is_active,filler)
SELECT g,(g%5000000)+1,round((random()*100000)::numeric,2),
 (ARRAY['click','view','purchase','signup','logout','error','refund','search'])[1+(g%8)],
 jsonb_build_object('k',md5(g::text),'n',g%1000),
 timestamptz '2020-01-01' + ((g%126230400)||' seconds')::interval,(g%3)=0,repeat('x',80)
FROM generate_series(`${lo},`${hi}) g ON CONFLICT (id) DO NOTHING;
UPDATE `${tbl} SET amount=amount+1.00,is_active=NOT is_active WHERE id BETWEEN `${uwlo} AND `${uwhi};
DELETE FROM `${tbl} WHERE id BETWEEN `${dwlo} AND `${dwhi};
SQL
  it=`$(( it + 1 ))
done
"@
& $Docker exec -d bench-cdc-src bash -c $writer | Out-Null
Write-Host "writer launched (continuous ${wd}s)"

# 4. Sample RSS + heap/GC every IntervalSec.
"t_sec,rss_mb,heap_alloc_mb,heap_sys_mb,heap_objects,gc_total,gc_pause_s,phase" | Out-File -FilePath $csv -Encoding utf8
$t0 = Get-Date; $coldDone = $false
while (((Get-Date) - $t0).TotalSeconds -lt $DurationSec) {
  $t = [int]((Get-Date) - $t0).TotalSeconds
  $rssMb = 0
  try { $rssMb = [math]::Round((Get-Process -Id $proc.Id -ErrorAction Stop).WorkingSet64/1MB,1) }
  catch { Write-Host "process gone at t=$t"; break }
  $ha=0;$hs=0;$ho=0;$gc=0;$gp=0
  try {
    foreach ($line in (Invoke-WebRequest -Uri "http://$METRICS/metrics" -UseBasicParsing -TimeoutSec 4).Content -split "`n") {
      if ($line -match '^sluice_go_memstats_heap_alloc_bytes\s+(\d+)') { $ha=[math]::Round([double]$matches[1]/1MB,1) }
      elseif ($line -match '^sluice_go_memstats_heap_sys_bytes\s+(\d+)') { $hs=[math]::Round([double]$matches[1]/1MB,1) }
      elseif ($line -match '^sluice_go_memstats_heap_objects\s+(\d+)') { $ho=[long]$matches[1] }
      elseif ($line -match '^sluice_go_gc_completed_total\s+(\d+)') { $gc=[long]$matches[1] }
      elseif ($line -match '^sluice_go_gc_pause_seconds_total\s+([\d.eE+-]+)') { $gp=[double]$matches[1] }
    }
  } catch {}
  if (-not $coldDone -and (Test-Path $errlog) -and (Select-String -Path $errlog -Pattern "entering CDC mode" -Quiet)) { $coldDone = $true }
  $phase = if ($coldDone) { "cdc" } else { "coldstart" }
  "$t,$rssMb,$ha,$hs,$ho,$gc,$gp,$phase" | Out-File -FilePath $csv -Append -Encoding utf8
  Write-Host ("  t={0,4}s rss={1,7}MB heap_alloc={2,6}MB heap_sys={3,6}MB gc={4} {5}" -f $t,$rssMb,$ha,$hs,$gc,$phase)
  Start-Sleep -Seconds $IntervalSec
}

# 5. Heap pprof artifact, then teardown.
try { Invoke-WebRequest -Uri "http://$PPROF/debug/pprof/heap" -UseBasicParsing -OutFile (Join-Path $OutDir "soak-$Label.heap.pprof") -TimeoutSec 10; Write-Host "heap pprof saved" } catch { Write-Host "pprof grab failed: $_" }
try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}
& $Docker exec bench-cdc-src bash -c "pkill -f generate_series 2>/dev/null; pkill psql 2>/dev/null" 2>$null | Out-Null
Write-Host "### '$Label' done -> $csv"
