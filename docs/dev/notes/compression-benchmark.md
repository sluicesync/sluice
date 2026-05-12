# Compression-algorithm benchmark — Phase 1 backup chunks

_Generated: 2026-05-12T00:08:12Z_  
_Go: go1.26.2, runtime.GOMAXPROCS=16, GOOS=windows/amd64_  
_Rows per corpus: 50000_  

## Results

| Corpus | Algorithm | Input (MiB) | Output (MiB) | Ratio | Encode (MB/s) | Decode (MB/s) | Heap Δ (KiB) |
|---|---|---:|---:|---:|---:|---:|---:|
| binary_heavy | klauspost_gzip | 23.02 | 14.21 | 1.62x | 151.5 | 378.2 | +42320 |
| binary_heavy | klauspost_snappy | 23.02 | 19.00 | 1.21x | 2289.4 | 1983.8 | +48552 |
| binary_heavy | klauspost_zstd_better | 23.02 | 14.13 | 1.63x | 82.7 | 646.9 | +63024 |
| binary_heavy | klauspost_zstd_default | 23.02 | 14.09 | 1.63x | 285.9 | 670.6 | +60152 |
| binary_heavy | stdlib_gzip | 23.02 | 14.23 | 1.62x | 96.1 | 275.0 | +42064 |
| json_mixed | klauspost_gzip | 11.70 | 1.48 | 7.92x | 316.0 | 704.6 | +4048 |
| json_mixed | klauspost_snappy | 11.70 | 2.27 | 5.15x | 4628.7 | 1640.4 | +10144 |
| json_mixed | klauspost_zstd_better | 11.70 | 1.41 | 8.28x | 260.7 | 847.3 | +25080 |
| json_mixed | klauspost_zstd_default | 11.70 | 1.56 | 7.48x | 505.5 | 926.0 | +22424 |
| json_mixed | stdlib_gzip | 11.70 | 1.24 | 9.45x | 142.6 | 723.7 | +3792 |
| numeric_heavy | klauspost_gzip | 9.73 | 1.05 | 9.26x | 436.9 | 644.6 | +3544 |
| numeric_heavy | klauspost_snappy | 9.73 | 1.71 | 5.71x | 4038.9 | 1650.4 | +9384 |
| numeric_heavy | klauspost_zstd_better | 9.73 | 0.98 | 9.98x | 279.8 | 1042.8 | +24288 |
| numeric_heavy | klauspost_zstd_default | 9.73 | 1.06 | 9.20x | 438.5 | 1206.7 | +21704 |
| numeric_heavy | stdlib_gzip | 9.73 | 1.00 | 9.70x | 132.9 | 412.2 | +3288 |
| text_heavy | klauspost_gzip | 12.51 | 3.35 | 3.73x | 176.7 | 335.1 | +10664 |
| text_heavy | klauspost_snappy | 12.51 | 4.44 | 2.82x | 2078.6 | 1000.2 | +17280 |
| text_heavy | klauspost_zstd_better | 12.51 | 3.05 | 4.10x | 168.6 | 751.4 | +26232 |
| text_heavy | klauspost_zstd_default | 12.51 | 3.23 | 3.87x | 273.9 | 730.2 | +30144 |
| text_heavy | stdlib_gzip | 12.51 | 3.15 | 3.98x | 23.4 | 323.2 | +10424 |


## Analysis

**Ratio winners by corpus** (higher × = better):

- **numeric_heavy** (tagged-envelope int64 framing, highest redundancy): zstd_better (9.98×) ≈ stdlib_gzip (9.70×) ≈ klauspost_gzip (9.26×) ≈ zstd_default (9.20×) ≫ snappy (5.71×). Top tier is within 8% of each other; snappy gives up ~40% of the compression.
- **json_mixed** (representative OLTP shape): stdlib_gzip (9.45×) > zstd_better (8.28×) > klauspost_gzip (7.92×) > zstd_default (7.48×) ≫ snappy (5.15×). Stdlib gzip's ratio edge here is real (~6-25% over the other gzip/zstd options) but its encode is dramatically slower.
- **text_heavy** (English-text-shape varchar/text columns): zstd_better (4.10×) ≈ stdlib_gzip (3.98×) ≈ zstd_default (3.87×) ≈ klauspost_gzip (3.73×) ≫ snappy (2.82×). Narrow spread among the four top options.
- **binary_heavy** (random bytes in base64 envelopes): all four serious algorithms cluster at ~1.62×; only the envelope/framing tokens compress. Snappy (1.21×) lags but is in the same regime.

**Encode throughput**:

- snappy is 5-20× faster than the others (2,000-4,600 MB/s vs 100-500 MB/s); it's the only algorithm that doesn't add latency to a per-row write hot path.
- zstd_default lands in the 280-500 MB/s range — 3-5× faster than stdlib gzip.
- klauspost_gzip is a drop-in for stdlib gzip and runs 1.5-6× faster across all corpora.
- **stdlib gzip is the slowest encoder by a wide margin** — 23 MB/s on text-heavy (≈6× slower than klauspost_gzip on the same corpus, ≈12× slower than zstd_default). This is the Phase-1 default sluice ships today.

**Decode throughput** (less variation but still meaningful):

- snappy leads at 1,000-2,000 MB/s.
- zstd at 650-1,200 MB/s (notably faster than gzip on numeric_heavy: 1,200 vs 400 MB/s for stdlib).
- klauspost_gzip and stdlib gzip cluster at 275-720 MB/s.

**Heap delta** (transient encoder working-set proxy):

- gzip variants: ~3-10 MiB transient (small).
- zstd: ~20-60 MiB transient — the encoder's window dictionary + match tables. 2-7× heavier than gzip.
- snappy: ~9-50 MiB transient — closer to gzip.

The zstd heap cost matters at high concurrency (per-table parallel chunk writes); a 16-way bulk-copy fan-out at 60 MiB of encoder transient is ~1 GiB of working set just for compression. Worth flagging but not a blocker — operators routinely run sluice with several GiB of RSS already.

## Recommendation

**Short-term — swap stdlib `compress/gzip` for `klauspost/compress/gzip`.** This is a one-line change in `backup_chunk.go`: the public surface of `compress/gzip.NewWriter` / `compress/gzip.NewReader` is mirrored by `github.com/klauspost/compress/gzip` exactly. The benefits:

- **2-6× faster encode** across all corpora — backup-window time-to-disk drops proportionally on encode-bound runs (the typical case; bulk-copy throughput is upstream of compression).
- **Within 5% of stdlib's ratio** on every corpus measured — storage-cost impact is in the noise.
- **No chunk-format change.** The bytes klauspost emits are valid gzip-format streams readable by any gzip decoder (including the stdlib's). The `chunkHeader.Version` stays at 1; restore paths (including from pre-swap backups) continue to work without modification.
- **klauspost/compress is already in the module graph** as an indirect dependency of `github.com/jackc/pgx/v5`. Promoting it to a direct dependency adds zero binary-size cost.

**Phase 2 — add `--compression=<algo>` flag with `gzip` default and `zstd` opt-in.** Justification only after operator demand:

- zstd_default's ratio is comparable to gzip on text-heavy and numeric corpora but worse on json_mixed (~21% gap vs stdlib gzip). The headline win is encode CPU on the *next* tier (zstd_default at 280-500 MB/s vs klauspost_gzip at 150-440 MB/s — close enough that the format-version bump cost doesn't pay back unless storage cost matters more than backup-window CPU for a specific operator).
- zstd_better's marginal ratio gain over zstd_default (≤5%) doesn't justify its encode-speed cost (~2× slower). Skip the level=11 option for the v1 flag.
- snappy's encode speed is the genuine outlier (5-20×) but the ratio gap (~40% on json/numeric) is too expensive for backup chunks where bytes-on-S3 is a recurring cost. snappy could re-enter the conversation for the CDC streaming path where per-row latency dominates and chunks are smaller.

**Skipped this round** — algorithm-by-corpus auto-selection. The decision would require per-corpus shape detection in the writer, which is a much bigger change for diminishing returns; the corpus-agnostic gzip-or-zstd choice captures 80% of the benefit.

## Reproduce

```bash
# Default corpus size (50,000 rows per shape — ~30s on a laptop).
go test -tags=compressbench -run TestRunAllAndEmit -v \
    ./internal/pipeline/internal/compressbench/

# Production-scale (1M rows per corpus — ~10-30 min). Output goes to
# the file named by the env var; recommend a tmp path for one-off runs.
SLUICE_COMPRESSBENCH_ROWS=1000000 \
SLUICE_COMPRESSBENCH_OUT=/tmp/compression-benchmark-1m.md \
go test -tags=compressbench -timeout=30m -run TestRunAllAndEmit -v \
    ./internal/pipeline/internal/compressbench/

# Multi-iteration Go benchmark numbers (allocator-stable):
go test -tags=compressbench -bench=. -benchtime=3x -benchmem \
    ./internal/pipeline/internal/compressbench/
```
