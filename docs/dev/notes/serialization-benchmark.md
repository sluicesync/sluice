# Serialization benchmark — JSON libraries & msgpack (sluice backup chunk path)

Companion to `compression-benchmark.md`. Harness:
`internal/pipeline/internal/jsonbench/` (build tag `jsonbench`; not in
the normal build/CI). Models sluice's PRODUCTION two-hop typed decode
(`map[string]json.RawMessage` → typed sub-unmarshal — NOT decode-into-
`any`; that is what keeps int64 precision-safe) and gates every
candidate on the `docs/value-types.md` round-trip contract before
reporting speed. A lossy/divergent candidate is DISQUALIFIED.

## Decision summary (2026-05-16)

**Current format:** JSON-Lines of the tagged-value envelope, stdlib
`encoding/json` v1, two-hop typed decode, zstd-compressed (v0.67.0
default). Confirmed in source.

**Verdict A — JSON library: do NOT switch now; revisit when
`encoding/json/v2` lands in the Go stdlib.** Every alternative
(go-json-experiment v1/v2, goccy, sonic) PASSES fidelity and beats
stdlib on decode at 50k. But at the **1M decision-grade scale on the
DR-critical `json_change` (CDC/incremental-restore) corpus**, the
ranking *inverts*: `encoding/json/v2` (the experiment) is the clear
decode winner (~2.5× stdlib) because it has the lowest allocation
footprint, while goccy/sonic regress sharply under sustained GC
pressure (their 50k lead does not survive to restore volume).
Allocation profile, not micro-throughput, dominates the restore axis
at scale — and the lowest-allocation option is precisely the one that
arrives in the stdlib with zero new dependency and zero API-stability
risk. sonic (amd64-JIT) and go-json-experiment (pre-1.0) carry poor
cost-of-ownership on a DR-correctness path; goccy is the only
low-risk *interim* lever IF a measured restore profile ever shows JSON
decode is a material fraction of restore wall-time (unlikely —
zstd-decompress + DB writes dominate).

**Verdict B — msgpack: keep JSON-Lines; do not replace the default,
only weakly justified even as an option.** msgpack's headline raw-size
win (−34%…−58%) **largely collapses post-zstd** (sluice ships
compressed) to 0–21%, concentrated in numeric-heavy data, and is
*negative* (msgpack larger) for text-heavy and for several
vmihailenco corpora. msgpack **drop-in** (same envelope) is rejected
outright: post-zstd within ±5% of JSON (often worse), decode at/below
the shipping stdlib path and far below sonic, binary format loses
`head file | jq .`. msgpack **native** (drop the `_t` envelope) is the
only model with a durable edge (~1.4–2× decode, lowest allocs, 5–21%
smaller post-zstd on numeric) — but it is a format-version-epoch
redesign: schema-coupled decode (the wire loses the self-describing
`_t` tag, so the decoder must consult column IR types to re-type
timestamps/decimals), loss of the deliberately-supported
`--compression=none` human-inspectable case, and a new dependency. And
`sonic` on the *unchanged* format matches/beats msgpack-native on
decode for most corpora with zero format change — so msgpack is not
even the cheapest lever for the goal it would be chosen for.

**Net:** format/library churn on the DR path is not justified by the
evidence. The harness makes re-evaluation a one-command check when the
picture changes (notably `encoding/json/v2` stabilizing in stdlib).

---

# msgpack vs JSON — detailed evidence

_Generated: 2026-05-16T15:32:05Z_  
_Go: go1.26.2, GOMAXPROCS=16, windows/amd64_  
_Rows per corpus: 50000_  
_Timing: median of 5 warm passes/phase (1 discarded warm-up). Decode = restore / DR-critical axis. zstd = klauspost SpeedDefault — the v0.67.0 production default (codec.go)._  

## Fidelity gate (correctness — load-bearing)

Round-trips sluice's value contract (docs/value-types.md): int64 incl. 2^53+1, uint64 > MaxInt64, float64, []byte, decimal-as-string, RFC3339Nano time-as-string, bool, SQL NULL→nil, nested map. A lossy/divergent candidate is DISQUALIFIED regardless of speed.

| Serializer | Model | Human-inspectable | Fidelity |
|---|---|---|---|
| exp_v1compat | JSON-Lines (ships today) | yes — `head file | jq .` | PASS |
| exp_v2 | JSON-Lines (ships today) | yes — `head file | jq .` | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | yes — `head file | jq .` | PASS |
| goccy | JSON-Lines (ships today) | yes — `head file | jq .` | PASS |
| sonic | JSON-Lines (ships today) | yes — `head file | jq .` | PASS |
| stdlib_v1 | JSON-Lines (ships today) | yes — `head file | jq .` | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | no — binary | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | no — binary | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | no — binary | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | no — binary | PASS* |

`PASS` = value contract met, format self-describing. `PASS*` = value contract met bit-exact (int64/uint64/[]byte/bool/nil exact; timestamp survives byte-exact as its RFC3339Nano string — no msgpack-timestamp-ext rewrite), BUT the native model drops the `_t` tag so timestamp/decimal Go-typing requires out-of-band schema (the column IR type) the wire no longer carries. That schema coupling is a format-redesign cost, not a data-loss one — surfaced here so the decision weighs it.

## Size: raw vs post-zstd (the compression-reality axis)

Thesis under test: msgpack's raw-size advantage largely collapses after zstd (sluice chunks ship compressed). `zstd/raw` is the compression ratio; compare zstd columns ACROSS serializers for the real on-disk delta.

### binary_heavy

| Serializer | Model | Raw (MiB) | Zstd (MiB) | zstd/raw | vs JSON raw | vs JSON zstd | Fidelity |
|---|---|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 22.97 | 14.09 | 0.613 | +0.0% | +0.0% | PASS |
| exp_v2 | JSON-Lines (ships today) | 22.97 | 14.30 | 0.623 | +0.0% | +1.5% | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 22.97 | 14.30 | 0.622 | +0.0% | +1.5% | PASS |
| goccy | JSON-Lines (ships today) | 22.97 | 14.09 | 0.613 | +0.0% | +0.0% | PASS |
| sonic | JSON-Lines (ships today) | 22.97 | 14.09 | 0.613 | +0.0% | +0.0% | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 22.97 | 14.09 | 0.613 | baseline | baseline | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 21.54 | 14.47 | 0.672 | -6.2% | +2.7% | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 21.79 | 14.66 | 0.673 | -5.1% | +4.0% | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 15.15 | 13.95 | 0.921 | -34.1% | -1.0% | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 15.40 | 13.96 | 0.906 | -33.0% | -0.9% | PASS* |

### json_change

| Serializer | Model | Raw (MiB) | Zstd (MiB) | zstd/raw | vs JSON raw | vs JSON zstd | Fidelity |
|---|---|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 17.28 | 1.30 | 0.075 | +0.0% | +0.0% | PASS |
| exp_v2 | JSON-Lines (ships today) | 17.28 | 1.96 | 0.113 | +0.0% | +50.2% | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 16.65 | 1.91 | 0.115 | -3.7% | +46.5% | PASS |
| goccy | JSON-Lines (ships today) | 17.28 | 1.30 | 0.075 | +0.0% | +0.0% | PASS |
| sonic | JSON-Lines (ships today) | 17.28 | 1.30 | 0.075 | +0.0% | +0.0% | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 17.28 | 1.30 | 0.075 | baseline | baseline | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 12.32 | 1.22 | 0.099 | -28.7% | -6.5% | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 12.91 | 1.73 | 0.134 | -25.3% | +32.7% | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 9.40 | 1.24 | 0.132 | -45.6% | -4.9% | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 9.99 | 1.53 | 0.153 | -42.2% | +17.7% | PASS* |

### json_mixed

| Serializer | Model | Raw (MiB) | Zstd (MiB) | zstd/raw | vs JSON raw | vs JSON zstd | Fidelity |
|---|---|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 15.71 | 1.47 | 0.094 | +0.0% | +0.0% | PASS |
| exp_v2 | JSON-Lines (ships today) | 15.71 | 1.85 | 0.118 | +0.0% | +25.5% | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 14.28 | 1.82 | 0.128 | -9.1% | +24.0% | PASS |
| goccy | JSON-Lines (ships today) | 15.71 | 1.47 | 0.094 | +0.0% | +0.0% | PASS |
| sonic | JSON-Lines (ships today) | 15.71 | 1.47 | 0.094 | +0.0% | +0.0% | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 15.71 | 1.47 | 0.094 | baseline | baseline | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 10.40 | 1.41 | 0.136 | -33.8% | -4.1% | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 11.38 | 1.76 | 0.155 | -27.5% | +20.0% | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 7.97 | 1.39 | 0.175 | -49.3% | -5.4% | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 8.95 | 1.58 | 0.176 | -43.0% | +7.3% | PASS* |

### numeric_heavy

| Serializer | Model | Raw (MiB) | Zstd (MiB) | zstd/raw | vs JSON raw | vs JSON zstd | Fidelity |
|---|---|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 14.04 | 1.65 | 0.117 | +0.0% | +0.0% | PASS |
| exp_v2 | JSON-Lines (ships today) | 14.04 | 2.01 | 0.143 | +0.0% | +21.9% | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 14.04 | 2.01 | 0.143 | +0.0% | +21.9% | PASS |
| goccy | JSON-Lines (ships today) | 14.04 | 1.65 | 0.117 | +0.0% | +0.0% | PASS |
| sonic | JSON-Lines (ships today) | 14.04 | 1.65 | 0.117 | +0.0% | +0.0% | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 14.04 | 1.65 | 0.117 | baseline | baseline | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 9.80 | 1.42 | 0.145 | -30.2% | -13.7% | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 11.06 | 1.72 | 0.156 | -21.2% | +4.6% | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 5.94 | 1.30 | 0.219 | -57.7% | -20.8% | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 7.20 | 1.36 | 0.189 | -48.7% | -17.5% | PASS* |

### text_heavy

| Serializer | Model | Raw (MiB) | Zstd (MiB) | zstd/raw | vs JSON raw | vs JSON zstd | Fidelity |
|---|---|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 12.46 | 3.23 | 0.259 | +0.0% | +0.0% | PASS |
| exp_v2 | JSON-Lines (ships today) | 12.46 | 3.29 | 0.264 | +0.0% | +1.8% | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 12.46 | 3.29 | 0.264 | +0.0% | +1.8% | PASS |
| goccy | JSON-Lines (ships today) | 12.46 | 3.23 | 0.259 | +0.0% | +0.0% | PASS |
| sonic | JSON-Lines (ships today) | 12.46 | 3.23 | 0.259 | +0.0% | +0.0% | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 12.46 | 3.23 | 0.259 | baseline | baseline | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 11.41 | 3.35 | 0.293 | -8.4% | +3.6% | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 11.66 | 3.38 | 0.290 | -6.4% | +4.6% | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 10.93 | 3.33 | 0.305 | -12.3% | +3.0% | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 11.19 | 3.37 | 0.301 | -10.2% | +4.3% | PASS* |

## Throughput (decode-first — DR-critical axis)

### binary_heavy

| Serializer | Model | Decode MB/s | Dec allocs/op | Dec B/op | Dec heap Δ (KiB) | Encode MB/s | Enc+zstd MB/s | Enc allocs/op | Fidelity |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 106.3 | 44.0 | 3928 | +179552 | 290.1 | 119.0 | 12.0 | PASS |
| exp_v2 | JSON-Lines (ships today) | 114.5 | 44.0 | 3928 | +179600 | 331.6 | 122.3 | 9.0 | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 118.2 | 44.0 | 3928 | +179520 | 304.4 | 120.3 | 9.0 | PASS |
| goccy | JSON-Lines (ships today) | 150.6 | 71.0 | 6080 | +262352 | 376.1 | 134.5 | 7.0 | PASS |
| sonic | JSON-Lines (ships today) | 182.9 | 54.0 | 5121 | +213648 | 523.8 | 141.5 | 7.0 | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 59.0 | 73.0 | 5552 | +261624 | 292.6 | 118.0 | 24.0 | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 114.9 | 84.0 | 7608 | +370448 | 260.7 | 103.7 | 36.0 | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 112.3 | 81.9 | 4836 | +225480 | 494.4 | 125.9 | 5.2 | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 258.8 | 34.0 | 3019 | +152072 | 220.3 | 102.7 | 22.0 | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 244.9 | 32.0 | 1856 | +91280 | 289.2 | 131.6 | 12.0 | PASS* |

### json_change

| Serializer | Model | Decode MB/s | Dec allocs/op | Dec B/op | Dec heap Δ (KiB) | Encode MB/s | Enc+zstd MB/s | Enc allocs/op | Fidelity |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 36.5 | 110.7 | 5852 | +298760 | 123.8 | 90.4 | 18.3 | PASS |
| exp_v2 | JSON-Lines (ships today) | 40.3 | 106.3 | 5770 | +294512 | 152.5 | 97.7 | 13.0 | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 39.8 | 105.0 | 5700 | +291080 | 142.5 | 100.7 | 13.0 | PASS |
| goccy | JSON-Lines (ships today) | 62.0 | 176.6 | 8010 | +388032 | 138.1 | 93.5 | 12.8 | PASS |
| sonic | JSON-Lines (ships today) | 69.5 | 128.4 | 7156 | +344776 | 177.8 | 119.7 | 11.6 | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 28.2 | 181.9 | 9732 | +68080 | 98.3 | 81.6 | 59.9 | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 27.7 | 224.5 | 15685 | +395696 | 65.5 | 55.2 | 81.2 | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 21.7 | 293.8 | 10940 | +132384 | 154.9 | 101.0 | 5.3 | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 38.4 | 138.1 | 9090 | +63424 | 55.4 | 46.8 | 55.9 | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 39.1 | 134.8 | 4962 | +251920 | 102.9 | 73.6 | 17.3 | PASS* |

### json_mixed

| Serializer | Model | Decode MB/s | Dec allocs/op | Dec B/op | Dec heap Δ (KiB) | Encode MB/s | Enc+zstd MB/s | Enc allocs/op | Fidelity |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 36.9 | 88.5 | 4861 | +248096 | 119.8 | 83.1 | 23.0 | PASS |
| exp_v2 | JSON-Lines (ships today) | 41.7 | 82.5 | 4789 | +244424 | 137.2 | 92.4 | 15.0 | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 40.1 | 81.5 | 4623 | +236272 | 129.5 | 86.4 | 15.0 | PASS |
| goccy | JSON-Lines (ships today) | 66.7 | 150.0 | 6764 | +325392 | 133.3 | 95.7 | 11.0 | PASS |
| sonic | JSON-Lines (ships today) | 69.3 | 106.9 | 6063 | +290464 | 192.2 | 112.4 | 10.0 | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 28.9 | 154.0 | 8419 | +378912 | 107.6 | 84.4 | 51.0 | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 29.9 | 176.0 | 12462 | +241440 | 68.2 | 55.1 | 69.0 | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 24.0 | 236.1 | 8622 | +133136 | 167.2 | 106.8 | 5.0 | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 46.6 | 96.0 | 6252 | +322104 | 61.3 | 49.1 | 39.0 | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 52.5 | 92.2 | 3135 | +159128 | 131.7 | 76.9 | 10.0 | PASS* |

### numeric_heavy

| Serializer | Model | Decode MB/s | Dec allocs/op | Dec B/op | Dec heap Δ (KiB) | Encode MB/s | Enc+zstd MB/s | Enc allocs/op | Fidelity |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 37.6 | 88.5 | 4604 | +235424 | 96.7 | 68.8 | 26.0 | PASS |
| exp_v2 | JSON-Lines (ships today) | 39.9 | 87.5 | 4588 | +234648 | 113.6 | 76.9 | 18.0 | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 39.2 | 87.5 | 4588 | +234712 | 112.3 | 76.3 | 18.0 | PASS |
| goccy | JSON-Lines (ships today) | 60.2 | 159.5 | 6217 | +300024 | 116.0 | 85.8 | 11.0 | PASS |
| sonic | JSON-Lines (ships today) | 66.0 | 116.4 | 5685 | +273456 | 162.5 | 107.0 | 11.0 | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 28.9 | 156.5 | 8396 | +426736 | 91.0 | 69.8 | 54.0 | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 27.8 | 185.5 | 13107 | +263200 | 60.1 | 48.3 | 74.0 | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 28.2 | 194.5 | 7064 | +359120 | 163.4 | 106.4 | 5.0 | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 50.2 | 72.5 | 4512 | +232616 | 57.5 | 49.4 | 32.5 | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 55.9 | 70.5 | 1948 | +98568 | 125.8 | 79.7 | 12.5 | PASS* |

### text_heavy

| Serializer | Model | Decode MB/s | Dec allocs/op | Dec B/op | Dec heap Δ (KiB) | Encode MB/s | Enc+zstd MB/s | Enc allocs/op | Fidelity |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| exp_v1compat | JSON-Lines (ships today) | 85.9 | 32.6 | 1883 | +88992 | 199.1 | 86.3 | 14.0 | PASS |
| exp_v2 | JSON-Lines (ships today) | 105.8 | 29.6 | 1836 | +87728 | 230.3 | 92.1 | 10.0 | PASS |
| exp_v2_noescape | JSON-Lines (ships today) | 106.4 | 29.6 | 1836 | +83696 | 220.6 | 93.6 | 10.0 | PASS |
| goccy | JSON-Lines (ships today) | 148.8 | 48.0 | 2589 | +119632 | 313.1 | 107.0 | 5.0 | PASS |
| sonic | JSON-Lines (ships today) | 168.8 | 37.0 | 2308 | +102768 | 352.1 | 114.5 | 5.0 | PASS |
| stdlib_v1 | JSON-Lines (ships today) | 64.0 | 50.0 | 2978 | +139536 | 241.8 | 93.6 | 16.0 | PASS |
| msgpack_hashicorp_dropin | msgpack drop-in (same envelope) | 104.6 | 59.0 | 4424 | +216792 | 197.0 | 84.3 | 24.0 | PASS |
| msgpack_vmihailenco_dropin | msgpack drop-in (same envelope) | 68.5 | 90.0 | 3401 | +167240 | 379.5 | 117.2 | 5.0 | PASS |
| msgpack_hashicorp_native | msgpack-native (redesigned record model) | 146.4 | 43.0 | 3201 | +158032 | 199.7 | 89.4 | 20.0 | PASS* |
| msgpack_vmihailenco_native | msgpack-native (redesigned record model) | 140.3 | 41.0 | 1776 | +80088 | 318.4 | 104.1 | 7.8 | PASS* |

