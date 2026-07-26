# TDX-Hydro compile scale rehearsal

The compile rehearsal generates 350,000 basin rows and 350,001 streamnet
rows in bounded 4,096-row source batches. Each basin has five coordinates and
each reach has two. Source order is descending native ID while the compiled
equal-Hilbert order is ascending global ID.

Run it after building the validator:

```bash
HFX_BINARY="$(cd ../.. && pwd)/target/release/hfx" \
  uv run python rehearse_compile_scale.py
```

Success writes one sorted JSON object to stdout and nothing else. Failure writes
one `compile rehearsal failed: ...` diagnostic to stderr, writes no stdout or
traceback, terminates the monitored child tree, and exits 1. The child has a
180-second timeout, a 1,073,741,824-byte process-tree RSS ceiling, and a fixed
2,000,000,000-byte scratch ceiling.

The committed workload was selected after measuring the many-row,
few-coordinate shape. Commit `a902eec` crossed 1 GiB at 1,074,249,728 bytes.
An intermediate unbounded path completed with a 1,032,421,376-byte adapter
high-water mark and a 1,022,558,208-byte sampled peak in 38.10 seconds. The
disk-backed implementation completed the same fixed rehearsal with a
539,262,976-byte adapter high-water and sampled peak, a 573,865,984-byte
parent-observed process-tree peak, a 6,592,282-byte scratch high-water mark,
and a 56.01-second wall time. `dscontarea_infer` was the largest allocating
phase at a 91,963,392-byte allocation delta. This is a CI
regression result, not certification of the production target.

The fixed rehearsal processed 2,450,002 authored coordinates in 56.01 seconds,
an end-to-end lower bound of about 43,740 coordinates/second including source
generation and validation. Conservatively applying that rate to 500 million
coordinates projects about 3.18 hours. The next campaign must record the real
clamp-only throughput because GeoPackage and geometry complexity differ.

The 8,193-basin golden ledger defines one normalized spool generation as the
closed basin and stream normalized spools together. It measured 954,084 spool
bytes for 3,334,144 source bytes, a 0.286156 ratio. The fan-in-2 multi-pass run
measured a 1,392,574-byte private-scratch high-water mark and a
1,504,176-byte transient working high-water mark. Scaling the measured
scratch/source ratio to the 8,859,344,896-byte observed input gives about
3.70 GB, below the 30,000,000,000-byte reserve. This small synthetic ratio is
a lifecycle proof, not a production compression forecast.

## Before/after phase evidence

The phase comparison used 350,000 basins, 350,001 stream rows, 1,750,000
basin coordinates, 700,002 stream coordinates, and 127,815,680 source bytes.
The selected topology dtypes were int64 native/downstream/global IDs, float64
DSContArea, uint32 Hilbert keys, and uint8 flags. The pre-streaming high-water
RSS was 1,074,151,424 bytes; the disk-backed run was 541,966,336 bytes.
Columns below are start RSS, end RSS, absolute phase peak, allocation delta,
and maximum intra-phase increase, all in bytes.

| Pre-streaming phase | Start | End | Peak | Delta | Increase |
|---|---:|---:|---:|---:|---:|
| `basins_load` | 152,092,672 | 319,324,160 | 319,324,160 | 167,231,488 | 167,231,488 |
| `streamnet_load` | 319,324,160 | 423,804,928 | 423,804,928 | 104,480,768 | 104,480,768 |
| `source_validate` | 423,804,928 | 605,995,008 | 605,995,008 | 182,190,080 | 182,190,080 |
| `basins_clamp` | 605,995,008 | 642,646,016 | 642,646,016 | 36,651,008 | 36,651,008 |
| `streamnet_clamp` | 642,646,016 | 668,909,568 | 668,909,568 | 26,263,552 | 26,263,552 |
| `source_post_clamp_validate` | 668,909,568 | 668,942,336 | 668,942,336 | 32,768 | 32,768 |
| `dscontarea_infer` | 668,942,336 | 682,360,832 | 686,653,440 | 13,418,496 | 17,711,104 |
| `topology` | 665,321,472 | 851,230,720 | 893,173,760 | 185,909,248 | 227,852,288 |
| `catchment_prepare` | 851,230,720 | 968,998,912 | 984,399,872 | 117,768,192 | 133,169,152 |
| `catchment_write` | 968,998,912 | 1,008,943,104 | 1,008,943,104 | 39,944,192 | 39,944,192 |
| `graph_write` | 1,008,943,104 | 1,009,696,768 | 1,009,696,768 | 753,664 | 753,664 |
| `snap_prepare` | 1,009,696,768 | 1,039,220,736 | 1,055,834,112 | 29,523,968 | 46,137,344 |
| `snap_write` | 1,039,220,736 | 1,044,561,920 | 1,044,561,920 | 5,341,184 | 5,341,184 |

| Disk-backed phase | Start | End | Peak | Delta | Increase |
|---|---:|---:|---:|---:|---:|
| `basins_load` | 159,809,536 | 186,695,680 | 186,695,680 | 26,886,144 | 26,886,144 |
| `streamnet_load` | 186,695,680 | 201,359,360 | 201,359,360 | 14,663,680 | 14,663,680 |
| `source_validate` | 201,359,360 | 268,795,904 | 268,795,904 | 67,436,544 | 67,436,544 |
| `basins_clamp` | 268,795,904 | 297,713,664 | 297,713,664 | 28,917,760 | 28,917,760 |
| `streamnet_clamp` | 297,713,664 | 301,940,736 | 301,940,736 | 4,227,072 | 4,227,072 |
| `source_post_clamp_validate` | 301,940,736 | 302,907,392 | 302,907,392 | 966,656 | 966,656 |
| `dscontarea_infer` | 302,907,392 | 404,865,024 | 404,865,024 | 101,957,632 | 101,957,632 |
| `topology` | 404,865,024 | 491,913,216 | 491,913,216 | 87,048,192 | 87,048,192 |
| `catchment_run_creation` | 491,913,216 | 493,649,920 | 493,649,920 | 1,736,704 | 1,736,704 |
| `catchment_graph_merge_write` | 493,649,920 | 538,460,160 | 538,460,160 | 44,810,240 | 44,810,240 |
| `snap_run_creation` | 538,460,160 | 538,558,464 | 538,558,464 | 98,304 | 98,304 |
| `snap_merge_write` | 538,558,464 | 540,639,232 | 540,835,840 | 2,080,768 | 2,277,376 |

The old synthetic allocating intervals were topology, source validation, and
catchment preparation by maximum intra-phase increase. The new largest
interval was DSContArea inference because it includes loading the fixed-width
compact accumulators from the normalized spools; no geometry-heavy phase
retains all dataset coordinates. The persistent 350,000-unit compact topology
index measured 25,200,008 NumPy payload bytes; it consists only of fixed-width
native/global/downstream IDs, contraction counts, outlets, upstream areas, and
CSR-style upstream offsets/IDs.

The production target is at most 25,769,803,776 bytes of process peak RSS for
an NGA input pair of at least 8,859,344,896 bytes. It leaves 8 GiB of a 32 GiB
ccx33 for the kernel, page cache, native allocations, validation, and sampling
error. The next paid campaign run must provide the real certification.

## Capacity disclosure

- 62-basin retained-download term: 549,279,383,552 bytes
- 62-basin retained-output term: 206,220,202,290 bytes
- one active compile-scratch reserve: 30,000,000,000 bytes
- 62-basin retained-plus-active projection: 785,499,585,842 bytes
- runbook volume: 150 GB decimal
- runbook usable check: 140,000,000,000 bytes

A 62-basin campaign needs a separately authorized retention policy or larger
volume before launch. The runbook also freezes `ccx43`, describes 16 vCPU /
64 GB, passes `--available-memory-bytes 60000000000`, requires
60,000,000,000 available bytes, and lists a superseded 10,000,000,000-byte
compile-scratch reserve. Those values are incompatible with the 8-vCPU /
32-GB ccx33 and this step's 30,000,000,000-byte reserve. Correcting them belongs
to a separate `scripts/hetzner/` step.
