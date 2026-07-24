# TDX-Hydro assembly scale rehearsal

## Scope

The full-scale synthetic rehearsal ran once on `2026-07-24` from HFX
revision `d50eb578dc8bb0fce43248c6f23cf33e208679d7`. It generated and assembled
`16000000` drainage units across `62` processing basins with `16000000`
proportional snap stems. The distribution was `seeded-skew` with seed
`20260723`.

The run was entirely local and incurred zero cloud spend. Its disposable
scratch root was `/private/tmp/hfx-tdx-hydro-m3`.

these results establish row-count-driven memory shape only; M4 remains authoritative for byte-per-unit and volume sizing, so toy geometries cannot size the campaign volume low.

## Machine and environment

| Fact | Recorded value |
|---|---|
| UTC start | `2026-07-24T13:24:07Z` |
| UTC finish | `2026-07-24T13:46:16Z` |
| Hardware model | `MacBook Pro (Mac16,5)` |
| Chip | `Apple M4 Max` |
| CPU cores | `16 (12 performance and 4 efficiency)` |
| Physical memory | `64 GB` |
| macOS | `15.7.3 (24G419)` |
| `/bin/bash` | `GNU bash, version 3.2.57(1)-release (arm64-apple-darwin24)` |
| uv | `0.9.0 (39b688653 2025-10-07)` |
| Python | `3.13.8` |
| GeoPandas | `1.1.4` |
| PyArrow | `22.0.0` |
| Shapely | `2.1.2` |
| psutil | `7.2.2` |

The adapter environment was resolved from the committed `uv.lock` with
`uv --frozen`.

## Exact invocation

The landed rehearsal command was invoked once:

```bash
uv run --frozen python rehearse_assembly_scale.py rehearse \
  --scratch-root /private/tmp/hfx-tdx-hydro-m3 \
  --basin-count 62 \
  --total-units 16000000 \
  --distribution seeded-skew \
  --seed 20260723 \
  --generator-batch-size 1024 \
  --verify-batch-size 65536 \
  --sample-interval-ms 50 \
  --rss-ceiling-bytes 32212254720 \
  --scratch-ceiling-bytes 68719476736
```

Its stdout report was captured at
`/private/tmp/hfx-tdx-hydro-m3-report.json`. The scratch tree was removed after
the evidence was retained.

## Dataset shape

| Metric | Result |
|---|---:|
| Processing basins | `62` |
| Drainage units | `16000000` |
| Snap stems | `16000000` |
| Snap stems per unit | `1.0` |
| Minimum basin units | `112123` |
| Maximum basin units | `425992` |
| Generator authored-row bound | `3072` |

The 62 per-basin unit counts were:

```text
[306951, 221791, 314333, 281624, 124836, 129226, 228277, 271548, 112123, 417725, 333662, 370542, 196735, 315392, 221815, 416248, 162881, 187606, 185309, 288125, 425025, 274635, 221424, 132379, 210787, 214212, 204593, 118908, 154513, 196320, 403486, 419959, 318079, 159757, 408616, 278310, 130211, 131098, 262452, 204841, 123576, 227416, 147357, 406484, 212864, 289665, 231262, 242915, 348718, 323831, 419013, 335089, 201457, 425992, 325664, 160504, 147364, 303911, 374703, 259778, 197995, 338088]
```

The complete snap-count vector was identical:

```text
[306951, 221791, 314333, 281624, 124836, 129226, 228277, 271548, 112123, 417725, 333662, 370542, 196735, 315392, 221815, 416248, 162881, 187606, 185309, 288125, 425025, 274635, 221424, 132379, 210787, 214212, 204593, 118908, 154513, 196320, 403486, 419959, 318079, 159757, 408616, 278310, 130211, 131098, 262452, 204841, 123576, 227416, 147357, 406484, 212864, 289665, 231262, 242915, 348718, 323831, 419013, 335089, 201457, 425992, 325664, 160504, 147364, 303911, 374703, 259778, 197995, 338088]
```

## Phase results

| Phase | Result | Wall time |
|---|---|---:|
| Generation | Passed | Not separately measured by report schema 1 |
| Referential proof | Passed | Not separately measured by report schema 1 |
| Assembly child | Exit `0` | `820.800883` s (`0.228000` h) |
| Interleaving inspection | Passed | Not separately measured by report schema 1 |
| M3-S1 verification | Passed at batch size `65536` | Not separately measured by report schema 1 |
| End to end | Passed | `22 min 9 s` |

The landed schema 1 report times only the real assembly child. The end-to-end
elapsed value is derived from the UTC wrapper timestamps and is not a sum of
separately instrumented phase measurements.

## Resource measurements

| Measurement | Value | Ceiling | Margin |
|---|---:|---:|---:|
| Process-tree peak RSS | `1018265600` bytes (`0.948334` GiB) | `32212254720` bytes (`30.0` GiB) | `31193989120` bytes (`29.051666` GiB, `96.8`%) |
| Referential-proof peak RSS | `579420160` bytes (`0.539627` GiB) | `32212254720` bytes (`30.0` GiB) | `31632834560` bytes (`29.460373` GiB, `98.2`%) |
| Peak scratch usage | `4429922304` bytes (`4.125687` GiB) | `68719476736` bytes (`64.0` GiB) | `64289554432` bytes (`59.874313` GiB, `93.6`%) |
| Final scratch usage | `4429922304` bytes (`4.125687` GiB) | `68719476736` bytes (`64.0` GiB) | `64289554432` bytes (`59.874313` GiB, `93.6`%) |

The referential-proof value is the sampled process-tree RSS before the
assembly staging directory appeared. It covers the landed serial per-basin
catchment-ID-set validation and the child interpreter baseline.

## Batch and row-group interleaving

| Evidence | Result |
|---|---:|
| Assembly input batch size | `1024` |
| Assembly row-group range | `4096` to `8192` |
| Input batches per basin | `[300, 217, 307, 276, 122, 127, 223, 266, 110, 408, 326, 362, 193, 308, 217, 407, 160, 184, 181, 282, 416, 269, 217, 130, 206, 210, 200, 117, 151, 192, 395, 411, 311, 157, 400, 272, 128, 129, 257, 201, 121, 223, 144, 397, 208, 283, 226, 238, 341, 317, 410, 328, 197, 417, 319, 157, 144, 297, 366, 254, 194, 331]` |
| Catchment input row groups per basin | `[300, 217, 307, 276, 122, 127, 223, 266, 110, 408, 326, 362, 193, 308, 217, 407, 160, 184, 181, 282, 416, 269, 217, 130, 206, 210, 200, 117, 151, 192, 395, 411, 311, 157, 400, 272, 128, 129, 257, 201, 121, 223, 144, 397, 208, 283, 226, 238, 341, 317, 410, 328, 197, 417, 319, 157, 144, 297, 366, 254, 194, 331]` |
| Graph input row groups per basin | `[300, 217, 307, 276, 122, 127, 223, 266, 110, 408, 326, 362, 193, 308, 217, 407, 160, 184, 181, 282, 416, 269, 217, 130, 206, 210, 200, 117, 151, 192, 395, 411, 311, 157, 400, 272, 128, 129, 257, 201, 121, 223, 144, 397, 208, 283, 226, 238, 341, 317, 410, 328, 197, 417, 319, 157, 144, 297, 366, 254, 194, 331]` |
| Snap input row groups per basin | `[300, 217, 307, 276, 122, 127, 223, 266, 110, 408, 326, 362, 193, 308, 217, 407, 160, 184, 181, 282, 416, 269, 217, 130, 206, 210, 200, 117, 151, 192, 395, 411, 311, 157, 400, 272, 128, 129, 257, 201, 121, 223, 144, 397, 208, 283, 226, 238, 341, 317, 410, 328, 197, 417, 319, 157, 144, 297, 366, 254, 194, 331]` |
| Output catchment row groups | `3906` |
| Output graph row groups | `3906` |
| Output snap row groups | `3906` |
| Basin-origin transitions | `15999032` |
| Post-first-input-batch transitions | `15935544` |
| Catchment row groups containing multiple basins | `3906` |
| Snap row groups containing multiple basins | `3906` |
| Interleaving proof | `true` |

Every basin consumed multiple 1,024-row input batches and multiple generated
input row groups. Cross-basin transitions continued after the first input
batch, and output catchment and snap row groups contained multiple basin
origins. This is evidence of k-way batch and row-group interleaving rather
than basin concatenation.

## Verifier result

The M3-S1 planetary-output verifier passed with batch size `65536`. It scanned
the complete assembled files and established:

- catchments were non-decreasing by `(hilbert, id)` in file order;
- snap IDs were exactly `1..N` in file order;
- the manifest declared `hfx.aux.snap.v2` at
  `aux/snap_stems.parquet` with `references_levels = [0]`;
- every snap `unit_id` resolved exactly once in `catchments.parquet`.

Report status was `passed` and the real assembly child returned `0`.

## Thirteen-hour budget assessment

`NOT INDICATED BY LOCAL TIMING: Local assembly took 0.228000 hours, below the 10.4-hour at-risk threshold and the 13-hour budget.` The
rehearsal used the planned ccx33 row count, but it ran on a different local
CPU, memory subsystem, filesystem, and operating system. This comparison is
not a CPU-normalized ccx33 extrapolation. ccx33 assembly wall time remains
unproven until M4.

## Conclusion

The one full-scale rehearsal `passed` generation, real assembly, batch and
row-group interleaving inspection, M3-S1 verification, and both configured
resource ceilings. Process-tree RSS retained `96.8` percent headroom and peak
scratch usage retained `93.6` percent headroom.
