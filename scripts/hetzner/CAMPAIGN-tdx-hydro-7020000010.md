# TDX-Hydro 7020000010 pilot campaign record

## Scope

The workstation-driven campaign ran on 2026-07-22 as
`tdx702-pilot` from checkout `433040a606c467351a0e6eb018c6c28ad4ee5a98`. It acquired pristine NGA
inputs, compiled once, validated twice, delivered to scratch storage, and
performed default teardown. This is an evidence record, not a live runbook.

All timestamps are UTC. Raw output is in the local orchestrator evidence log
`planning/2026-07-21-tdx-hydro-single-basin-adapter/milestone-4/step-1/evidence.md`.
E-labels below refer to that log.

## Campaign parameters

| Parameter | Executed value |
|---|---|
| Campaign | `tdx702-pilot` |
| Server / ID | `hfx-build-tdx702-pilot` / `153972727` |
| Server IPv4 | `2.28.15.249` |
| Server type | `ccx33` dedicated core |
| Image / location | `debian-12` / `fsn1` |
| Volume / ID | `hfx-build-tdx702-pilot-data` / `106438595` |
| Volume size / mount | 100 GB / `/mnt/hfx` |
| Checkout SHA | `433040a606c467351a0e6eb018c6c28ad4ee5a98` |
| Fabric version | `NGA-TDX-Hydro-20230126` |
| Scratch URI | `s3://pourpoint-hfx/scratch/tdx-hydro-pilot/2026-07-23T092027Z/` |

S3 credentials were protected at `/etc/pourpoint-hfx.env`; values were not
logged. Current costs were verified before provision. Evidence: E01-E04.

## UTC timeline and results

### 1. Provision and bootstrap

Provision: 2026-07-22T15:11:16Z to 2026-07-22T15:11:34Z, exit
0. Bootstrap: 2026-07-22T15:11:48Z to
2026-07-22T15:13:55Z, exit 0. Bootstrap built
`/root/hfx/target/release/hfx` from
`4ad9c26ea6b2ca960420d7197faa431fcb613a77`, verified toolchains, and found the
adapter. The fix-PR checkouts subsequently converged to compiled revision
`433040a606c467351a0e6eb018c6c28ad4ee5a98`. Evidence: E02-E04 and E17D-E17H.

### 2. Probes and acquisition

HEAD probes exited 0 with basins 5907767296 bytes and streamnet
1676398592 bytes. Available bytes were
99703738368, projected headroom 92119572480, and estimated
sequential times 7584 seconds at 1 MB/s and 1264 seconds at 6 MB/s.
Decision: GO. Evidence: E05-E07.

Basins ran 2026-07-22T15:20:13Z to 2026-07-22T15:34:10Z, exit
0, 5907767296 bytes, SHA-256 `6d75b56428227824749a497b279a5d0891fd67d4c6a82df1b746f620add6da1a`, layer
`basins`. Streamnet followed from 2026-07-22T15:34:46Z to
2026-07-22T15:37:07Z, exit 0, 1676398592 bytes,
SHA-256 `ae3e5c881cad7c4e3b85a594b4e59889e698f5262e18466a1748c13af6948ca9`, layer `TDX_streamnet_7020000010_01`. Both passed
SQLite and ogrinfo checks. Evidence: E09-E14.

### 3. Fabric version and sole build

Pinned `NGA-TDX-Hydro-20230126`: no explicit label or Last-Modified header was
available; the latest `gpkg_contents.last_change` value was streamnet's
2023-01-26T21:57:00.865Z (basins: 2022-06-30T20:53:53.843Z), yielding the
20230126 suffix. Evidence: E13-E15.

The only successful build ran 2026-07-23T08:59:30Z to
2026-07-23T09:15:59Z, exit 0. Output was
`/mnt/hfx/work/tdx-hydro-7020000010`; external report was
`/mnt/hfx/work/tdx-hydro-7020000010-build-report.json`. Evidence:
E17-E20.

Area selected `m2` with relative error
0.1289 over 331263 links.
Orientation recorded 322344 coincidence-proven links,
1702 predecessor-proven roots, and
1767 trusted isolated roots, partitioning every
native reach. Isolated roots remain a documented vertex-order trust assumption.

Diagnostics: 246 contracted edges,
0 contracted roots,
246 traversals, 123 dropped
reaches, 0 basins clamp vertices,
0 streamnet clamp vertices, 3578 roots,
and 1767 trusted polygon-bearing
isolated roots. The real-data diagnostics also recorded 149 degenerate reaches,
5807 short-successor resolutions, and 5424 near-degenerate reach-side
resolutions. Manifest topology was `tree`. Evidence: E19-E20.

### 4. Validation and delivery

Release strict 100-percent validation exited 0. Adapter
HFX/GeoParquet validation exited 0. Both were 0.
Evidence: E21-E22.

Upload ran 2026-07-23T09:20:29Z to 2026-07-23T09:21:19Z, exit 0,
and verified exactly seven objects at
`s3://pourpoint-hfx/scratch/tdx-hydro-pilot/2026-07-23T092027Z/`. Evidence:
E23-E26.

| Object | Bytes | SHA-256 |
|---|---:|---|
| `manifest.json` | 910 | `f424029db57aa40685013e4a0b52d45b4845377ea77e65f417173864822a3033` |
| `catchments.parquet` | 2162910660 | `e74f04f9fd0dfd6eb2a5340e0222c2874e0a7776917d3fe9b9aaadfaa2c64639` |
| `graph.parquet` | 10816141 | `e15e2f6028537a0c776e8b9c31d72633a6f3d6508181f3c867d221c5d50440a4` |
| `aux/snap_stems.parquet` | 673654278 | `a6e30f66cf723610330d582eb903d303a83a793f106a9e29aadaf1b47dae2746` |
| `build-report.json` | 235453 | `4b7731bd6559744c27eaa561b6eba3c3b57f44ab45f405106d8625e36290bc55` |
| `NOTICE` | 1615 | `4563d646ccbb05721974bfbe2b59b4e6c4fcc4a00d337ab64d21bc32f85f1d1f` |
| `CITATION.txt` | 883 | `b84e98110158e1f017c97e2c657a1ce11bf753087bcb18e1195886bd5c3fc0f2` |

### 5. Teardown and zero footprint

Default teardown ran 2026-07-23T09:22:56Z to 2026-07-23T09:23:17Z, exit
0, and reported both exact resources absent. Independent
server/volume listings omitted both names. All VM evidence was local before
teardown; nothing durable survived on the VM. Evidence: E90-E93.

## Phase exits and log references

| Phase | Exit | Evidence | VM canonical log |
|---|---:|---|---|
| Provision | 0 | E02 | n/a |
| Bootstrap | 0 | E03-E04 | n/a |
| Basins | 0 | E09-E10 | `/mnt/hfx/logs/hfx-tdx702-pilot-basins.log` |
| Streamnet | 0 | E11-E12 | `/mnt/hfx/logs/hfx-tdx702-pilot-streamnet.log` |
| Build | 0 | E17-E20 | `/mnt/hfx/logs/hfx-tdx702-pilot-build.log` |
| HFX validate | 0 | E21 | n/a |
| Adapter validate | 0 | E22 | n/a |
| Upload | 0 | E23-E26 | `/mnt/hfx/logs/hfx-tdx702-pilot-upload.log` |
| Teardown | 0 | E90-E93 | n/a |

VM log contents needed by this record were copied locally before teardown.

## Deviations

The first provision attempt met the dedicated-core quota and exited 1; teardown
confirmed zero footprint before the campaign paused. A later provision created
the server but met the volume-size quota and exited 1; after unrelated capacity
was released, the rerun completed the same server and volume. The initial
identity command exited 127 because the Cargo environment was not sourced; the
corrected identity command exited 0. NGA HEAD requests timed out or returned
504, so bounded GET header probes supplied the content lengths. Evidence:
E02-E07.

The acquired files required a `.gpkg` suffix before adapter ingestion. Four
build attempts then exited 1 on real-data cases. The campaign incorporated
adapter fixes from PR #141 for zero-length reaches, PR #142 for DSContArea
recalibration, and PR #144 for short-successor orientation before the final
build succeeded from `433040a606c467351a0e6eb018c6c28ad4ee5a98`. Evidence:
E17-E18F.

## Acceptance criteria map

| Criterion | Evidence | Result |
|---|---|---|
| Area and orientation checks passed | E19-E20 | Done |
| Diagnostic counts recorded | E19-E20 | Done |
| Both validation layers exited 0 | E21-E22 | Done |
| Seven objects verified outside `hfx/` | E23-E26 | Done |
| Teardown and independent absence passed | E90-E93 | Done |

## Conclusion

Basin `7020000010` compiled from pristine NGA TDX-Hydro
`NGA-TDX-Hydro-20230126`, validated, and was verified at
`s3://pourpoint-hfx/scratch/tdx-hydro-pilot/2026-07-23T092027Z/`.
The campaign server and volume were deleted.
