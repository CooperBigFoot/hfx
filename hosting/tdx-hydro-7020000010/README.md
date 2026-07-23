# TDX-Hydro 7020000010 - HFX Dataset

This is an HFX v0.3.0 compilation of pristine National
Geospatial-Intelligence Agency (NGA) TanDEM-X Hydro (TDX-Hydro) data for
processing basin `7020000010`. It is a validated single-level pilot held in a
scratch prefix, not a planetary release.

HFX spec and toolkit: https://github.com/CooperBigFoot/hfx

## Manifest facts

| Field | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | tdx_hydro |
| `fabric_version` | NGA-TDX-Hydro-20230126 |
| `adapter_version` | 0.1.0 |
| `region` | 7020000010 |
| `unit_count` | 331263 |
| `topology` | tree |
| CRS | EPSG:4326 |
| HFX levels | level 0 only |
| `parent_id` | null everywhere |
| `has_up_area` | true |
| Auxiliary | `hfx.aux.snap.v2` at `aux/snap_stems.parquet` |

## Objects

Scratch base URI: s3://pourpoint-hfx/scratch/tdx-hydro-pilot/2026-07-23T092027Z/

| Object | Bytes | SHA-256 |
|---|---:|---|
| `manifest.json` | 910 | `f424029db57aa40685013e4a0b52d45b4845377ea77e65f417173864822a3033` |
| `catchments.parquet` | 2162910660 | `e74f04f9fd0dfd6eb2a5340e0222c2874e0a7776917d3fe9b9aaadfaa2c64639` |
| `graph.parquet` | 10816141 | `e15e2f6028537a0c776e8b9c31d72633a6f3d6508181f3c867d221c5d50440a4` |
| `aux/snap_stems.parquet` | 673654278 | `a6e30f66cf723610330d582eb903d303a83a793f106a9e29aadaf1b47dae2746` |
| `build-report.json` | 235453 | `4b7731bd6559744c27eaa561b6eba3c3b57f44ab45f405106d8625e36290bc55` |
| `NOTICE` | 1615 | `4563d646ccbb05721974bfbe2b59b4e6c4fcc4a00d337ab64d21bc32f85f1d1f` |
| `CITATION.txt` | 883 | `b84e98110158e1f017c97e2c657a1ce11bf753087bcb18e1195886bd5c3fc0f2` |

The prefix is outside `hfx/`. Source GeoPackages and logs are not hosted.

## Source identities

| Product | URL | Bytes | SHA-256 | Layer |
|---|---|---:|---|---|
| basins | `https://earth-info.nga.mil/php/download.php?file=7020000010-basins-gpkg` | 5907767296 | `6d75b56428227824749a497b279a5d0891fd67d4c6a82df1b746f620add6da1a` | `basins` |
| streamnet | `https://earth-info.nga.mil/php/download.php?file=7020000010-streamnet-gpkg` | 1676398592 | `ae3e5c881cad7c4e3b85a594b4e59889e698f5262e18466a1748c13af6948ca9` | `TDX_streamnet_7020000010_01` |

Source version is `NGA-TDX-Hydro-20230126`. No explicit label or Last-Modified
header was available; the latest `gpkg_contents.last_change` value was
streamnet's 2023-01-26T21:57:00.865Z (basins:
2022-06-30T20:53:53.843Z), yielding the 20230126 suffix.
Inputs were accessed 2026-07-22.

## Build and validation

VM checkout `433040a606c467351a0e6eb018c6c28ad4ee5a98` ran the adapter build once. Its report was the
external sibling `/mnt/hfx/work/tdx-hydro-7020000010-build-report.json`.

The empirical area check selected `m2`, checked
331263 polygon-bearing links, and had selected relative
error 0.1289. Orientation recorded
322344 coincidence-proven links,
1702 predecessor-proven roots, and
1767 trusted isolated roots. Isolated-root
orientation is a documented TauDEM/TDX vertex-order trust assumption.

Diagnostics: 246 contracted edges,
0 contracted roots,
246 contracted link traversals,
123 polygon-less dropped reaches,
0 basins clamp vertices, and
0 streamnet clamp vertices. Full ID lists and
counts are in `build-report.json`.

The release validator passed strict 100-percent sampling, and the adapter
wrapper passed HFX plus GeoParquet checks.

## License and citation

NGA TDX-Hydro is licensed under Creative Commons Attribution-ShareAlike 4.0
International (CC BY-SA 4.0):
https://creativecommons.org/licenses/by-sa/4.0/legalcode

This adaptation is distributed under the same license. See `NOTICE` and
`CITATION.txt`.

## Provenance

- Source: pristine NGA TDX-Hydro processing basin `7020000010`
- Source version: `NGA-TDX-Hydro-20230126`
- Adapter checkout: `433040a606c467351a0e6eb018c6c28ad4ee5a98`
- HFX format version: 0.3.0
- Built UTC: `2026-07-23T08:59:31.405850+00:00`
- Validated UTC: `2026-07-23T09:19:08Z`
- Uploaded: `s3://pourpoint-hfx/scratch/tdx-hydro-pilot/2026-07-23T092027Z/`
- Campaign record: `scripts/hetzner/CAMPAIGN-tdx-hydro-7020000010.md`
