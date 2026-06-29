# GRIT 2.0.0 — HFX Dataset

This dataset is an HFX v0.3.0 compilation of the Global River Topology (GRIT)
vector datasets, with segment (`level=0`) and reach (`level=1`) drainage
units.

HFX spec and toolkit: https://github.com/CooperBigFoot/hfx

## Manifest facts

| Field | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | grit |
| `fabric_version` | 1.0.0 |
| `adapter_version` | grit-global-2.0.0 |
| `unit_count` | 22,337,300 |
| Total size | ~43 GB |
| CRS | EPSG:4326 |
| `topology` | dag |
| Auxiliaries | two `hfx.aux.snap.v2` snap indexes (segment-stems, reach-stems) |

The `catchments.parquet` and both snap parquet files carry the GeoParquet 1.1
`bbox` covering struct (`xmin`/`ymin`/`xmax`/`ymax`) with per-row-group leaf
statistics, enabling spatial-range pushdown.

## Objects

Base URL: https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/

- `manifest.json`
- `catchments.parquet`
- `graph.parquet`
- `aux/snap_segments.parquet`
- `aux/snap_reaches.parquet`
- `NOTICE`, `CITATION.txt`, `README.md` (attribution objects)

## License and citation

License: CC BY-NC 4.0
(https://creativecommons.org/licenses/by-nc/4.0/), inherited from the source
data. NonCommercial use only.

Source data citation:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) vector datasets”. Zenodo. doi:10.5281/zenodo.17435232.

DOI: 10.5281/zenodo.17435232 (https://doi.org/10.5281/zenodo.17435232)

See `CITATION.txt` in this directory for the BibTeX entry.

## Validating a download

```sh
git clone https://github.com/CooperBigFoot/hfx && cd hfx && cargo run -p hfx-validator -- /path/to/downloaded/grit-hfx-v0.3.0 --strict
```

Alternatively, `cargo install hfx-validator` (version 0.4.0 or later; earlier
published versions predate spec 0.3.0), then run
`hfx-validator /path/to/downloaded/grit-hfx-v0.3.0 --strict`.

## DAG `up_area_km2` semantics

Choice: partitioned (option a). Each segment's `up_area_km2` reflects the
source-area share routed through that segment.

Algorithm: per-segment chain anchor. Segment rows use GRIT
`drainage_area_out` directly. Reach rows are computed by anchoring each
parent segment's outlet reach to the segment `drainage_area_out`, then
walking upstream within that segment and subtracting each downstream reach's
local `area_km2`.

Consumer caveat: in DAG split-rejoin geometry, the sum of `up_area_km2` over
a flow set is not the watershed area. Consumers must use the graph plus
`level=1` reaches for true watershed accumulation.

## Known data caveats

44 reach rows have `up_area_km2=NULL`, spread across 17 segments where the
chain-anchor algorithm could not resolve the outlet. These are anomalies in
the GRIT v1.0 source topology, not defects of the HFX encoding.
`has_up_area` remains true; nulls are permitted per HFX spec. They
represent <0.001% of rows. See `adapters/grit-v2/build_adapter.py` in the
HFX repository for the detection rule.

Fallback segment IDs:

- AF: 140004152, 390037110, 480000538
- AS: 180020690
- EU: 230045414
- NA: 190003639, 220083647, 300005125, 360109293, 410046446
- SA: 9627, 120012564
- SI: 110020043, 110020254, 430009780, 580009350
- SP: 90000652

## Provenance

- Source: GRIT v1.0 (https://doi.org/10.5281/zenodo.17435232)
- Adapter: grit-v2 (`adapters/grit-v2/build_adapter.py`, FORMAT_VERSION 0.3.0)
- HFX spec version: 0.3.0
- Bbox: planetary [-180, -90, 180, 90]
- Row count: 22,337,300 catchments across 2 levels

## Hosting

Upstream Tech sponsors the hosting infrastructure for this dataset
(infrastructure sponsor only). Upstream Tech is not the data publisher or
vendor.
