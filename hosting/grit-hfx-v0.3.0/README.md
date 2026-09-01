# GRIT 2.0.0: HFX Dataset

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
| `adapter_version` | grit-global-2.1.0 |
| `unit_count` | 22,337,300 |
| Total size | 299,117,889,306 bytes |
| CRS | EPSG:4326 |
| `topology` | dag |
| Auxiliaries | two `hfx.aux.snap.v2` snap indexes (segment-stems, reach-stems) and exactly one `hfx.aux.d8_raster.v2` planetary D8 entry |

The `catchments.parquet` and both snap parquet files carry the GeoParquet 1.1
`bbox` covering struct (`xmin`/`ymin`/`xmax`/`ymax`) with per-row-group leaf
statistics, enabling spatial-range pushdown.

The planetary `hfx.aux.d8_raster.v2` entry maps
`flow_dir=aux/d8/flow_dir.tif` and `flow_acc=aux/d8/flow_acc.tif`. Its metadata
object is exactly
`{"crs": "EPSG:8857", "flow_dir_encoding": "grass", "flow_acc_units": "km2"}`.
Both COGs share an `EPSG:8857` grid of `1,070,000 x 500,000` with transform
`[30.0, 0.0, -15000000.0, 0.0, -30.0, 8400000.0]`. The direction COG has one
`uint8` band with nodata `255`; the accumulation COG has one `float32` band
with NaN nodata. Both COG validators returned `valid=true`, `errors=[]`, and
`warnings=[]`.

## Objects

Base URL: https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/

- `manifest.json`
- `catchments.parquet`
- `graph.parquet`
- `aux/snap_segments.parquet`
- `aux/snap_reaches.parquet`
- `aux/d8/flow_dir.tif`
- `aux/d8/flow_acc.tif`
- `NOTICE`, `CITATION.txt`, `README.md` (attribution objects)

| Object | Bytes | SHA-256 |
| --- | ---: | --- |
| `manifest.json` | `1,426` | `02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c` |
| `aux/d8/flow_dir.tif` | `50,686,516,478` | `eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656` |
| `aux/d8/flow_acc.tif` | `205,069,870,081` | `30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b` |
| `catchments.parquet` | `32,508,030,908` | `50c987343181af8a848170cf121571e7ae815ac491b93bcf9cc07a04a9e12c59` |
| `graph.parquet` | `699,720,490` | `8f61a64fb6746213638655053118eb47c939a4f89796f4a1ef017ab0fd923e81` |
| `aux/snap_segments.parquet` | `3,674,757,248` | `cab39a2be4333cdd8e9a02b78186fa6f6ff3d55c761e670e7104605eeab4dda3` |
| `aux/snap_reaches.parquet` | `6,478,991,001` | `afd994adb2fbfdf25e09dc51b4e1441eb74e34ed5647a6a5b3b4e2a67dc9dfc3` |

The normalized post-attach identity gate matched all four vector byte counts
and SHA-256 values exactly. This records that raster attachment left the vector
artifacts byte-identical.

## License and citation

License: CC BY-NC 4.0
(https://creativecommons.org/licenses/by-nc/4.0/), inherited from both source
datasets. The combined derived HFX dataset is available for NonCommercial use
only.

Vector source dataset:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) vector datasets”. Zenodo. doi:10.5281/zenodo.17435232.

Zenodo record 17435232. DOI: 10.5281/zenodo.17435232
(https://doi.org/10.5281/zenodo.17435232)

Raster source dataset, including the drainage_direction and width-partitioned
drainage_area rasters used by this compilation:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) raster datasets”. Zenodo. doi:10.5281/zenodo.15715535.

Zenodo record 15715535. DOI: 10.5281/zenodo.15715535
(https://doi.org/10.5281/zenodo.15715535)

Related paper:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT): A Bifurcating River Hydrography”. Water Resources Research, 61, e2024WR038308. doi:10.1029/2024WR038308.

DOI: 10.1029/2024WR038308
(https://doi.org/10.1029/2024WR038308)

See `CITATION.txt` in this directory for the plain-text and BibTeX entries.

## Validating a download

```sh
git clone https://github.com/CooperBigFoot/hfx && cd hfx && cargo run -p hfx-cli -- /path/to/downloaded/grit-hfx-v0.3.0 --strict
```

Alternatively, clone this repository, run `cargo install --path crates/hfx-cli`,
then run `hfx /path/to/downloaded/grit-hfx-v0.3.0 --strict`.

Planetary artifact validation record from the validated build:

```bash
cargo build -p hfx-cli
/usr/bin/time -p ./target/debug/hfx /mnt/hfx/scratch/grit-hfx-global --strict
```

Both commands exited `0`. The complete strict report was:

```text
0 error(s), 0 warning(s), 0 info(s)
Result: VALID
```

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

- Vector source: GRIT v1.0 vector datasets, Zenodo record 17435232
  (https://doi.org/10.5281/zenodo.17435232)
- Raster source: GRIT v1.0 raster datasets, Zenodo record 15715535
  (https://doi.org/10.5281/zenodo.15715535); this compilation uses
  drainage_direction and width-partitioned drainage_area
- Paper: Wortmann et al. 2025, Water Resources Research,
  doi:10.1029/2024WR038308 (https://doi.org/10.1029/2024WR038308)
- Adapter: grit-v2 (`adapters/grit-v2/build_adapter.py`, FORMAT_VERSION 0.3.0)
- HFX spec version: 0.3.0
- Bbox: planetary [-180, -90, 180, 90]
- Row count: 22,337,300 catchments across 2 levels

## Hosting

Upstream Tech sponsors the hosting infrastructure for this dataset
(infrastructure sponsor only). Upstream Tech is not the data publisher or
vendor.

## Publication status

**Status: PUBLISHED AND ACCEPTED**

The human-authorized in-place switch is complete. The canonical public
`manifest.json` is 1,426 bytes with SHA-256
`02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c` and
declares exactly one planetary `hfx.aux.d8_raster.v2` entry. The released
reader floor for this declaration is pourpoint 0.3.0.

The repository retains the former 1,132-byte manifest as byte-exact rollback
material. `AUTHORITY.md`, `canonical-publication.json`, `containment.json`, and
`revert-rehearsal.json` record the declaration authority, human-controlled
publication, accepted exposure interval, cache-clearing guidance, and verified
rollback rehearsal. Those records are the operational and historical authority;
this README describes the current public dataset.
