# Build an adapter

This guide shows you how to build an adapter, a tool that compiles a source hydrofabric into a conformant HFX dataset.
A source hydrofabric is a dataset describing a region's river network.
This guide targets HFX 0.3.0.
It supports the normative specification and does not replace it.
Read the [specification](../spec/HFX_SPEC.md) first.

## Prerequisites

Before you start, gather these tools:

- Python 3.11 or later for a Python adapter. Rust and other languages are valid.
- `uv` for Python dependency management when you follow this repository's adapter pattern.
- The `hfx` command-line validator on your `PATH`. Install it with `cargo install --path crates/hfx-cli` from a checkout of this repository.
- A full read of the [specification](../spec/HFX_SPEC.md).
- Common Python libraries: `geopandas`, `pyarrow`, `shapely`, and `pyogrio`.

## Mental model

An adapter is a one-way offline compile step.
Raw source data goes in, and conformant HFX artifacts come out.
A consumer, a program that reads an HFX dataset, then reads the compiled output.

An HFX 0.3.0 dataset is a flat directory with three required core artifacts and optional declared extras:

| Artifact | Required | Description |
|---|---|---|
| `catchments.parquet` | Yes | Drainage-unit polygons with levels, parents, outlets, and a bounding-box covering |
| `graph.parquet` | Yes | Same-level upstream adjacency graph |
| `manifest.json` | Yes | Dataset identity, topology, counts, and auxiliary declarations |
| Auxiliary artifacts | No | Files declared in `manifest.json` under `auxiliary` |

HFX 0.3.0 carries snap features and raster refinement inputs as auxiliary data.
Snap features use the [`hfx.aux.snap.v2`](../spec/aux/snap/v2.md) schema.
The blessed D8 raster schema is [`hfx.aux.d8_raster.v2`](../spec/aux/d8_raster/v2.md).

## Glossary

| Term | Definition |
|---|---|
| Adapter | A tool that compiles a source hydrofabric into conformant HFX artifacts |
| Drainage unit | One polygonal hydrologic unit at one dataset-local level, one row of `catchments.parquet` |
| Level | A dataset-local resolution tier, where `0` is the recommended coarsest tier |
| Parent | The containing coarser drainage unit referenced by `parent_id` |
| Unit outlet | The single EPSG:4326 coordinate where water exits a drainage unit |
| Headwater | A unit with no same-level upstream neighbors |
| Topology tree | A strictly convergent network where each unit has at most one downstream same-level neighbor |
| Topology dag | A network with bifurcations, where one unit may drain to multiple downstream units |
| `up_area_km2` | The inclusive cumulative upstream area at the same level |
| Hilbert sort | Row ordering by Hilbert curve index on centroid coordinates, which lets a consumer skip row groups |
| Bounding-box covering | A GeoParquet 1.1 struct column that stores each row's bounding box for spatial pruning |
| Auxiliary schema | A declared non-core data contract such as `hfx.aux.d8_raster.v2` |

## Pipeline overview

Build the dataset in nine stages:

1. Inspect the source fabric.
2. Assign stable unit IDs.
3. Derive levels and parents.
4. Reproject to EPSG:4326.
5. Validate geometries and outlets.
6. Hilbert-sort and write `catchments.parquet`.
7. Build and write `graph.parquet`.
8. Declare optional auxiliary data.
9. Write `manifest.json`.

Run the validator after each stage during development.
Schema and ID errors caught early prevent cascading failures.

## Pipeline stages

### Inspect the source fabric

Answer these questions before you write code:

- Which source features become HFX drainage units?
- Which source fields define resolution tiers?
- Do the available tiers nest perfectly, or should they be separate datasets?
- Does the source have explicit stream-line or node features for snap data?
- Does the fabric partition drainage at bifurcations?
- Which source field maps to the HFX `id`, and is it positive, unique, and stable?
- Which optional auxiliary artifacts should you declare?

### Assign stable unit IDs

Make every `id` an `int64` value that is strictly positive and unique within the dataset.
The value `0` is reserved and must never appear in `catchments.parquet` or `graph.parquet`.
If the source uses strings or UUIDs, create a stable integer mapping and persist it for cross-table consistency.

### Derive levels and parents

Give each drainage unit a dataset-local `level`.
Number the coarsest available tier `0` and use increasing values for finer tiers.

Reference the next coarser containing unit through `parent_id`.
If an intermediate source tier is absent from the HFX dataset, let `parent_id` skip to the next available coarser unit.
Give root units a null `parent_id`.

Keep the parent relation an acyclic forest.
Make parent levels strictly coarser than child levels.
Keep sibling interiors disjoint, and make children tile their parent within validator tolerance.

### Reproject to EPSG:4326

Put all vector data in EPSG:4326.
Do not embed a bare `"EPSG:4326"` string in the GeoParquet metadata.
GeoParquet 1.1 requires a PROJJSON dict, a JSON encoding of a coordinate reference system, or no `crs` key, and an absent key defaults to OGC:CRS84, which is semantically equivalent.
Read the GeoParquet section below for the metadata details.

### Validate geometries and outlets

Run `shapely.make_valid` or `ST_MakeValid` on every drainage-unit polygon before you write it.
Source fabrics often carry slivers, duplicate vertices, and self-touching rings.

Give every unit exactly one outlet coordinate:

- A flow-through unit puts its outlet on the boundary shared with the downstream same-level unit.
- A terminal sink puts its outlet inside the unit polygon for interior drainage, or on the dataset boundary for a coastal terminus.
- A DAG fork unit puts its outlet at the fan-out boundary node.

Outlet tolerance is a validator parameter.
The manifest does not store it.

### Hilbert-sort and write catchments.parquet

Sort rows by Hilbert curve index computed on centroid coordinates.
For a dataset with multiple levels, sort by `(level ASC, hilbert_index ASC)`.
A single-level dataset keeps the Hilbert-only rule.
This ordering lets a consumer skip row groups through bounding-box statistics.

```python
# Compute a Hilbert index on centroids and sort rows by it.
centroids = gdf.geometry.centroid
gdf["hilbert_index"] = centroids.hilbert_distance(total_bounds=gdf.total_bounds)
gdf = gdf.sort_values(["level", "hilbert_index", "id"], kind="mergesort").reset_index(drop=True)
```

The snippet computes a Hilbert index per row and sorts by level, then that index, then id.
The `hilbert_index` is a sort key only, so do not store it as a column.

Write these required columns:

`id`, `level`, `parent_id`, `area_km2`, `up_area_km2`, `outlet_lon`, `outlet_lat`, `bbox`, and `geometry`.

The `bbox` column is a single non-nullable Parquet struct with four `float32` leaves named `xmin`, `ymin`, `xmax`, and `ymax`.
Each leaf holds the bounds of that row's `geometry` in EPSG:4326.
This struct is a GeoParquet 1.1 covering, a struct column that spatial tools read to skip data blocks that cannot match a query.
Read the GeoParquet section below for the metadata you must declare.
The `geometry` column holds a Polygon or MultiPolygon as WKB, the well-known binary geometry encoding.

You may add two optional columns:

`source_id` and `level_label`.

Write one row group for a file with fewer than 4,096 rows.
Use row groups of 4,096 to 8,192 rows for larger files.
Write Parquet row-group statistics on the four struct leaves `bbox.xmin`, `bbox.ymin`, `bbox.xmax`, and `bbox.ymax`.

### Build and write graph.parquet

Write a Parquet file with the columns `id` (`int64`), `level` (`int16`), `upstream_ids` (`list<int64>`), `bbox_minx`, `bbox_miny`, `bbox_maxx`, and `bbox_maxy` (each `float32`).
Every unit ID from `catchments.parquet` must appear exactly once.
A headwater gets `upstream_ids = []`.
The four `bbox_*` columns hold the referenced unit's bounds, so a consumer can skip graph row groups.

Graph edges are same-level only.
Cross-level relationships belong in `parent_id`.
Detect and break cycles during the compile step, because fixing them after validation is costly.

Sort graph rows by the same Hilbert rule as catchments, computed on the referenced unit's centroid.
Write row-group statistics on the four `bbox_*` columns.

### Declare optional auxiliary data

HFX 0.3.0 carries snap features and rasters as auxiliary data.
Declare each auxiliary artifact in `manifest.json` under `auxiliary`.

Provide snap features when the source has useful reach or node geometries.
Snap features use the [`hfx.aux.snap.v2`](../spec/aux/snap/v2.md) schema.
Read that schema page for its column contract.

For D8 rasters, declare the [`hfx.aux.d8_raster.v2`](../spec/aux/d8_raster/v2.md) schema:

```json
{
  "schema": "hfx.aux.d8_raster.v2",
  "artifacts": {
    "flow_dir": "flow_dir.tif",
    "flow_acc": "flow_acc.tif"
  },
  "metadata": {
    "crs": "EPSG:8857",
    "flow_dir_encoding": "grass",
    "flow_acc_units": "km2"
  }
}
```

Set `metadata.crs` to the uppercase EPSG authority string resolved by both raster headers.
Set `metadata.flow_dir_encoding` to `esri`, `taudem`, or `grass` and set `metadata.flow_acc_units` to `cells` or `km2`.
Keep the D8 pair on the source grid where its neighbor pointers were derived.
Write both files as internally tiled Cloud-Optimized GeoTIFFs with the same CRS, dimensions, affine transform, pixel dimensions, and grid alignment.
Use `uint8` or `int8` for the `flow_dir` header dtype.
Use `float32` or `int32` for the `flow_acc` header dtype.
An entry with `flow_acc_units` set to `cells` requires `float32`; an entry set to `km2` permits `int32` or `float32`.
Give each raster header a nodata tag with a producer-selected value.
The GeoTIFF headers are authoritative for dtype and nodata, so omit both properties from auxiliary metadata.

### Write manifest.json

```json
{
  "format_version": "0.3.0",
  "fabric_name": "my-fabric",
  "fabric_version": "2026.1",
  "crs": "EPSG:4326",
  "has_up_area": true,
  "topology": "tree",
  "region": "north-america",
  "bbox": [-140.0001, 24.9999, -52.9999, 60.0001],
  "unit_count": 82341,
  "created_at": "2026-05-14T00:00:00Z",
  "adapter_version": "1.0.0"
}
```

Write `fabric_name` as lowercase ASCII with no whitespace.
Write `created_at` as an RFC 3339 timestamp.
Make `unit_count` equal the row count of `catchments.parquet`.
Let `bbox` enclose all units, padded outward by a small epsilon if you wish, and keep planetary bounds within `[-180, -90, 180, 90]`.

Do not write the removed legacy manifest fields: `has_rasters`, `has_snap`, `fabric_level`, `flow_dir_encoding`, `terminal_sink_id`, and `atom_count`.

## GeoParquet and HFX bounding boxes

The `catchments.parquet` file stores its per-row bounds in a single `bbox` struct column with four `float32` leaves.
That struct is a GeoParquet 1.1 covering, so declare it in the file-level `geo` metadata.
A consumer reads the covering to skip row groups through Parquet column statistics before it deserializes geometry.

Declare the covering at the path `geo.columns.geometry.covering.bbox`, with each entry pointing at the matching struct leaf:

```python
# Build GeoParquet 1.1 metadata that declares the bbox covering.
import json
import pyarrow as pa
import pyarrow.parquet as pq

def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }
    return {b"geo": json.dumps(geo).encode("utf-8")}
```

The function returns the `geo` metadata block that names the primary geometry column and points the four covering references at the leaves of the `bbox` struct.
Attach this metadata to the Parquet schema before you write the file, and verify the leaf statistics after each write.

The `graph.parquet` file has no geometry column, so it carries flat `bbox_minx`, `bbox_miny`, `bbox_maxx`, and `bbox_maxy` columns instead of a covering struct.

## Topology choice

Set `topology` to `"tree"` for a strictly convergent network.
Set `topology` to `"dag"` when bifurcations exist.

The `up_area_km2` value is same-level only:

- When you precompute inclusive same-level upstream area, set `has_up_area` to `true` and populate `up_area_km2`.
- When the area is absent or ambiguous, set `has_up_area` to `false` and leave `up_area_km2` null.
- For a DAG fabric that partitions area at bifurcations, prefer `has_up_area = false` unless the source semantics match the inclusive same-level definition.

When you are uncertain, omit upstream area.
An incorrect value produces a silent correctness bug, and an absent value stays recoverable.

## Adapter author checklist

### Catchments

- [ ] `id` is `int64`, positive, and unique, and `0` does not appear.
- [ ] `level` is `int16` and non-negative.
- [ ] `parent_id` is null or references an existing coarser unit.
- [ ] The parent relation is acyclic.
- [ ] Sibling interiors are disjoint within tolerance.
- [ ] Finer units tile parent units within tolerance.
- [ ] `outlet_lon` and `outlet_lat` are finite WGS84 coordinates.
- [ ] `geometry` is a valid Polygon or MultiPolygon in WKB.
- [ ] Rows are Hilbert-sorted, and multi-level datasets sort by level first.
- [ ] `bbox` is a non-nullable struct with `float32` leaves that carry row-group statistics.
- [ ] The GeoParquet 1.1 covering is declared in the `geo` metadata.

### Graph

- [ ] The file is `graph.parquet`.
- [ ] Every unit has exactly one graph row.
- [ ] `level` matches the referenced catchment unit.
- [ ] Every `upstream_ids` entry references an existing same-level unit.
- [ ] The same-level graph is acyclic.
- [ ] The four `bbox_*` columns are present and carry row-group statistics.

### Auxiliary

- [ ] Each `auxiliary` entry has a `schema`, a non-empty `artifacts` map, and an object `metadata`.
- [ ] Snap features, when present, follow `hfx.aux.snap.v2`.
- [ ] Artifact paths are relative and stay inside the dataset root.
- [ ] An `hfx.aux.d8_raster.v2` entry contains `flow_dir`, `flow_acc`, `metadata.crs`, `metadata.flow_dir_encoding`, and `metadata.flow_acc_units`.
- [ ] Each D8 raster header supplies its authoritative dtype and nodata tag, and both headers resolve to the declared EPSG CRS.
- [ ] A third-party schema uses a reverse-DNS style identifier.

### Manifest

- [ ] `format_version` is `"0.3.0"`.
- [ ] `fabric_name` is lowercase ASCII with no whitespace.
- [ ] `created_at` is RFC 3339.
- [ ] `unit_count` equals the `catchments.parquet` row count.
- [ ] The removed legacy fields are not present.

### Validation

- [ ] `hfx --strict --sample-pct 100 ./dataset` passes.
- [ ] JSON Schema validation against `schemas/manifest.schema.json` passes.
- [ ] Any auxiliary metadata schema validation passes.
