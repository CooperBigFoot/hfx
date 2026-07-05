# Anatomy of a dataset

You can inspect a complete HFX dataset by opening four surfaces: the manifest, the catchments table, the graph table, and any auxiliary declarations.
This tour uses `examples/tiny/` as the worked reference and stays at the file-inspection layer.

## The reference dataset

First, open `examples/tiny/`, a complete HFX `format_version` `0.3.0` dataset with five drainage units, one polygonal hydrologic unit at one dataset-local resolution level.
The directory contains `manifest.json`, `catchments.parquet`, `graph.parquet`, human-readable CSV dumps named `catchments.csv` and `graph.csv`, and `README.md`.
The dataset models a tiny 2x2 world in EPSG:4326, the coordinate reference system that stores longitude and latitude.
Unit 1 is the root drainage unit at `level` 0, the coarsest dataset-local resolution tier.
It covers the full `bbox` `[0, 0, 2, 2]`, where `bbox` means `[minx, miny, maxx, maxy]`, and it has `area_km2` 4.0 with null `parent_id`.
Units 2, 3, 4, and 5 sit at `level` 1, each has `parent_id` 1, each has `area_km2` 1.0, and each covers a 1x1 quadrant.

## Inspect the manifest

Next, inspect `manifest.json`, the sidecar metadata file for the dataset.
It gives you the dataset identity, coordinate system, topology class, row count, and optional auxiliary artifact declarations.

```sh
python3 - <<'PY'
# Load the manifest with Python's standard JSON module.
import json  # Use the standard library parser.

# Open the tiny dataset manifest from the repository root.
with open("examples/tiny/manifest.json", encoding="utf-8") as f:  # Read the manifest file.
    manifest = json.load(f)  # Parse JSON into a Python dictionary.

# Print the identity and shape fields you usually check first.
for key in ("format_version", "fabric_name", "crs", "topology", "unit_count", "bbox", "has_up_area"):  # Choose stable manifest fields.
    print(f"{key}: {manifest[key]}")  # Print each field as key and value.

# Expected values include format_version 0.3.0, fabric_name conformance-tiny, unit_count 5, and has_up_area False.
PY
```

You just saw the identity fields for the tiny dataset.
`fabric_name` is the source fabric identifier, `topology` is either `tree` or `dag`, and `unit_count` is the row count of `catchments.parquet`.
Here `has_up_area` is `false`, so the `up_area_km2` column carries null values for every row.

The tiny manifest contains `format_version` `"0.3.0"`, `fabric_name` `"conformance-tiny"`, `crs` `"EPSG:4326"`, `has_up_area` `false`, `topology` `"tree"`, `bbox` `[0.0, 0.0, 2.0, 2.0]`, `unit_count` `5`, `created_at` `"2026-01-01T00:00:00Z"`, and `adapter_version` `"conformance-fixture-v2"`.
It omits the optional `fabric_version`, `region`, and `auxiliary` fields.
A manifest can declare optional `auxiliary[]` entries, but tiny declares zero auxiliary entries.

## Inspect catchments.parquet

Next, inspect `catchments.parquet`, the Parquet table with one row per drainage unit.
DuckDB, an in-process SQL engine that reads Parquet directly, is a concise way to view the table shape.

```sh
duckdb -c "DESCRIBE SELECT * FROM 'examples/tiny/catchments.parquet';" # List columns and physical types from the repository root.
```

You should see the HFX catchments schema in file order.
The `bbox` column is a Parquet struct with `float32` leaves named `xmin`, `ymin`, `xmax`, and `ymax`.
The `geometry` column is binary WKB, well-known binary, the standard binary encoding for vector geometry.

The catchments table has these columns in order: `id` as non-null `int64`, `level` as non-null `int16`, nullable `parent_id` as `int64`, `area_km2` as non-null `float32`, nullable `up_area_km2` as `float32`, `outlet_lon` as non-null `float64`, `outlet_lat` as non-null `float64`, non-null `bbox` as `struct<xmin: float32, ymin: float32, xmax: float32, ymax: float32>`, non-null `geometry` as binary WKB, nullable `source_id` as `string`, and nullable `level_label` as `string`.
The `id` column is the unique unit ID within the dataset, and `id = 0` is reserved.
The `level` column names the dataset-local resolution tier.
The `parent_id` column names the containing coarser unit, with null values on root units.
The `area_km2` column stores geodesic area in square kilometers.
The `up_area_km2` column stores inclusive upstream area at this level when the producer precomputes it.
The outlet columns store longitude and latitude in EPSG:4326.
The `source_id` column can carry a source-fabric identifier, and tiny fills it with `src-1` through `src-5`.
The `level_label` column can carry a source-fabric tier label, and tiny uses `coarse` on the root and `fine` on level 1.

`bbox` is also a GeoParquet 1.1 covering.
GeoParquet is a convention for storing geometry in Parquet.
A covering is a per-row bounding box registered in file metadata so spatial tools can read it for filtering.

```sh
duckdb -c "SELECT id, level, parent_id, area_km2, outlet_lon, outlet_lat FROM 'examples/tiny/catchments.parquet' ORDER BY id;" # Show the five drainage units and their outlet coordinates.
```

You should see five rows ordered by `id`.
Unit 1 has `level` 0, null `parent_id`, `area_km2` 4.0, and outlet coordinates `(1.0, 0.0)`.
Units 2 through 5 have `level` 1, `parent_id` 1, and `area_km2` 1.0.

```sh
duckdb -c "SELECT id, bbox.xmin AS xmin, bbox.ymin AS ymin, bbox.xmax AS xmax, bbox.ymax AS ymax FROM 'examples/tiny/catchments.parquet' ORDER BY id;" # Expand the GeoParquet covering struct into readable bbox leaves.
```

You should see unit 1 with bbox leaves `(0, 0, 2, 2)`.
You should see units 2 through 5 with 1x1 bbox leaves.
Those row boxes match the tiny 2x2 world described by the manifest bbox.

```python
import geopandas as gpd  # Use GeoPandas to read GeoParquet.

# Read the GeoParquet catchments table from the repository root.
gdf = gpd.read_parquet("examples/tiny/catchments.parquet")  # Decode the Parquet geometry metadata.

# Print the first geometries as decoded vector shapes.
print(gdf.geometry.head())  # Show the first polygon geometries.

# The CSV dump renders the root geometry as WKT text, for example POLYGON ((2 0, 2 2, 0 2, 0 0, 2 0)).
```

GeoPandas, a Python library that reads GeoParquet into a geospatial data frame, decodes the WKB geometry column as polygons.
The tiny root polygon covers the full 2x2 extent in EPSG:4326.
The CSV dump writes WKB as WKT text, which is well-known text for readable vector geometry.

## Inspect graph.parquet

Next, inspect `graph.parquet`, the same-level upstream adjacency graph over drainage units.
An adjacency graph records direct upstream unit IDs at the same level.
The table carries one row for every drainage unit and carries zero geometry columns.
Its bounding box shape is four flat `float32` columns named `bbox_minx`, `bbox_miny`, `bbox_maxx`, and `bbox_maxy`.

```sh
duckdb -c "DESCRIBE SELECT * FROM 'examples/tiny/graph.parquet';" # List the graph columns and physical types.
```

You should see `id`, `level`, `upstream_ids`, and four flat bbox columns.
The `id` column is a foreign key into `catchments.parquet`.
The `upstream_ids` column is a non-null list of direct upstream unit IDs at the same level.

```sh
duckdb -c "SELECT id, upstream_ids FROM 'examples/tiny/graph.parquet' ORDER BY id;" # Show the tiny same-level adjacency lists.
```

You should see unit 1 with `[]`, unit 2 with `[]`, unit 3 with `[]`, unit 4 with `[2,3]`, and unit 5 with `[4]`.
Every unit in `catchments.parquet` has exactly one graph row.
Every `upstream_ids` entry references an existing unit at the same level.
Cross-level relationships live in `parent_id`.

## Auxiliary artifacts

Finally, inspect the manifest for auxiliary artifacts, optional declared data that an HFX dataset may carry.
The tiny dataset declares zero `auxiliary[]` entries and ships zero auxiliary files.

HFX blesses `hfx.aux.d8_raster.v1`, a paired D8 flow-direction raster and flow-accumulation raster.
D8 is an eight-direction flow-routing convention where each cell points to one of eight neighbors.
This auxiliary schema requires artifact keys `flow_dir` and `flow_acc`, and both paths point at Cloud-Optimized GeoTIFF files.
Its metadata requires `flow_dir_encoding`, with value `"esri"` or `"taudem"`.
The `flow_dir` raster is `uint8` with NoData `255`.
The `flow_acc` raster is `float32` with NoData `-1.0`.
Read the schema at [`spec/aux/d8_raster/v1.md`](../spec/aux/d8_raster/v1.md).

HFX also blesses `hfx.aux.snap.v2`, optional reach or node geometries used to attach an outlet point to a drainage unit.
This auxiliary schema requires the artifact key `snap`, which points at a Parquet file.
Its columns are `id` as `int64`, `unit_id` as `int64`, `weight` as `float32`, nullable `stem_role` as `string` with values `mainstem`, `tributary`, `distributary`, or `unknown`, optional nullable `bbox` as a struct covering, and `geometry` as WKB Point or LineString.
Its metadata requires `name`, `description`, `references_levels`, and `weight_semantics`.
Read the schema at [`spec/aux/snap/v2.md`](../spec/aux/snap/v2.md).

You now have the inspection path for the core HFX files in `examples/tiny/`.
Use the canonical [HFX specification](../spec/HFX_SPEC.md) for the full artifact contract.
Use the [Glossary](../understanding/glossary.md) for domain terms that appear across the documentation.
