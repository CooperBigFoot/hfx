# `tiny` — reference HFX dataset

A complete, valid HFX `format_version` 0.3.0 dataset with 5 drainage units (`fabric_name` `conformance-tiny`, EPSG:4326, `topology` tree, ~7 KB across the three artifacts). It exists as a READ-ONLY inspectable reference for implementers — modeled on geoparquet's `examples/` pattern of shipping a real artifact alongside human-readable dumps — so you can open a working dataset by hand while reading [`../../spec/HFX_SPEC.md`](../../spec/HFX_SPEC.md), the canonical contract.

## Provenance

The `manifest.json` and the two parquet files are copied **byte-for-byte** from [`../../conformance/valid/tiny`](../../conformance/valid/tiny), which is the source of truth (produced by [`../../conformance/generate_fixtures.py`](../../conformance/generate_fixtures.py)). Never edit `examples/tiny` in place — if the fixture ever changes, re-copy the artifacts from it and regenerate the CSV dumps.

## Artifacts

| File | Size | Description |
|---|---|---|
| [`manifest.json`](./manifest.json) | 295 B | Dataset metadata: `format_version` 0.3.0, `fabric_name`, CRS, bbox, `unit_count` 5, `has_up_area: false`, `topology: tree`. |
| [`catchments.parquet`](./catchments.parquet) | ~5 KB | One row per drainage unit: ids, levels, areas, outlets, a GeoParquet 1.1 `bbox` covering struct, WKB polygon geometry. |
| [`graph.parquet`](./graph.parquet) | 2.2 KB | Topology: one row per unit with its `upstream_ids` adjacency list and bbox. |
| [`catchments.csv`](./catchments.csv) | 554 B | Human-readable dump of `catchments.parquet` (header + 5 rows). |
| [`graph.csv`](./graph.csv) | 183 B | Human-readable dump of `graph.parquet` (header + 5 rows). |

## Embedded schemas

Captured from `python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('examples/tiny/catchments.parquet'))"` (struct fields shown; the `geo` GeoParquet 1.1 covering metadata footer is omitted for brevity):

```text
id: int64 not null
level: int16 not null
parent_id: int64
area_km2: float not null
up_area_km2: float
outlet_lon: double not null
outlet_lat: double not null
bbox: struct<xmin: float not null, ymin: float not null, xmax: float not null, ymax: float not null> not null
  child 0, xmin: float not null
  child 1, ymin: float not null
  child 2, xmax: float not null
  child 3, ymax: float not null
geometry: binary not null
source_id: string
level_label: string
```

And the same for `graph.parquet`:

```text
id: int64 not null
level: int16 not null
upstream_ids: list<element: int64> not null
  child 0, element: int64
bbox_minx: float not null
bbox_miny: float not null
bbox_maxx: float not null
bbox_maxy: float not null
```

## CSV dumps

The CSVs render every row of the parquet files with these choices:

- **Geometry**: WKB → WKT via shapely — WKT chosen over hex for readability.
- **`upstream_ids`** (`list<int64>`): compact JSON-style string, e.g. `[]` or `[2,3]`.
- **Nulls**: nullable columns render as empty cells — here that is `parent_id` on the root unit and `up_area_km2` throughout (`has_up_area: false`). `source_id` and `level_label` are populated on every row in this fixture (`src-1`…`src-5`, `coarse`/`fine`).
- **Column order**: parquet schema order.
- **Line endings**: LF.

Regeneration: the dumps are generated from the parquet files in this directory with pyarrow + shapely, written with LF line endings via csv `lineterminator="\n"`.

## Relationship to `conformance/`

The fixtures under [`../../conformance`](../../conformance) are validator and interoperability fixtures, kept — in that directory's own words — "focused on contract coverage rather than realism or dataset size". `examples/` exists for inspectability instead: a worked artifact an implementer can open by hand, with schemas and dumps inline.

## Validating

From the repo root:

```sh
cargo run -p hfx-cli -- examples/tiny --strict
```

Expected output ends with `Result: VALID` (exit 0). With the binary installed, the equivalent is:

```sh
hfx examples/tiny --strict
```
