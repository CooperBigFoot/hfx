# HFX v0.2 Units and Refinement Direction

**Status:** Proposed

**Date:** 2026-05-07

## Decision

HFX v0.2 should move from the v0.1 "catchment atom" model to a multi-level
drainage-unit model. HFX should describe the normalized hydrofabric data and
its optional auxiliary artifacts, but it should not prescribe a delineation
algorithm, unit-selection strategy, refinement strategy, engine return type, or
runtime composition across datasets.

The core HFX dataset should remain small and stable:

- `manifest.json`
- `catchments.parquet`
- `graph.arrow`
- `snap.parquet` when snap targets are available

Raster data and other refinement inputs should move out of the core artifact
set and into manifest-declared auxiliary data. Standard auxiliary schemas can
be defined and validated by HFX, but custom auxiliary schemas must remain
possible.

## Rationale

The word "atom" implies one indivisible base unit. That is too restrictive for
multi-resolution hydrofabrics and for engines that may use different unit
levels in different parts of one delineation. HFX should expose all available
hydrofabric units and their relationships; the engine should decide how to use
them.

Refinement should also be decoupled from one implementation. A D8 flow-direction
and flow-accumulation raster pair is one useful refinement input, but a refiner
may also use finer HFX units, stream-network features, another HFX dataset,
custom rasters, external services, or user-defined auxiliary files.

## Core Concepts

### Drainage unit

A drainage unit is one polygonal hydrologic unit at one resolution level.
Every row in `catchments.parquet` is a drainage unit.

The term "atom" should be removed from the specification vocabulary. Engines
may choose units at any level, or mix levels, but HFX itself does not define
which units are indivisible during computation.

### Level

A level identifies a resolution tier in the dataset. HFX should not prescribe
how engines choose levels during delineation.

Recommended numbering:

- `level = 0` is the coarsest available tier.
- Higher level numbers are progressively finer tiers.

This ordering matches spatial pyramids and allows future finer levels to be
added without renumbering existing coarse tiers.

### Parent relationship

Cross-level hierarchy should be explicit, not encoded only in IDs. A unit may
reference its containing coarser unit with `parent_id`.

HUC-style nested identifiers are useful as optional display or source metadata,
but engines and validators should not depend on parsing an encoded ID prefix.
An explicit parent relationship avoids assumptions such as "at most 99 children
per parent" and works for global datasets.

### Unit outlet

Every drainage unit should declare its outlet coordinate. The graph describes
connectivity, and the polygon describes area, but neither alone identifies
where water exits the unit.

The outlet is useful for snapping, refinement, visualization, cross-level
transitions, and cross-dataset alignment.

## Proposed Core Schemas

### `catchments.parquet`

One row per drainage unit.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unique unit ID within this dataset |
| `level` | `int16` | No | Resolution tier for this unit |
| `parent_id` | `int64` | Yes | Containing unit at the next coarser available level |
| `area_km2` | `float32` | No | Geodesic area of this unit in km2 |
| `up_area_km2` | `float32` | Yes | Inclusive upstream drainage area at this unit's level, if precomputed |
| `outlet_lon` | `float64` | No | Unit outlet longitude in EPSG:4326 |
| `outlet_lat` | `float64` | No | Unit outlet latitude in EPSG:4326 |
| `bbox_minx` | `float32` | No | Bounding box west |
| `bbox_miny` | `float32` | No | Bounding box south |
| `bbox_maxx` | `float32` | No | Bounding box east |
| `bbox_maxy` | `float32` | No | Bounding box north |
| `geometry` | `binary` WKB | No | Polygon or MultiPolygon, EPSG:4326 |

Candidate invariants:

- `id` is unique across the dataset.
- `level >= 0`.
- `parent_id` is null for coarsest units.
- Non-null `parent_id` references an existing unit at a coarser level.
- `outlet_lon` and `outlet_lat` are finite valid EPSG:4326 coordinates.
- The outlet lies inside, on the boundary of, or within tolerance of the unit
  geometry.

### `graph.arrow`

The graph remains separate from `catchments.parquet`.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unit ID, foreign key to `catchments.parquet` |
| `level` | `int16` | No | Resolution tier for this graph row |
| `upstream_ids` | `list<int64>` | No | Direct upstream unit IDs at the same level |

Candidate invariants:

- Every unit row has exactly one graph row.
- Every graph row references an existing unit.
- `upstream_ids` only reference existing units at the same level.
- Graph edges represent same-level hydrologic connectivity.
- Cross-level relationships are represented by `parent_id`, not by graph edges.
- Graphs are acyclic within each level.

### `snap.parquet`

Snap targets remain optional and separate. They are not a replacement for unit
outlets. Snap targets are richer outlet-resolution features such as stream
reaches, network nodes, or other features useful for attaching an arbitrary
query point to the hydrofabric.

`snap.parquet` should reference drainage units by ID. If needed, the target
level can be stored explicitly to make checks and reads cheaper, but the level
is derivable from the referenced unit.

## Auxiliary Data

HFX core should not require raster refinement artifacts. Instead, the manifest
should allow datasets to declare optional auxiliary data by data schema, not by
implementation code.

Example:

```json
{
  "auxiliary": [
    {
      "schema": "hfx.aux.d8_raster.v1",
      "role": "refinement_input",
      "artifacts": {
        "flow_dir": "flow_dir.tif",
        "flow_acc": "flow_acc.tif"
      },
      "metadata": {
        "flow_dir_encoding": "esri"
      }
    },
    {
      "schema": "user.example.custom_refinement_inputs.v1",
      "role": "custom",
      "artifacts": {
        "index": "custom/index.parquet"
      },
      "metadata": {}
    }
  ]
}
```

The `schema` field describes the data layout and semantics. It does not name
or require executable refiner code.

Validation policy:

- The core validator validates the core files and manifest structure.
- Known `hfx.aux.*` schemas can have dedicated validators.
- Unknown custom auxiliary schemas are allowed.
- Unknown auxiliary schemas should receive only light validation, such as JSON
  shape and artifact path sanity checks.

## Engine Boundary

HFX is not a delineation engine. HFX should expose data; engines decide how to
consume it.

Engine responsibilities include:

- outlet resolution
- level selection
- mixed-level traversal strategy
- refinement strategy
- runtime composition across multiple datasets
- result and error types
- geometry assembly and dissolve

HFX responsibilities include:

- file schemas
- unit identity
- unit levels
- parent-child hierarchy
- same-level graph topology
- unit outlets
- optional snap targets
- auxiliary artifact declarations
- validation of declared HFX data

## Cross-Dataset Use

Cross-HFX composition should be a runtime concern. For example, an engine may
use GRIT drainage units as the primary hydrofabric and MERIT-derived rasters as
refinement input.

Each HFX dataset should be self-describing, but a dataset manifest should not
need to know every other dataset it may be combined with at runtime.

## Open Questions

- Should `parent_id` reference only the next coarser available level, or may it
  skip levels when intermediate parents are absent?
- Should `snap.parquet` include a required `level` column, or derive level from
  `catchment_id` / `unit_id`?
- Should `up_area_km2` be defined per level only, or can it reference a finer
  accumulation basis?
- What tolerance should validators use for outlet-on-polygon checks?
- Should standard D8 raster auxiliary data prefer two files or one multi-band
  file?
- Which standard auxiliary schemas should HFX bless first?

## Consequences

This direction simplifies the core exchange contract while making HFX more
flexible for multi-resolution, custom-refinement, and cross-dataset workflows.
It requires a larger v0.2 schema change than simply adding fields to v0.1, but
it draws a clearer boundary between HFX as a data standard and `shed` or other
engines as runtime implementations.
