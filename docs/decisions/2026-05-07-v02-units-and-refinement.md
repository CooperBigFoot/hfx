# HFX v0.2 Units and Refinement Direction

**Status:** Accepted

**Date:** 2026-05-07

**Follow-up:** Validator implementation is tracked in the v0.2 validator PR
after the spec and JSON Schemas are frozen.

## Decision

HFX v0.2 should move from the v0.1 "catchment atom" model to a multi-level
drainage-unit model. HFX should describe the normalized hydrofabric data and
its optional auxiliary artifacts, but it should not prescribe a delineation
algorithm, unit-selection strategy, refinement strategy, engine return type, or
runtime composition across datasets.

The core HFX dataset should remain small, stable, and cloud-optimized:

- `manifest.json`
- `catchments.parquet`
- `graph.parquet`
- `snap.parquet` when snap features are available

The v0.2 core is therefore all Parquet plus a JSON manifest. A single columnar
format keeps the exchange contract simpler and enables predicate pushdown,
range reads, and consistent cloud access patterns across the vector files.

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

The graph file should be Parquet rather than Arrow IPC. Arrow IPC's main
advantage was mmap-friendly local loading, but object-store reads rely on HTTP
range requests instead. Engines usually reload graph data into runtime-specific
representations such as CSR arrays or hash maps, so an IPC mmap layout is not
the durable contract that matters most. Parquet keeps the core format uniform
and gives cloud readers useful row-group statistics.

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

Levels are dataset-local. A `level` value is meaningful within one HFX dataset
and has no cross-dataset meaning. Engines consuming multiple datasets must not
assume level numbers align.

Adapters may include an optional `level_label` column on `catchments.parquet`
to preserve source-fabric semantics such as `huc8`, `pfaf-l4`, or
`grit-segment`. The label is not normalized, not validated beyond type, and not
comparable across datasets. Engines must not parse it for behavior.

### Parent relationship

Cross-level hierarchy should be explicit, not encoded only in IDs. A unit may
reference its containing coarser unit with `parent_id`.

HUC-style nested identifiers are useful as optional display or source metadata,
but engines and validators should not depend on parsing an encoded ID prefix.
An explicit parent relationship avoids assumptions such as "at most 99 children
per parent" and works for global datasets.

The parent relation forms a forest. It is acyclic, and following `parent_id`
from any unit reaches a root unit with null `parent_id` in finitely many steps.
`parent_id` references the next coarser available unit. If intermediate source
levels are absent from the HFX dataset, it skips to the next available coarser
unit.

Perfect nesting is required within one HFX dataset. For any unit `U`, the union
of all finer units whose `parent_id` chain terminates at `U` equals `U`'s
geometry within tolerance, and siblings have disjoint interiors. Fabrics that
do not nest cleanly should be shipped as separate HFX datasets.

### Unit outlet

Every drainage unit should declare exactly one outlet coordinate. The graph
describes connectivity, and the polygon describes area, but neither alone
identifies where water exits the unit.

The single-outlet rule holds even when same-level graph topology is a DAG. A
unit with multiple downstream graph neighbors still has one physical outlet;
the fan-out happens at that outlet through multiple graph edges. A polygon with
multiple physical exit points should be represented as multiple drainage units.

The outlet is useful for snapping, refinement, visualization, cross-level
transitions, and cross-dataset alignment.

Outlet validation should be characterized by topology role:

- Flow-through unit: the outlet lies, within tolerance, on the boundary segment
  shared with the downstream same-level neighbor.
- Terminal sink: the outlet lies inside the unit polygon for interior drainage
  or lakes, or on the dataset boundary for a coastal terminus.
- DAG fork unit: the outlet lies on the boundary at the fan-out node.

Tolerance is therefore a validation parameter for checking the topology role,
not the primary semantic contract.

## Proposed Core Schemas

### `catchments.parquet`

One row per drainage unit.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unique unit ID within this dataset |
| `level` | `int16` | No | Dataset-local resolution tier for this unit |
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

The `bbox_*` columns are intentionally explicit even though they are derivable
from geometry. Combined with spatial row-group sorting, Parquet column
statistics on these fields enable predicate pushdown for partial-region reads.
WKB binary min/max statistics do not provide meaningful spatial filtering.

Allowed optional columns:

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `source_id` | `string` | Yes | Source-fabric identifier for audit and traceability |
| `level_label` | `string` | Yes | Source-fabric label for this resolution tier |

If `source_id` is present, the manifest's `fabric_name` and `fabric_version`
qualify the ID space. It is not required and not part of the normalized HFX
identity model.

Candidate invariants:

- `id` is unique across the dataset.
- `level >= 0`.
- `parent_id` is null for coarsest units.
- Non-null `parent_id` references an existing unit at the next coarser available
  level.
- The parent relation is an acyclic forest.
- Finer units perfectly nest within their parent chain, within tolerance.
- Sibling units have disjoint interiors, within tolerance.
- `up_area_km2`, when present, is defined per level as the inclusive sum of
  same-level upstream units. It is deliberately non-comparable across levels.
- `outlet_lon` and `outlet_lat` are finite valid EPSG:4326 coordinates.
- The outlet satisfies the topology-role rule for the unit.

### `graph.parquet`

The graph remains separate from `catchments.parquet`.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unit ID, foreign key to `catchments.parquet` |
| `level` | `int16` | No | Resolution tier for this graph row |
| `upstream_ids` | `list<int64>` | No | Direct upstream unit IDs at the same level |

Candidate invariants:

- Every unit row has exactly one graph row.
- Every graph row references an existing unit.
- `level` matches the referenced unit's level.
- `upstream_ids` only reference existing units at the same level.
- Graph edges represent same-level hydrologic connectivity.
- Cross-level relationships are represented by `parent_id`, not by graph edges.
- Graphs are acyclic within each level.

### `snap.parquet`

Snap features remain optional and separate. They are not a replacement for unit
outlets. The default snap feature layer is the river network, represented as
reaches. Producers without a river network may still ship point-style targets
such as gauges or named outlets.

Allowed geometry types are Point and LineString.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unique snap feature ID within this dataset |
| `unit_id` | `int64` | No | Referenced drainage unit ID |
| `geometry` | `binary` WKB | No | Point or LineString, EPSG:4326 |
| `weight` | `float32` | No | Producer-defined preference for snapping |
| `stem_role` | `string` | Yes | One of `mainstem`, `tributary`, or `unknown` when known |
| `bbox_minx` | `float32` | Yes | Optional bounding box west |
| `bbox_miny` | `float32` | Yes | Optional bounding box south |
| `bbox_maxx` | `float32` | Yes | Optional bounding box east |
| `bbox_maxy` | `float32` | Yes | Optional bounding box north |

`snap.parquet` does not store a `level` column. The level is derived from the
referenced unit, which avoids drift and is cheap to validate by joining against
`catchments.parquet`.

Snap features also do not encode internal snap-to-snap connectivity. Reach-level
routing, hydraulic models, and stream-order computation are separate
applications. Producers that need those relationships should ship them as
auxiliary data.

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
      "artifacts": {
        "flow_dir": "flow_dir.tif",
        "flow_acc": "flow_acc.tif"
      },
      "metadata": {
        "flow_dir_encoding": "esri"
      }
    },
    {
      "schema": "org.example.custom_refinement_inputs.v1",
      "artifacts": {
        "index": "custom/index.parquet"
      },
      "metadata": {}
    }
  ]
}
```

The `schema` field describes the data layout and semantics. It does not name
or require executable refiner code. Auxiliary entries should not include a
free-string `role` field; known schemas already imply their role, and unknown
roles provide little machine-checkable signal.

Auxiliary schema namespaces:

- `hfx.aux.*`: blessed schemas defined by HFX in `spec/aux/<name>/v<N>.md`,
  with companion JSON Schema for metadata. Validators must implement these
  schemas as stable surface area.
- `hfx.x.*`: provisional schemas in the HFX spec repo. Validators may implement
  them, but engines should not depend on them as stable contracts.
- `<reverse-dns>.*`: third-party schemas outside the HFX spec repo. Validators
  perform structural checks only, such as file presence and well-formed
  metadata JSON.

Auxiliary schemas use explicit `vN` breaking versions. Within one `vN`, changes
must be additive only, such as new optional metadata fields or new optional
artifacts. Renaming, removing, or retyping a field requires `v(N+1)`.
Validators dispatch on exact `vN` matches, and multiple versions may coexist.

For v0.2, HFX should bless exactly one auxiliary schema:
`hfx.aux.d8_raster.v1`. It replaces v0.1's first-class raster artifact with two
files: `flow_dir` and `flow_acc`. Two files match v0.1, independent COG access
patterns, and MERIT Hydro source layout. Other candidates such as DEM,
provenance, and river-network attributes should be blessed only after concrete
demand from multiple consumers.

Validation policy:

- The core validator validates the core files and manifest structure.
- Known `hfx.aux.*` schemas have dedicated validators.
- Known `hfx.x.*` schemas may have dedicated validators.
- Unknown third-party auxiliary schemas are allowed.
- Unknown auxiliary schemas receive only structural validation.

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
- optional snap features
- auxiliary artifact declarations
- validation of declared HFX data

## Cross-Dataset Use

Cross-HFX composition is entirely a runtime concern. Different fabrics produce
different units with different boundaries and disjoint ID spaces, so there is
no general data-spec answer for composing them.

Each HFX dataset describes one self-consistent fabric. A dataset manifest should
not need to know every other dataset it may be combined with at runtime. Engines
may still combine datasets geometrically, such as using GRIT drainage units as
the primary hydrofabric and MERIT-derived rasters as refinement input.

## Consequences

This direction simplifies the core exchange contract while making HFX more
flexible for multi-resolution, custom-refinement, and cross-dataset workflows.
It requires a larger v0.2 schema change than simply adding fields to v0.1, but
it draws a clearer boundary between HFX as a data standard and `shed` or other
engines as runtime implementations.
