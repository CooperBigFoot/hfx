# HFX - HydroFabric Exchange Specification

**Version 0.2.1**

---

## Overview

HFX is the canonical data contract consumed by watershed delineation engines.
It is not a native hydrofabric format. Every source fabric MUST be compiled into
HFX by an adapter before an engine sees it.

The engine operates on HFX exclusively. It contains no fabric-specific logic.
HFX specifies normalized drainage-unit data, same-level topology, optional snap
features, and manifest-declared auxiliary artifacts. It does not specify a
delineation algorithm, level-selection strategy, refinement strategy, engine
return type, or runtime composition across datasets.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

### Terminology

A **drainage unit** is one polygonal hydrologic unit at one dataset-local
resolution level. Every row in `catchments.parquet` is a drainage unit.

A **level** identifies a resolution tier within one HFX dataset. Level values
are dataset-local and have no cross-dataset meaning. `level = 0` is the
recommended coarsest tier; higher values SHOULD represent progressively finer
tiers.

A **parent** is the containing coarser drainage unit named by `parent_id`.
Following `parent_id` from any unit MUST reach a root unit with null
`parent_id` in finitely many steps.

A **unit outlet** is the single EPSG:4326 coordinate where water exits the
drainage unit. The outlet is distinct from optional snap features.

---

## Artifact Summary

| Artifact | Required | Purpose |
|---|---:|---|
| `catchments.parquet` | Yes | Drainage-unit polygons, levels, parents, outlets |
| `graph.parquet` | Yes | Same-level upstream adjacency graph |
| `manifest.json` | Yes | Dataset metadata and auxiliary declarations |

Auxiliary artifacts such as D8 rasters are not first-class core files in v0.2.
They are declared in `manifest.json` under `auxiliary[]` and validated according
to their schema. HFX v0.2 blesses one auxiliary schema:
[`hfx.aux.d8_raster.v1`](./aux/d8_raster/v1.md). HFX v0.2.1 also blesses
optional snap features through [`hfx.aux.snap.v1`](./aux/snap/v1.md).

---

## 1. `catchments.parquet`

One row per drainage unit.

### Schema

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unique unit ID within this dataset |
| `level` | `int16` | No | Dataset-local resolution tier |
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

Allowed optional columns:

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `source_id` | `string` | Yes | Source-fabric identifier for audit and traceability |
| `level_label` | `string` | Yes | Source-fabric label for this tier, such as `huc8` or `pfaf-l4` |

`source_id` is qualified by the manifest's `fabric_name` and
`fabric_version`. It is not part of the normalized HFX identity model.
`level_label` is unnormalized and not validated beyond type; engines MUST NOT
parse it for behavior.

### Spatial Partitioning

- Rows MUST be sorted by Hilbert curve index computed on centroid
  coordinates.
- For datasets with multiple levels, rows MUST be sorted by
  `(level ASC, hilbert_index ASC)`. Single-level datasets keep the Hilbert-only
  rule.
- `hilbert_index` is a sort key, not a stored column; no `hilbert_index` field
  is present on `catchments.parquet`.
- Files with fewer than 4,096 rows MUST contain exactly one row group.
- Files with 4,096 or more rows MUST use row groups of 4,096-8,192 rows.
- Parquet row-group statistics on `bbox_minx`, `bbox_miny`, `bbox_maxx`, and
  `bbox_maxy` MUST be written.

Row-group sizing violations are WARN diagnostics in the reference validator and
are promoted to ERROR under `--strict`.

### Unit Invariants

- `id = 0` is reserved and MUST NOT be used.
- IDs are positive and unique across the dataset.
- `level >= 0`.
- `parent_id` is null for coarsest available units.
- Non-null `parent_id` references an existing unit at a strictly coarser level.
- The parent relation is an acyclic forest.
- Finer units perfectly nest within their parent chain, within validator
  tolerance.
- Sibling units have disjoint interiors, within validator tolerance.
- `outlet_lon` and `outlet_lat` are finite WGS84 coordinates.
- `up_area_km2`, when present, is the inclusive cumulative upstream area for
  same-level graph traversal. It is deliberately non-comparable across levels.
  Producers MAY emit null `up_area_km2` for individual rows where computation
  is not possible (e.g., source-data anomalies preventing the producer's chosen
  accumulation algorithm from converging); the manifest's `has_up_area=true`
  flag claims coverage at the column level, not every-row populated.

For DAG-topology datasets, `up_area_km2` semantics at bifurcations are
producer-defined and MUST be documented in the manifest or an accompanying
README. Producers MUST choose and document one of: (a) area partitioned by flow
physics at bifurcations, (b) geometric union of upstream catchments, or (c)
mainstem-routed single-threaded accumulation. Engines comparing upstream areas
across DAG datasets MUST reconcile these semantics before comparison.

Perfect nesting is required within one HFX dataset. Fabrics whose levels do not
nest cleanly SHOULD be shipped as separate HFX datasets.

---

## 2. `graph.parquet`

The same-level upstream adjacency graph over drainage units. Contains no
geometry. This is what engines traverse during upstream accumulation.

### Schema

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `id` | `int64` | No | Unit ID, foreign key to `catchments.parquet` |
| `level` | `int16` | No | Resolution tier for this graph row |
| `upstream_ids` | `list<int64>` | No | Direct upstream unit IDs at the same level |
| `bbox_minx` | `float32` | No | Bounding box west for the referenced unit |
| `bbox_miny` | `float32` | No | Bounding box south for the referenced unit |
| `bbox_maxx` | `float32` | No | Bounding box east for the referenced unit |
| `bbox_maxy` | `float32` | No | Bounding box north for the referenced unit |

### Spatial Partitioning

- Rows MUST be sorted by Hilbert curve index computed on referenced unit
  centroid coordinates.
- For datasets with multiple levels, rows MUST be sorted by
  `(level ASC, hilbert_index ASC)`. Single-level datasets keep the Hilbert-only
  rule.
- `hilbert_index` is a sort key, not a stored column; no `hilbert_index` field
  is present on `graph.parquet`.
- Files with fewer than 4,096 rows MUST contain exactly one row group.
- Files with 4,096 or more rows MUST use row groups of 4,096-8,192 rows.
- Parquet row-group statistics on `bbox_minx`, `bbox_miny`, `bbox_maxx`, and
  `bbox_maxy` MUST be written.

Row-group sizing violations are WARN diagnostics in the reference validator and
are promoted to ERROR under `--strict`.

### Notes

- Every unit in `catchments.parquet` MUST have exactly one graph row.
- Every graph row ID MUST reference an existing unit.
- `level` MUST match the referenced unit's level.
- `upstream_ids` entries MUST reference existing units at the same level.
- Cross-level relationships are represented by `parent_id`, not graph edges.
- The graph MUST be acyclic within each level.
- For tree-topology fabrics, each unit has at most one downstream neighbor.
- For DAG-topology fabrics, distributaries are represented by one upstream unit
  appearing in multiple downstream rows.

The on-disk `list<int64>` schema is the contract. Engines MAY convert it to CSR
arrays, hash maps, or any other runtime layout after loading.

---

## 3. Snap Features

Snap features are optional auxiliary data declared with
[`hfx.aux.snap.v1`](./aux/snap/v1.md), not a core root-level artifact.

---

## 4. Unit Outlet Rules

Every drainage unit declares exactly one outlet coordinate. The single-outlet
rule holds even when same-level graph topology is a DAG. A unit with multiple
downstream graph neighbors still has one physical outlet; fan-out happens at
that outlet through multiple graph edges. A polygon with multiple physical exit
points SHOULD be represented as multiple drainage units.

Outlet validation is characterized by topology role:

- **Flow-through unit:** the outlet lies, within validator tolerance, on the
  boundary segment shared with the downstream same-level neighbor.
- **Terminal sink:** the outlet lies inside the unit polygon for interior
  drainage or lakes, or on the polygon boundary at the coast -- for regional
  datasets cut at the coastline, this is also the dataset boundary; for global
  datasets, it is the unit polygon's coastal edge.
- **DAG fork unit:** the outlet lies on the boundary at the fan-out node.

The tolerance is a validation parameter, not a stored dataset field.

---

## 5. `manifest.json`

Sidecar metadata file. Declares dataset identity, topology class, row counts,
and optional auxiliary artifacts.

The manifest describes **what the data is**, not how an engine uses it.
Traversal policies and refinement policies are engine runtime parameters.

### Schema Example

```json
{
  "format_version": "0.2.1",
  "fabric_name": "example-fabric",
  "fabric_version": "2026.1",
  "crs": "EPSG:4326",
  "has_up_area": true,
  "topology": "tree",
  "region": "europe",
  "bbox": [-25.0, 34.0, 45.0, 72.0],
  "unit_count": 194356,
  "created_at": "2026-05-14T00:00:00Z",
  "adapter_version": "0.2.0",
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
    }
  ]
}
```

### Field Definitions

| Field | Type | Required | Description |
|---|---|---:|---|
| `format_version` | string | Yes | MUST be `"0.2.1"` |
| `fabric_name` | string | Yes | Source fabric identifier. Lowercase ASCII, no whitespace |
| `fabric_version` | string | No | Version of the source fabric |
| `crs` | string | Yes | MUST be `"EPSG:4326"` in HFX v0.2 |
| `has_up_area` | bool | Yes | Whether `up_area_km2` is computed and populated. When true, the column carries computed values for rows where computation is possible; nulls are permitted for rows where computation is not possible. When false, all values are null. |
| `topology` | string | Yes | `"tree"` or `"dag"` |
| `region` | string | No | Geographic or source-fabric subset label |
| `bbox` | float[4] | Yes | `[minx, miny, maxx, maxy]` covering all units |
| `unit_count` | int | Yes | Row count of `catchments.parquet` |
| `created_at` | string | Yes | RFC 3339 timestamp of ETL run |
| `adapter_version` | string | Yes | Version of the adapter that produced this dataset |
| `auxiliary` | array | No | Optional declared auxiliary data |

The following v0.1 manifest fields are removed in v0.2:
`has_rasters`, `has_snap`, `fabric_level`, `flow_dir_encoding`,
`terminal_sink_id`, and `atom_count`.

### Auxiliary Declarations

Each `auxiliary[]` entry has this shape:

| Field | Type | Required | Description |
|---|---|---:|---|
| `schema` | string | Yes | Auxiliary schema ID |
| `artifacts` | object | Yes | Non-empty mapping from artifact key to relative path |
| `metadata` | object | Yes | Schema-specific metadata block |

Auxiliary artifact paths are relative to the dataset root. Absolute paths and
paths that escape the dataset root are non-conformant.

Auxiliary schema namespaces:

- `hfx.aux.*`: blessed schemas defined by HFX in `spec/aux/<name>/v<N>.md`,
  with companion JSON Schema for metadata. Validators MUST implement these
  schemas as stable surface area.
- `hfx.x.*`: provisional schemas in the HFX spec repo. Validators MAY implement
  them, but engines SHOULD NOT depend on them as stable contracts.
- `<reverse-dns>.*`: third-party schemas outside the HFX spec repo. Validators
  perform structural checks only, such as file presence and well-formed
  metadata JSON.

Auxiliary schemas use explicit `vN` breaking versions. Within one `vN`, changes
MUST be additive only. Renaming, removing, or retyping a field requires
`v(N+1)`.

---

## Deployment Patterns

An HFX dataset is the artifact bundle described by a single `manifest.json`.
Every dataset covers a contiguous extent of its source fabric. That extent is
either the fabric in full or a named subset. Both deployment patterns are
conformant with v0.2 and use the same artifact schemas.

### Global Datasets

A global dataset covers the full extent of its source fabric.

- `region` SHOULD be omitted.
- `bbox` spans the full source-fabric extent. For a planetary dataset this is
  `[-180, -90, 180, 90]`; these boundary values are exact in EPSG:4326 and
  MUST NOT be padded beyond them.
- `fabric_name` and `fabric_version` identify the dataset unambiguously.

### Partial-Fabric Datasets

A partial-fabric dataset covers a named subset of the source fabric.

- `region` SHOULD be populated with a free-form label such as `"europe"`,
  `"pfaf27"`, or `"conus"`.
- `bbox` spans the subset extent.
- The subset MUST be closed under upstream traversal for each same-level
  graph: every `upstream_ids` entry MUST resolve to a unit present in the same
  dataset.

HFX does not prescribe a controlled vocabulary for `region`.

---

## Validation

A conformant HFX v0.2.1 dataset MUST pass the following validation classes.

### File Presence

- `manifest.json`, `catchments.parquet`, and `graph.parquet` are required.
- Auxiliary artifacts are required only when referenced by `manifest.json`.
- `graph.arrow` is a legacy v0.1 artifact and is not valid in v0.2.
- `snap.parquet` at the dataset root is a legacy v0.2 artifact. Producers MUST
  migrate snap features to `hfx.aux.snap.v1`.

### Manifest

- `format_version` MUST be `"0.2.1"`.
- A v0.2 validator MUST reject v0.1 datasets with a clear unsupported-version
  diagnostic rather than attempting dual-reader compatibility.
- Required fields MUST be present and well-typed.
- `unit_count` MUST equal the row count of `catchments.parquet`.
- `auxiliary[]` entries MUST have parseable schema IDs, non-empty artifact
  keys, relative artifact paths, and object metadata.

### Schema and Values

- Required Parquet columns MUST exist with the specified physical types.
- `id` values MUST be positive and unique.
- `level` values MUST be non-negative.
- `parent_id`, when present, MUST be positive.
- Bounding boxes MUST be finite and ordered.
- Graph bounding boxes MUST be finite and ordered.
- Outlet coordinates MUST be finite and within WGS84 longitude/latitude range.
- `stem_role`, when present, MUST be `mainstem`, `tributary`,
  `distributary`, or `unknown`.
- `weight` values MUST be finite and non-negative; the "monotonically
  increasing in drainage dominance" phrase describes the semantic intent of the
  column, not a structural constraint enforced by validators.

### Referential Integrity

- All graph IDs exist in `catchments.parquet`.
- All graph `upstream_ids` entries exist in `catchments.parquet`.
- Every catchment unit has exactly one graph row.
- All graph edges are same-level edges.
- All `parent_id` values reference existing units at strictly coarser levels.
- All snap `unit_id` values reference existing units.

### Graph, Parent, and Geometry Invariants

- Same-level graphs are acyclic.
- The parent relation is an acyclic forest.
- Finer units MUST nest within parent units within validator tolerance.
- Sibling units MUST have disjoint interiors within validator tolerance.
- Unit outlets MUST satisfy their topology-role rule within validator tolerance.
- Geometry samples are structurally valid WKB Polygon or MultiPolygon values.
- Snap geometries are WKB Point or LineString values.

### Reference Validator Coverage

The v0.2 reference validator enforces the manifest, schema, ID/value,
referential-integrity, parent-forest, same-level graph, WKB geometry, snap, and
auxiliary checks listed above. Expensive geometry-topology checks are
conformance requirements but MAY be implemented incrementally by validators:
perfect nesting, sibling interior disjointness, and outlet topology-role
position checks are reserved for follow-up validator releases. Producers SHOULD
still enforce these invariants during ETL.

### Auxiliary Validation

- Known `hfx.aux.*` schemas receive dedicated validation.
- Known `hfx.x.*` schemas MAY receive dedicated validation.
- Unknown third-party schemas receive structural validation only.
- `hfx.aux.d8_raster.v1` requires `flow_dir` and `flow_acc` artifacts and a
  valid metadata block as defined in [`spec/aux/d8_raster/v1.md`](./aux/d8_raster/v1.md).
- `hfx.aux.snap.v1` requires one `snap` artifact and a valid metadata block as
  defined in [`spec/aux/snap/v1.md`](./aux/snap/v1.md).

## Version Compatibility

`format_version` follows [SemVer](https://semver.org) semantics. While the
MAJOR version is 0, a MINOR bump signals a breaking change and a PATCH bump
signals a compatible or editorial change. The 0.2.0 → 0.2.1 hard cut predates
this policy (see the [CHANGELOG](./CHANGELOG.md)); that history is recorded
as-is and is not retroactively reclassified.

Examples of compatible changes:

- Adding an OPTIONAL manifest field that readers can ignore without changing
  the interpretation of existing data.
- Adding a new allowed value to an open enum-like field, such as blessing a
  new `hfx.aux.*` auxiliary schema.
- Editorial clarification that does not alter conformance requirements.

Examples of breaking changes:

- Adding a REQUIRED column or manifest field.
- Retyping an existing field or changing the meaning of an existing field
  value.
- Removing a field.

To stay forward compatible with future compatible releases, engine and reader
implementations:

- SHOULD NOT reject manifests containing unknown fields introduced by a newer
  compatible release.
- SHOULD validate the field values they rely on, even where only one value is
  currently allowed.

This guidance describes forward-compatibility behavior for engine and reader
implementations. The pinned 0.2.1 JSON Schema
(`schemas/manifest.schema.json`) sets `additionalProperties: false` and serves
as an exact-match conformance tool for producers targeting 0.2.1.

## Migration from v0.2

HFX v0.2.1 is a hard-cut manifest version. Producers migrating from v0.2 MUST:

- Set `manifest.json::format_version` to `"0.2.1"`.
- Add `bbox_minx`, `bbox_miny`, `bbox_maxx`, and `bbox_maxy` to
  `graph.parquet`.
- Move root-level `snap.parquet` into one or more `hfx.aux.snap.v1`
  declarations.
- Allow `stem_role = "distributary"` where snap features represent a branch
  diverging at a bifurcation.
- Expect validators to enforce the `stem_role` enum; invalid values that were
  previously unchecked are non-conformant in v0.2.1.

---

*This specification is intentionally silent on adapter implementation. Any tool
that produces conformant artifacts from any source fabric is a valid adapter.*
