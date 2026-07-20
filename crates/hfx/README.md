# hfx

Shared types and validation primitives for HFX artifacts.

## Purpose

`hfx` defines the canonical in-memory representation of the [HFX specification](../../spec/HFX_SPEC.md). Every type enforces its invariants at construction time (parse, don't validate): a `UnitId` is always positive, a `BoundingBox` is always non-degenerate, a `Manifest` always names a real fabric. Invalid states are unrepresentable.

The crate has no I/O dependencies. Deserialization from Parquet, Arrow, JSON, and GeoTIFF is the responsibility of downstream crates (`hfx-cli`, source adapters, the delineation engine).

## Architecture

- Primitive modules:
  - `id.rs`: `UnitId`, `SnapId`, `IdError`
  - `area.rs`: `AreaKm2`, `Weight`, `MeasureError`
  - `geo.rs`: `Longitude`, `Latitude`, `BoundingBox`, `WkbGeometry`, `GeoError`
  - `raster.rs`: `EpsgCode`, `FlowDirEncoding`, `FlowAccumulationUnits`, `D8RasterMetadataV2`, and their parse errors
- Composite modules:
  - `catchment.rs`: `CatchmentUnit`
  - `snap.rs`: `SnapTarget`, `StemRole`
  - `graph.rs`: `AdjacencyRow`, `DrainageGraph`, `GraphError`
  - `manifest.rs`: `Manifest`, `ManifestBuilder`, related enums and errors
- `lib.rs` re-exports the public surface and defines shared helper traits such as `HasBbox` and `HasUnitId`.

## Glossary

| Term | Meaning |
|---|---|
| Catchment atom | Smallest indivisible drainage unit in an HFX dataset; one row of `catchments.parquet` |
| Snap target | A candidate point or linestring reach to which a pour point may be snapped; one row of `snap.parquet` |
| Adjacency row | One node in the upstream drainage graph — an atom ID plus the IDs of its direct upstream neighbours |
| Drainage graph | Complete upstream adjacency over all atoms; in-memory representation of `graph.parquet` |
| Manifest | Dataset metadata (`manifest.json`): format version, CRS, topology, artifact availability, bounding box |
| Mainstem | Primary channel in a drainage network, as opposed to tributaries or distributaries |
| WKB | Well-Known Binary — OGC binary encoding for geometry; treated as an opaque byte buffer by this crate |
| D8 | Single-flow-direction model where each raster cell drains to exactly one of its eight neighbours |
| Fabric level | Optional hierarchical subdivision level within the source fabric (e.g., resolution tier, nesting depth) |
| Terminal sink | The virtual outlet for an entire dataset; ID value `0` is reserved as its sentinel |

## Key Types

| Type | Module | Role |
|---|---|---|
| `UnitId` | `id` | Strictly-positive `i64` identifier for a catchment unit; distinct from `SnapId` to prevent accidental mixing |
| `SnapId` | `id` | Strictly-positive `i64` identifier for a snap target |
| `AreaKm2` | `area` | Finite, non-negative `f32` area in km² |
| `Weight` | `area` | Finite, non-negative `f32` snap ranking weight. Higher values MUST indicate greater hydrological dominance (adapters typically write upstream drainage area in km² or cell count). |
| `BoundingBox` | `geo` | Axis-aligned WGS84 bbox; enforces `min < max` on both axes at construction |
| `WkbGeometry` | `geo` | Non-empty WKB byte buffer; geometry parsing is delegated to callers |
| `CatchmentUnit` | `catchment` | One row of `catchments.parquet` — id, local area, optional upstream area, bbox, geometry |
| `SnapTarget` | `snap` | One row of `snap.parquet` — id, catchment FK, weight, `StemRole`, bbox, geometry |
| `StemRole` | `snap` | Enum (`Mainstem` / `Tributary` / `Distributary` / `Unknown`) — replaces a `bool` flag so call sites are self-documenting |
| `AdjacencyRow` | `graph` | One node in the adjacency graph — atom ID and its upstream neighbour IDs |
| `DrainageGraph` | `graph` | HashMap-indexed adjacency over all units; O(1) lookup by `UnitId`. Optimised for validation, not traversal — engines are expected to convert to CSR at load time |
| `EpsgCode` | `raster` | Validated uppercase EPSG authority string for a raster CRS |
| `FlowDirEncoding` | `raster` | D8 convention enum (`Esri` / `Taudem` / `Grass`) |
| `FlowAccumulationUnits` | `raster` | Flow-accumulation units enum (`Cells` / `Km2`) |
| `D8RasterMetadataV2` | `raster` | Typed parsing boundary for required D8 raster v2 metadata |
| `Manifest` | `manifest` | Parsed `manifest.json`; constructed exclusively via `ManifestBuilder` |
| `ManifestBuilder` | `manifest` | Builder for `Manifest` — required fields validated in `new()`, optional fields set via chainable `with_*` methods |
| `HasBbox` | `lib` | Trait for generic spatial filtering over any artifact row type |
| `HasUnitId` | `lib` | Trait for generic operations over any unit-identified row type |
