# Glossary

### Adapter

An adapter is a compiler that converts one source hydrofabric into an HFX dataset offline.

### Auxiliary data

Auxiliary data is optional extra artifacts a dataset may declare in its manifest, such as D8 flow-direction rasters or snap features.
Core required files are `catchments.parquet`, `graph.parquet`, and `manifest.json`.

### Bounding box

A bounding box is the rectangular coordinate extent that encloses a unit's geometry.
HFX stores bounding boxes to support spatial filtering and validation.

### `catchments.parquet`

`catchments.parquet` is the required HFX file with one row per unit catchment.
It carries each unit's polygon, level, parent, outlet, and bounding box.

### Conformance

Conformance is the property of an HFX dataset that passes every validation class the specification defines.

### Consumer

A consumer is any program that reads an HFX dataset.
A delineation engine is a consumer that traces drainage over the HFX graph.

### D8 flow-direction raster

A D8 flow-direction raster is an auxiliary grid where each cell points flow toward one of its eight neighboring cells.
HFX can declare D8 rasters as auxiliary data.

### DAG

A DAG is a directed acyclic graph, a graph whose edges have direction and that contains no cycles.
In HFX, DAG topology lets a unit send flow to more than one downstream unit.

### Delineation engine

A delineation engine is a consumer that traces drainage over the HFX graph.

### Drainage graph

A drainage graph is the same-level network recording which unit catchment flows into which.
It is stored in `graph.parquet` and carries no geometry.

### Drainage unit

A drainage unit is the specification's name for one unit catchment.
Each row of `catchments.parquet` is one drainage unit.

### Flow accumulation

Flow accumulation is a measure at a location of how much upstream area drains through it.
It is typically the count of upstream cells contributing flow to that cell.

### GeoParquet

GeoParquet is a Parquet-based columnar format for geospatial vector data.
HFX uses it in `catchments.parquet` for geometry and bounding-box covering metadata.

### `graph.parquet`

`graph.parquet` is the required HFX file holding the same-level drainage graph as upstream adjacency lists.
It carries no geometry.

### HFX

HFX is HydroFabric Exchange, the open specification and Rust toolkit for a compiled drainage format.
Consumers read HFX in place of any source fabric.

### Hydrofabric

A hydrofabric is a dataset describing a region's river network and the land that drains into it.

### Level

A level is a dataset-local resolution tier for unit catchments.
`level = 0` is the recommended coarsest tier, and higher values are finer.

### Manifest

A manifest is the `manifest.json` sidecar file that declares dataset identity, topology class, row counts, and auxiliary declarations.

### Outlet

An outlet is the single EPSG:4326 coordinate where water exits a unit catchment.

### Parent

A parent is the containing coarser drainage unit named by `parent_id`.
Finer units nest inside parent units.

### Pour point

A pour point is an approximate geographic point of interest supplied to a consumer to mark where a query begins.
A consumer may snap it onto the drainage network before using it.

### Snapping

Snapping is aligning an approximate input coordinate onto the drainage network so a consumer starts from the right unit.
Snap features are optional auxiliary data.

### Snap feature

A snap feature is an optional auxiliary geometry that can help a consumer align an approximate input coordinate onto the drainage network.

### Source fabric

A source fabric is the hydrofabric an adapter reads before it writes an HFX dataset.

### Topology

Topology is the shape of the drainage graph.
In HFX, topology is either `tree` or `dag`, and the manifest declares it.

### Tree topology

Tree topology is a drainage graph shape in which every unit has at most one downstream neighbor.

### Unit catchment

A unit catchment is the land area that drains to a single outlet.
Each row of `catchments.parquet` is one unit catchment, also called a drainage unit.

### Upstream adjacency list

An upstream adjacency list is the list of direct upstream unit IDs stored for one unit in `graph.parquet`.
