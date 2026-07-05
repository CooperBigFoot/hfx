# Understanding HFX

## What HFX is

HFX, HydroFabric Exchange, is an open specification and Rust toolkit for a compiled drainage format.
It gives you one normalized contract that a consumer, any program that reads an HFX dataset, can read in place of any source fabric.
An adapter, a compiler for one source hydrofabric, builds an HFX bundle once and offline.
A hydrofabric, a dataset describing a region's river network and the land that drains into it, keeps its source-specific details on the adapter side of that boundary.
First, an adapter reads the source fabric.
Next, the adapter writes the HFX files.
Finally, a consumer reads the HFX bundle through the schemas in the specification.

## The unit catchment

A unit catchment, the land area that drains to a single outlet, is the basic spatial unit in HFX.
A drainage unit, the specification's name for the same land-area unit, appears as one row in `catchments.parquet`.
An outlet, the single point where water leaves a unit catchment, gives each unit one downstream exit point.
The unit polygon tells you the land area.
The outlet tells you where that land area drains out.

## Levels

A level, a resolution tier that says how finely the region is subdivided, belongs to one dataset.
Level values have dataset-local meaning.
`level = 0` is the recommended coarsest tier.
Higher values represent finer tiers.
A finer unit sits inside a coarser parent unit.
This nesting lets you reason about the same region at more than one resolution.

## The drainage graph

A drainage graph, a same-level network recording which unit flows into which, connects unit catchments within one level.
It carries no geometry.
The polygons live in `catchments.parquet`.
The graph records the drainage relationships.
First, a consumer chooses the unit that contains or receives the starting point.
Next, the consumer walks the graph upstream from that unit.
Finally, the consumer gathers the same-level units that drain through the chosen unit.

## Tree topology and DAG topology

Topology, the shape of the drainage graph, is declared by the dataset.
Tree topology, a graph shape where every unit has at most one downstream neighbor, matches single-path downstream flow at a level.
DAG topology, a graph shape based on a DAG, a directed acyclic graph, allows one unit to send flow to more than one downstream unit.
Those splits represent distributaries.
A DAG still has no cycles, so graph traversal always moves through a finite drainage network.

## Snapping

Snapping, aligning an approximate input coordinate onto the drainage network, helps a consumer start from the right place.
A pour point, an approximate geographic point of interest, may fall near the drainage network rather than exactly on the unit a consumer should use.
HFX keeps snapping engine-agnostic.
Snap features can appear as auxiliary data.
They are optional extras declared by the dataset.

## The HFX artifacts

An HFX bundle has three required files plus optional extras.
`catchments.parquet`, the file holding one row per unit catchment with its polygon, level, parent, and outlet, carries the spatial units.
`graph.parquet`, the file holding the same-level drainage graph with no geometry, carries the upstream adjacency lists.
`manifest.json` is the manifest, a sidecar file describing what the dataset is.
The manifest declares dataset identity, topology class, row counts, and auxiliary declarations.
Auxiliary data, optional extra artifacts a dataset may declare, can include D8 flow-direction rasters or snap features.
The exact schemas live in the [HFX specification](../spec/HFX_SPEC.md), and one-line definitions live in the [Glossary](glossary.md).
