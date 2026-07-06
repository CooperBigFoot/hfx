# HFX

HFX, HydroFabric Exchange, is an open specification and Rust toolkit for a compiled drainage format, a normalized on-disk representation of a river network prepared ahead of time.
A hydrofabric, a dataset describing a region's river network, can differ from another hydrofabric in structure, naming, and topology rules.
HFX gives a consumer, software that reads an HFX dataset, one normalized contract to read.
An adapter, a tool that compiles a specific source hydrofabric into HFX, produces an HFX dataset before runtime.
A consumer can be a delineation engine, software that traces drainage from a point.

## Where to go next

- [Understanding HFX](understanding/index.md) explains the model and the roles in the exchange.
- [Anatomy of a dataset](anatomy/index.md) shows the parts that make up an HFX dataset.
- [Specification](spec/HFX_SPEC.md) contains the normative format contract.
- [Build an adapter](adapter/index.md) guides adapter authors through source compilation choices.
- [Validate a dataset](validate/index.md) explains how you check an HFX dataset.
- [Reference](reference/crates.md) lists the Rust crates and reference material.
- [About](about/index.md) describes the project scope and governance.
