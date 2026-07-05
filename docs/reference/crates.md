# Rust crates

HFX ships as two Rust crates with separate responsibilities.
Use `hfx` when you need the library types in another Rust project.
Use `hfx-cli` when you need the installable `hfx` validation command.

## `hfx`

The `hfx` crate is the minimal library crate that other Rust projects depend on.
It defines the canonical in-memory types for the HFX format.
Those types include dataset identifiers, bounding boxes, and the parsed `manifest.json` model.
Each type enforces its invariants at construction time, so invalid states are unrepresentable.

The library keeps its dependency surface small.
It depends only on `thiserror` for typed errors and `tracing` for structured diagnostics.
It has no I/O dependencies.
It does not require GDAL, a geospatial data library for reading raster and vector formats.

The crate lives at `crates/hfx` in the repository.
You can inspect the current source at [crates/hfx](https://github.com/CooperBigFoot/hfx/tree/main/crates/hfx).

Once the crate is published to crates.io, add it to another Rust project with:

```bash
# add the HFX library crate after publication
cargo add hfx
```

This adds the `hfx` library crate to your project dependencies.

Before publication, depend on the repository checkout instead.
Use a git dependency that points at the repository, or use a path dependency that points at `crates/hfx`.

## `hfx-cli`

The `hfx-cli` crate is the installable tool crate.
It validates an HFX dataset directory against the HFX specification.
It builds the command-line binary named `hfx`.

The tool depends on the `hfx` library and a larger validation stack.
That stack includes `clap`, `serde`, `serde_json`, `arrow`, `parquet`, `tiff`, `geozero`, `rand`, `tracing`, `tracing-subscriber`, `thiserror`, and `gdal`.
It requires GDAL at build time and at run time.
The validator reads raster coordinate reference system metadata and raster extent metadata through GDAL.

The crate lives at `crates/hfx-cli` in the repository.
You can inspect the current source at [crates/hfx-cli](https://github.com/CooperBigFoot/hfx/tree/main/crates/hfx-cli).

Install the validator from a repository checkout with:

```bash
# install the hfx validator binary from a repository checkout
cargo install --path crates/hfx-cli
```

This puts the `hfx` binary on your `PATH`.

Run the installed binary against a dataset directory with:

```bash
# validate a dataset directory; exit code 0 means valid, 1 means invalid
hfx ./path/to/dataset --strict
```

The command validates the dataset directory and exits with a status code you can use in scripts.

## API documentation

The repository at [CooperBigFoot/hfx](https://github.com/CooperBigFoot/hfx) is the primary source reference today.
Use the in-repository crate paths `crates/hfx` and `crates/hfx-cli` for the current API surface.
Hosted API documentation on docs.rs becomes available once the crates are published to crates.io under the `hfx` and `hfx-cli` names.
