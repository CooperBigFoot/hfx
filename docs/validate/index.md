# Validate a dataset

This page shows you how to validate an HFX dataset, a directory holding `manifest.json`, `catchments.parquet`, `graph.parquet`, and any declared auxiliary artifacts.
First, you install or build the validator.
Next, you run it against a dataset and read its report.
Finally, you compare your results with the conformance fixtures, a set of small HFX datasets kept in the repository to exercise the validator.

## Install the validator

The validator lives in `hfx-cli`, a Rust crate that builds the `hfx` binary.
The validator links against GDAL, a geospatial data library, installed on the host, so the host needs GDAL available at build time.

First, install from a checkout when you want the binary on your `PATH`.

```bash
# Build and install the hfx binary from this checkout.
cargo install --path crates/hfx-cli
```

This compiles the `hfx-cli` crate and installs the `hfx` binary into your Cargo bin directory, so you can call `hfx` from anywhere.

Next, build in place when you want to run the binary from this checkout.

```bash
# Compile the hfx-cli crate inside this checkout.
cargo build -p hfx-cli

# Run the freshly built binary from the target directory.
./target/debug/hfx ./path/to/dataset
```

`cargo build -p hfx-cli` produces `./target/debug/hfx`, which you invoke directly by path.
Once `hfx-cli` is published to crates.io, `cargo install hfx-cli` will fetch and install it without a checkout.

## Run it against a dataset

First, pass the path to one HFX dataset directory.

```bash
# Validate one HFX dataset directory.
./target/debug/hfx conformance/valid/tiny
```

You pass the path to an HFX dataset directory, and the validator checks it in a single pass.

Next, choose the report format with `--format text|json`.
The default is `text`.
Use `--format json` for machine-readable output in CI pipelines, a continuous integration setup that runs checks automatically.

```bash
# Emit a machine-readable report.
./target/debug/hfx --format json conformance/valid/tiny
```

`--format json` prints the same findings as a JSON document that a script can parse.

Finally, add flags when you need stricter or narrower checks.
Use `--strict` to promote warnings to errors, so any warning makes the run fail.
Use `--skip-rasters` to skip the raster checks.
Use `--sample-pct N` to set the geometry spot-check sample percentage.
The default sample percentage is `1.0`.

## Read the report

Each finding prints on its own line in this shape.

```text
[SEVERITY] <artifact> (<check_id>): <message>
```

`SEVERITY` is one of `ERROR`, `WARN`, or `INFO`.
`<artifact>` names the file the finding is about.
`<check_id>` is the stable identifier for the rule that fired, for example `manifest.crs`.
`<message>` explains the finding.

After the findings, the report prints a summary line and a verdict.

```text
N error(s), M warning(s), K info(s)
Result: VALID
```

`Result: VALID` prints when the dataset has no error-severity findings.
`Result: INVALID` prints when at least one error-severity finding exists.
The exit code is `0` when the dataset is valid and `1` when it is invalid, so scripts can branch on the exit code.
Under `--strict`, warnings become errors, so a dataset with warnings reports `INVALID` and exits `1`.

## What the conformance fixtures show

The conformance fixtures are small HFX datasets kept in the repository to exercise the validator.
`conformance/valid/` holds datasets expected to pass.
Each subdirectory carries a `README.md` naming its expected diagnostic as `none`.
The valid subdirectories are `tiny`, `grit-two-level`, `grit-two-snap`, `tiny-with-aux-d8`, `tiny-with-two-aux-d8`, `tiny-with-two-aux-d8-tiled`, and `up-area-partial-nulls`.
`conformance/invalid/` holds intentionally broken datasets, each designed to trigger one specific failure mode.
Each invalid subdirectory carries a `README.md` naming the single expected diagnostic.
For example, `crs-mismatch` expects `manifest.crs`.
Example invalid subdirectories include `crs-mismatch`, `dangling-upstream-ref`, `parent-cycle`, `aux-snap-weight-negative`, `catchments-multi-level-unsorted`, `covering-metadata-missing`, and `legacy-format-version`.

First, run a valid fixture.
Next, run an invalid fixture.

```bash
# A valid fixture reports VALID and exits 0.
./target/debug/hfx conformance/valid/tiny

# An invalid fixture reports INVALID and exits 1.
./target/debug/hfx conformance/invalid/crs-mismatch
```

The valid fixtures show what a conformant dataset looks like, and each invalid fixture isolates one broken rule so you can see the matching diagnostic and the `INVALID` verdict.
Finally, read each fixture's `README.md` for its expected diagnostic.
