# HFX

HFX (HydroFabric Exchange) is an open specification and Rust toolkit for a compiled drainage format that lets watershed delineation engines consume any source hydrofabric, a dataset describing a region's river network, through one normalized contract.
Adapters compile source-specific hydrofabrics such as HydroBASINS, GRIT, or MERIT Hydro into HFX once, offline.
Engines then consume HFX artifacts through the same documented format.

For the full documentation, use the hosted site: <https://cooperbigfoot.github.io/hfx/>.
The site covers [Understanding HFX](https://cooperbigfoot.github.io/hfx/understanding/), dataset anatomy, the specification, adapter development, validation, and reference material.

## Source of truth

The primary normative artifact is the development specification at [spec/HFX_SPEC.md](./spec/HFX_SPEC.md).

Supporting public interfaces live alongside it:

- [schemas/manifest.schema.json](./schemas/manifest.schema.json) defines the manifest contract in machine-readable form.
- [examples/](./examples) holds reference datasets for implementers.
- [conformance/](./conformance) holds validator fixtures and intentionally invalid datasets.

The validator and adapters serve the specification.

## Validator CLI

The validator lives in the `hfx-cli` crate and installs the `hfx` binary:

```bash
cargo install --path crates/hfx-cli
hfx ./path/to/dataset
```

For machine-readable output:

```bash
hfx --format json ./path/to/dataset
```

- `--strict` promotes warnings to errors.
- `--skip-rasters` skips `flow_dir.tif` and `flow_acc.tif` checks.
- Exit code `0` means the dataset is valid; exit code `1` means it is invalid.

Validation behavior is defined against [spec/HFX_SPEC.md](./spec/HFX_SPEC.md).

## Datasets

One reference dataset is hosted on Cloudflare R2 and readable directly over HTTPS via range requests for partial reads.
[Upstream Tech](https://upstream.tech/) sponsors the hosting infrastructure as infrastructure sponsor only; the source data authors are credited below.
More dataset detail lives on the [Datasets page](https://cooperbigfoot.github.io/hfx/reference/datasets/).

### GRIT 2.0.0 HFX

An HFX compilation of the Global River Topology (GRIT) vector datasets, with segment (`level=0`) and reach (`level=1`) drainage units.

Manifest: <https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/manifest.json>

| Property | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | grit |
| `fabric_version` | 1.0.0 |
| `adapter_version` | grit-global-2.0.0 |
| `unit_count` | 22,337,300 |
| `topology` | dag |
| CRS | EPSG:4326 |
| Auxiliaries | two `hfx.aux.snap.v2` snap indexes (segment-stems, reach-stems) |
| Total size | ~43 GB |

**Objects** (base URL `https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/`):

- `manifest.json`
- `catchments.parquet`
- `graph.parquet`
- `aux/snap_segments.parquet`
- `aux/snap_reaches.parquet`
- `NOTICE`, `CITATION.txt`, `README.md` (attribution objects)

**License and attribution:** CC BY-NC 4.0 (<https://creativecommons.org/licenses/by-nc/4.0/>), inherited from the source data, so NonCommercial use only.
Any use of this dataset must credit the source data authors:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) vector datasets”. Zenodo. doi:10.5281/zenodo.17435232.

DOI: <https://doi.org/10.5281/zenodo.17435232>

**Validate** any local copy:

```bash
hfx ./path/to/dataset --strict
```

## Versioning

HFX carries two independent version tracks: the SPEC track is the format version itself (`format_version`, currently 0.3.0), while the TOOLKIT track covers the lockstep workspace crates `hfx` and `hfx-cli`, whose next curated release is 0.5.0 (implementing spec 0.3.0).
See the [versioning policy](https://cooperbigfoot.github.io/hfx/spec/versioning/) for the full policy, the spec-to-toolkit mapping table, and the release procedure.

## Adapters

Contributions are welcome on the adapter side; a HydroBASINS adapter is the most-wanted next adapter.
See the [adapter guide](https://cooperbigfoot.github.io/hfx/adapter/) for how to build one and [adapters/](./adapters) for the existing compilers.

## Status

HFX spec 0.3.0 is the current format version, and the hosted GRIT 2.0.0 dataset conforms to it.
The workspace crates are `hfx` and `hfx-cli`, published to crates.io under those names since 0.4.0.
The next curated toolkit release, 0.5.0, implements spec 0.3.0 and makes `hfx.aux.d8_raster.v2` the blessed D8 raster schema.
