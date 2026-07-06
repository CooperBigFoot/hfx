# Datasets

Reference datasets give you concrete HFX artifacts to inspect and validate.
Use them when you want to open a working dataset by hand while reading the specification.

## Tiny in-repository dataset

The tiny dataset is a complete, valid HFX dataset at `format_version` 0.3.0.
It lives at `examples/tiny/` in the repository.
It has 5 drainage units.
Its `fabric_name` is `conformance-tiny`.
Its CRS, a coordinate reference system, is EPSG:4326.
Its `topology` is `tree`.

The dataset is roughly 7 KB across `manifest.json`, `catchments.parquet`, and `graph.parquet`.
Parquet is a columnar data file format used for the catchment and graph tables.
The directory also ships human-readable CSV dumps.
Treat this dataset as a read-only inspectable reference for implementers.

Validate it from the repository root with:

```bash
# validate the tiny reference dataset; expect Result: VALID and exit 0
cargo run -p hfx-cli -- examples/tiny --strict
```

This command validates the in-repository tiny dataset in strict mode.
The equivalent installed-binary command is `hfx examples/tiny --strict`.

## Hosted GRIT dataset

The hosted GRIT dataset is an HFX compilation of Global River Topology, a global river-network dataset.
It carries segment drainage units at `level=0` and reach drainage units at `level=1`.

| Property | Value |
| --- | --- |
| `format_version` | 0.3.0 |
| `fabric_name` | grit |
| `fabric_version` | 1.0.0 |
| `adapter_version` | grit-global-2.0.0 |
| `unit_count` | 22,337,300 |
| `topology` | dag |
| CRS | EPSG:4326 |
| Auxiliaries | two `hfx.aux.snap.v2` snap indexes, segment-stems and reach-stems |
| Total size | ~43 GB |

The hosted objects use Cloudflare R2, an object storage service.
They are readable directly over HTTPS with range requests, HTTP reads of selected byte spans.
The base object URL is `https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/`.
The manifest URL is `https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/manifest.json`.

The base URL contains these HFX objects:

- `manifest.json`
- `catchments.parquet`
- `graph.parquet`
- `aux/snap_segments.parquet`
- `aux/snap_reaches.parquet`

The base URL also contains these attribution objects:

- `NOTICE`
- `CITATION.txt`
- `README.md`

Upstream Tech sponsors the hosting infrastructure as an infrastructure sponsor only.
Upstream Tech is not the publisher or vendor of the data.
Their site is `https://upstream.tech/`.

The source-data license is [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/), inherited from the source data, NonCommercial use only.
Any use must credit the source-data authors.
Use this source-data citation:

> Wortmann, M. et al. (2025) "Global River Topology (GRIT) vector datasets". Zenodo. doi:10.5281/zenodo.17435232.

The DOI URL is `https://doi.org/10.5281/zenodo.17435232`.

```bibtex
@dataset{wortmann_2025_17435232,
  author       = {Wortmann, Michel and
                  Slater, Louise and
                  Hawker, Laurence and
                  Liu, Yinxue and
                  Neal, Jeffrey},
  title        = {Global River Topology (GRIT) vector datasets},
  month        = dec,
  year         = 2025,
  publisher    = {Zenodo},
  version      = {1.0},
  doi          = {10.5281/zenodo.17435232},
  url          = {https://doi.org/10.5281/zenodo.17435232},
}
```

Validate a downloaded copy with:

```bash
# validate a downloaded GRIT copy; exit code 0 means valid
hfx ./path/to/dataset --strict
```

This command validates your local copy of the GRIT HFX dataset in strict mode.
