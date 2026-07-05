# GRIT v2 → HFX Adapter

Python adapter that compiles the [GRIT](https://zenodo.org/records/17435232) v1.0 vector hydrofabric into one planetary HFX v0.3.0 dataset with two drainage-unit levels: segment catchments (`level=0`) and reach catchments (`level=1`).

## Status

Current reference adapter. This adapter compiled the hosted GRIT 2.0.0 reference dataset:

<https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/>

The hosted manifest reports:

| Property | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | grit |
| `fabric_version` | 1.0.0 |
| `adapter_version` | grit-global-2.0.0 |
| `topology` | dag |
| CRS | EPSG:4326 |
| `has_up_area` | true |
| `unit_count` | 22,337,300 |

The 22,337,300 drainage units are GRIT segment catchments (`level=0`) plus GRIT reach catchments (`level=1`).

## Inputs

Source is the GRIT v1.0 Zenodo outer archive ([zenodo.org/records/17435232](https://zenodo.org/records/17435232)). The adapter extracts the per-region segment, segment-catchment, reach, and reach-catchment GeoPackages for the seven published regions: `AF`, `AS`, `EU`, `NA`, `SA`, `SI`, `SP`.

## How to Run

The build is staged. Run from the repo root with `uv`, passing the working root via `--root` and the Zenodo outer archive via `--outer-archive`:

```bash
uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py \
    --root /path/to/grit-workdir \
    --outer-archive /path/to/17435232.zip \
    <subcommand>
```

Subcommands, in pipeline order:

| Subcommand | Purpose |
|---|---|
| `probe` | Tier 0 reach schema probe |
| `stage1` | Per-region preprocessing: extract inputs and write intermediate shards |
| `stage2` | Dense ID assignment over existing shards, then Hilbert sort |
| `phase25` | Resolve graph edges and compute reach upstream areas |
| `write` | Stages 6–9: write the final HFX artifacts (`--out` overrides the output directory) |
| `validate` | Run the Rust HFX validator over the output |

`stage1`, `stage2`, `phase25`, and `write` accept `--regions` (comma-separated region codes, or `all`; default is all seven).

## Outputs

- `catchments.parquet`
- `graph.parquet`
- `manifest.json`
- `aux/snap_segments.parquet` — `hfx.aux.snap.v2` index `segment-stems` (references level 0)
- `aux/snap_reaches.parquet` — `hfx.aux.snap.v2` index `reach-stems` (references level 1)

## Validate Locally

```bash
cargo run -p hfx-cli -- /path/to/dataset --strict
```

## License and Attribution

The compiled dataset inherits CC BY-NC 4.0 from the source data — NonCommercial use only. Any use must credit the source data authors:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) vector datasets”. Zenodo. doi:10.5281/zenodo.17435232.
