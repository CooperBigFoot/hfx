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
| `raster` | Attach native-grid D8 rasters to an existing compiled HFX dataset |
| `validate` | Run the Rust HFX validator over the output |

`stage1`, `stage2`, `phase25`, and `write` accept `--regions` (comma-separated region codes, or `all`; default is all seven).

## Outputs

- `catchments.parquet`
- `graph.parquet`
- `manifest.json`
- `aux/snap_segments.parquet` — `hfx.aux.snap.v2` index `segment-stems` (references level 0)
- `aux/snap_reaches.parquet` — `hfx.aux.snap.v2` index `reach-stems` (references level 1)

## Attach D8 Rasters

The `raster` subcommand attaches the GRIT v1.0 raster products from [Zenodo record 15715535](https://zenodo.org/records/15715535) to an existing compiled HFX dataset. It requires:

- every applicable `drainage_direction` ZIP archive, passed once per archive with `--flow-dir-archive`;
- every applicable width-partitioned `drainage_area` ZIP archive, passed once per archive with `--flow-acc-archive`;
- the existing compiled dataset directory, passed with `--dataset-dir`;
- a disposable raster work directory, passed with `--work-dir`.

Run from the repo root:

```bash
uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py raster \
    --flow-dir-archive /path/to/drainage-direction-archive-1.zip \
    --flow-dir-archive /path/to/drainage-direction-archive-2.zip \
    --flow-acc-archive /path/to/drainage-area-archive-1.zip \
    --flow-acc-archive /path/to/drainage-area-archive-2.zip \
    --dataset-dir /path/to/existing-compiled-hfx \
    --work-dir /path/to/raster-work
```

The command mosaics the source rasters directly on their native EPSG:8857 grid and writes:

- `aux/d8/flow_dir.tif`
- `aux/d8/flow_acc.tif`

The operation preserves source values and nodata tags. `flow_dir.tif` remains `int8`, including negative GRASS flow-direction codes, and `flow_acc.tif` remains `int32` in km2. The native-grid mosaic performs direct cell copies and COG retiling, with the source CRS, resolution, alignment, and values preserved.

The command amends the existing `manifest.json` idempotently. Repeated runs converge to exactly one `hfx.aux.d8_raster.v2` entry:

```json
{
  "schema": "hfx.aux.d8_raster.v2",
  "artifacts": {
    "flow_dir": "aux/d8/flow_dir.tif",
    "flow_acc": "aux/d8/flow_acc.tif"
  },
  "metadata": {
    "crs": "EPSG:8857",
    "flow_dir_encoding": "grass",
    "flow_acc_units": "km2"
  }
}
```

The amended manifest retains `format_version` 0.3.0 and records `adapter_version` `grit-global-2.1.0`. Existing vector artifacts remain byte-identical.

## Validate Locally

```bash
cargo run -p hfx-cli -- /path/to/dataset --strict
```

## Test the Adapter

```bash
uv run --project adapters/grit-v2 pytest adapters/grit-v2
```

## License and Attribution

The compiled dataset inherits CC BY-NC 4.0 from the source data — NonCommercial use only. Any use must credit the source data authors:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) vector datasets”. Zenodo. doi:10.5281/zenodo.17435232.
