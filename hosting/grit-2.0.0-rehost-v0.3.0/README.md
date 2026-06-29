# GRIT 2.0.0 Re-host -- HFX v0.3.0 Covering (checkpoint runbook)

Status: PREPARED by s13 (agent). NOT FIRED. Every step below is HUMAN/infra. No
agent runs the re-compile, the dry-run against real data, or the upload. The
codex sandbox has no network, the GRIT source archive is not on disk, and R2
writes are human-only via `aws --profile upstream-r2`.

This PUBLISHES the GRIT 2.0.0 dataset, rebuilt with the HFX v0.3.0 GeoParquet
`bbox` covering struct (manifest `format_version` 0.3.0, validated by the hfx
toolkit 0.4.0), to a NEW additive prefix `grit/hfx-v0.3.0/`. It overwrites
nothing and deletes nothing. The legacy `grit/2.0.0/` dataset (format_version
0.2.1, flat bbox) stays live for any client still pinned to it. The new prefix
is empty before this run.

`scripts/upload-r2-rehost.sh` PUBLISHES the 5 data objects (`TARGET_PREFIX`
already = `grit/hfx-v0.3.0/`). The attribution objects (`NOTICE`,
`CITATION.txt`, `README.md`) are published to the new prefix by the separate
attribution step below; the data-object script does not touch them.

## OD-8 -- RESOLVED (additive new prefix)

OD-8 (`planning/milestone-alden-feedback/pending-human-decisions.md`) is
DECIDED: the 0.3.0 build is published under the NEW prefix `grit/hfx-v0.3.0/`,
NOT in place over `grit/2.0.0/`. This keeps every external client still pinned
to pyshed 0.2.4 (flat bbox) reading the legacy URL working unchanged. The
script's `TARGET_PREFIX` is already `"grit/hfx-v0.3.0/"`; the scope-guard regex
is derived from that constant.

## Object set (5 data objects PUBLISHED; manifest LAST)

| Object | Content |
|---|---|
| `catchments.parquet` | WKB Polygon + `bbox` covering struct (leaf row-group stats) |
| `graph.parquet` | flat (no geometry, no covering) |
| `aux/snap_segments.parquet` | snap index, `hfx.aux.snap.v2` |
| `aux/snap_reaches.parquet` | snap index, `hfx.aux.snap.v2` |
| `manifest.json` | `format_version` 0.3.0 (uploaded LAST) |

## Manifest facts (delta from the legacy 0.2.1 dataset at `grit/2.0.0/`)

| Field | Legacy `grit/2.0.0/` | New `grit/hfx-v0.3.0/` |
|---|---|---|
| `format_version` | 0.2.1 | 0.3.0 |
| catchments / snap bbox | 4 flat float32 columns | `bbox` covering struct |
| snap aux declaration | `hfx.aux.snap.v1` | `hfx.aux.snap.v2` |
| `fabric_version` / `adapter_version` | grit 1.0.0 / grit-global-2.0.0 | unchanged |
| `unit_count` | 22,337,300 | unchanged |

## Human fire sequence

(a) RE-COMPILE -- infra/human (the GRIT source archive is NOT on disk):
    rebuild GRIT 2.0.0 from source with the merged grit-v2 adapter
    (`adapters/grit-v2/build_adapter.py`, FORMAT_VERSION 0.3.0 + covering struct)
    into a local staging dir, e.g. `<staging>/grit-2.0.0/`.

(b) VALIDATE -- human:
      cargo run -p hfx-validator -- <staging>/grit-2.0.0 --strict      # exit 0
      python <covering-stack>/verify_struct_leaf_stats.py \
        <staging>/grit-2.0.0/catchments.parquet                        # exit 0
    (`verify_struct_leaf_stats.py` ships with the s07/s08 covering stack.) The
    leaf-stats check is P5 gate-1: without row-group stats on
    `bbox.{xmin,ymin,xmax,ymax}`, the covering reads SLOWER than the flat layout.

(c) PRE-STATE + DRY-RUN -- human:
      aws s3api list-objects-v2 --bucket basin-delineations-public \
        --prefix grit/hfx-v0.3.0/ --profile upstream-r2 \
        > grit-hfx-v0.3.0-prestate-$(date +%Y%m%d).json   # expect EMPTY: the new prefix has nothing yet
      scripts/upload-r2-rehost.sh --dry-run --staging <staging>/grit-2.0.0
    Review: exactly 5 operations, all under `grit/hfx-v0.3.0/`, `manifest.json`
    listed LAST. The empty-prefix listing is the for-the-record pre-state; there
    is nothing to overwrite, so there is no ETag-diff rollback to capture.

(d) EXECUTE -- human only:
      scripts/upload-r2-rehost.sh --execute --staging <staging>/grit-2.0.0
      # type the confirmation phrase exactly:  REHOST GRIT 2.0.0 COVERING

(e) PUBLISH ATTRIBUTION -- human only (resolves the 404 on the attribution
    objects under the new prefix). `NOTICE` and `CITATION.txt` are byte-identical
    to the legacy `grit/2.0.0/` copies; `README.md` is the 0.3.0 dataset README:
      aws s3 cp hosting/grit-2.0.0/NOTICE         s3://basin-delineations-public/grit/hfx-v0.3.0/NOTICE        --profile upstream-r2 --content-type text/plain
      aws s3 cp hosting/grit-2.0.0/CITATION.txt   s3://basin-delineations-public/grit/hfx-v0.3.0/CITATION.txt   --profile upstream-r2 --content-type text/plain
      aws s3 cp hosting/grit-hfx-v0.3.0/README.md s3://basin-delineations-public/grit/hfx-v0.3.0/README.md      --profile upstream-r2 --content-type text/markdown

## Post-fire assertions (human)

- The 5 data objects now EXIST under `grit/hfx-v0.3.0/`:
      aws s3api list-objects-v2 --bucket basin-delineations-public \
        --prefix grit/hfx-v0.3.0/ --profile upstream-r2   # 5 data objects + 3 attribution objects
- `curl -s .../grit/hfx-v0.3.0/manifest.json | jq -r .format_version` -> `0.3.0`.
- The 3 attribution objects (`NOTICE`, `CITATION.txt`, `README.md`) EXIST under
  `grit/hfx-v0.3.0/` (no 404).
- The legacy `grit/2.0.0/` dataset is UNCHANGED (it was never touched).
- Re-run the validator on a fresh download -> exit 0.

## Do NOT (applies to the script and this checkpoint)

- No `--execute` by any agent (human, step s15).
- No external sync utility. No `aws s3 rm`. No deletion. No teardown.
- Do not touch the legacy `grit/2.0.0/` prefix.
- Do not touch any prefix other than `grit/hfx-v0.3.0/` (`TARGET_PREFIX`).
