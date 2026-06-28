# GRIT 2.0.0 Re-host -- HFX v0.3.0 Covering (checkpoint runbook)

Status: PREPARED by s13 (agent). NOT FIRED. Every step below is HUMAN/infra. No
agent runs the re-compile, the dry-run against real data, or the upload. The
codex sandbox has no network, the GRIT source archive is not on disk, and R2
writes are human-only via `aws --profile upstream-r2`.

This re-publishes the already-hosted GRIT 2.0.0 dataset rebuilt with the HFX
v0.3.0 GeoParquet `bbox` covering struct (manifest `format_version` 0.3.0,
validated by the hfx toolkit 0.4.0). It OVERWRITES the 5 data objects under
`grit/2.0.0/` in place (default; see OD-8). The attribution objects (`NOTICE`,
`CITATION.txt`, `README.md`) are NOT touched -- those were published by CP-1
(`scripts/upload-r2-attribution.sh`) and stay as-is.

## OD-8 -- DECIDE BEFORE STEP (c)

OD-8 (`planning/milestone-alden-feedback/pending-human-decisions.md`) reserves
the prefix policy for Nik: overwrite `grit/2.0.0/` IN PLACE (planner default) or
publish the 0.3.0 build under a NEW prefix. In-place overwrite breaks any
external client still pinned to pyshed 0.2.4 (flat bbox) reading that URL. Make
the call, then:

- in place   -> leave `scripts/upload-r2-rehost.sh` `TARGET_PREFIX` as `"grit/2.0.0/"`.
- new prefix -> change ONLY the `TARGET_PREFIX` one-liner (e.g. `"grit/2.0.0-covering/"`).
  The scope guard regex is derived from that constant and tracks the change.

## Object set (5 data objects, overwritten; manifest LAST)

| Object | Content |
|---|---|
| `catchments.parquet` | WKB Polygon + `bbox` covering struct (leaf row-group stats) |
| `graph.parquet` | flat (no geometry, no covering) |
| `aux/snap_segments.parquet` | snap index, `hfx.aux.snap.v2` |
| `aux/snap_reaches.parquet` | snap index, `hfx.aux.snap.v2` |
| `manifest.json` | `format_version` 0.3.0 (uploaded LAST) |

## Manifest facts (delta from the live 0.2.1 object)

| Field | Was (live) | After re-host |
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

(c) PRE-STATE + DRY-RUN -- human (OD-8 must be chosen first):
      aws s3api list-objects-v2 --bucket basin-delineations-public \
        --prefix grit/2.0.0/ --profile upstream-r2 \
        > grit-2.0.0-prestate-$(date +%Y%m%d).json   # ETags/sizes, rollback insurance
      scripts/upload-r2-rehost.sh --dry-run --staging <staging>/grit-2.0.0
    Review: exactly 5 operations, all under `grit/2.0.0/` (or the chosen prefix),
    `manifest.json` listed LAST, attribution objects reported as SKIPPED.

(d) EXECUTE -- human only:
      scripts/upload-r2-rehost.sh --execute --staging <staging>/grit-2.0.0
      # type the confirmation phrase exactly:  REHOST GRIT 2.0.0 COVERING

## Post-fire assertions (human)

- `curl -s .../grit/hfx-v0.3.0/manifest.json | jq -r .format_version` -> `0.3.0`.
- The 5 data-object ETags CHANGED vs the pre-state listing (bytes were rewritten);
  the attribution-object ETags (`NOTICE`, `CITATION.txt`, `README.md`) UNCHANGED.
- Re-run the validator on a fresh download -> exit 0.

## Do NOT (applies to the script and this checkpoint)

- No `--execute` by any agent (human, step s15).
- No external sync utility. No `aws s3 rm`. No deletion. No teardown.
- Do not touch the attribution objects (`NOTICE` / `CITATION.txt` / `README.md`).
- Do not touch any prefix other than the chosen `TARGET_PREFIX`.
