# Fixture: invalid/crs-mismatch

## Topology

Same five-atom Y-tree as `valid/tiny`. Catchments and graph are identical to `valid/tiny`. Only the manifest differs: `crs` is set to `"EPSG:32632"` instead of the required `"EPSG:4326"`.

```
   1   2
    \ /
     3
     |
     4
     |
     5  (outlet)
```

`upstream_ids` mapping (same as valid/tiny):

| id | upstream_ids |
|---|---|
| 1 | [] (headwater) |
| 2 | [] (headwater) |
| 3 | [1, 2] |
| 4 | [3] |
| 5 | [4] |

## What this fixture tests

- Expected `check_id`: **`manifest.crs`**
- Verifies that a manifest declaring an unsupported CRS (`EPSG:32632`) is rejected.
- Diagnostic message must contain `"EPSG:32632"`.

## Expected validator outcome

```
Exit code: 1
Result:    INVALID
Diagnostics:
  [ERROR] manifest.json field "crs" (manifest.crs): crs must be "EPSG:4326",
          got "EPSG:32632"
  [WARN]  catchments.parquet (schema.catchments.rg_size): catchments.parquet row
          group 0 has 5 rows; recommended range is [4096, 8192]
```

## Regenerate

```bash
uv run conformance/generate_fixtures.py
```

Note: this regenerates ALL fixtures; there is no per-fixture flag.

## Last regenerated

2026-04-21

## File SHA-256 manifest

| File | SHA-256 |
|---|---|
| `catchments.parquet` | `7a2d06f1260173dd665e21c0732fd12d52809bb925e5ffe712394cb185e0684a` |
| `graph.arrow` | `678f8d1ac90d27e5dbe3453d08d6103119dc5748fca1dd36eab917b2bd674d2c` |
| `manifest.json` | `722b4afd967a6912e34ce5c473c55f6c2307042323844887e6bfcff68b61a8c5` |
