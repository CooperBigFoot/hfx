# Fixture: valid/tiny

## Topology

Five-atom Y-tree. IDs 1–5.

```
   1   2
    \ /
     3
     |
     4
     |
     5  (outlet)
```

`upstream_ids` mapping:

| id | upstream_ids |
|---|---|
| 1 | [] (headwater) |
| 2 | [] (headwater) |
| 3 | [1, 2] (confluence) |
| 4 | [3] |
| 5 | [4] |

## What this fixture tests

This is the canonical valid fixture. It exercises the full validator happy path:

- Expected `check_id`: **PASS** (no errors; one tolerated `schema.catchments.rg_size` WARNING because 5 rows < 4096)
- CRS: `EPSG:4326`
- `has_up_area: false`, `has_rasters: false`, `has_snap: false`

## Expected validator outcome

```
Exit code: 0
Result:    VALID
Diagnostics:
  [WARN] catchments.parquet (schema.catchments.rg_size): catchments.parquet row group 0
         has 5 rows; recommended range is [4096, 8192]
```

The rg_size WARNING is expected and tolerated in non-strict mode.

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
| `manifest.json` | `1714b292074c1f8e57c937239d6ec2231370e9dadeed7ebaa50bf074866dc62b` |
