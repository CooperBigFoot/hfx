# Fixture: invalid/dangling-upstream-ref

## Topology

Same five-atom Y-tree as `valid/tiny`, except id=3 has a dangling upstream reference to id=999, which does not exist in `catchments.parquet`.

```
   1   2  999(!)
    \ | /
     3       ← upstream_ids = [1, 2, 999]
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
| 3 | [1, 2, **999**] ← dangling ref |
| 4 | [3] |
| 5 | [4] |

## What this fixture tests

- Expected `check_id`: **`referential.upstream_not_in_catchments`**
- Verifies that upstream references pointing to non-existent catchment IDs are detected.
- Diagnostic message must contain `"999"` (the missing ID).

## Expected validator outcome

```
Exit code: 1
Result:    INVALID
Diagnostics:
  [ERROR] cross-file (referential.upstream_not_in_catchments): upstream_id 999 at
          graph row 2 does not exist in catchments.parquet
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
| `graph.arrow` | `65ff9b30663ea4c4144f1ccd4546c46cf35ae92ac7465d470ac7a89b8661a1a3` |
| `manifest.json` | `1714b292074c1f8e57c937239d6ec2231370e9dadeed7ebaa50bf074866dc62b` |
