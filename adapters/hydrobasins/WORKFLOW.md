# HydroBASINS Adapter Workflow

This operator workflow covers single-region, selected multi-region, all-present-regions, strict-build, and planetary-manifest operation for the HydroBASINS standard, without-lakes product across one contiguous source Pfafstetter level range. The default source range is `1-12`. The adapter normalizes every selected source layer to EPSG:4326 and writes one nested HFX v0.3.0 dataset, with optional HydroRIVERS snap output when source Pfaf level 12 is selected.

## Prepare Inputs and Environment

Pass the canonical `extract/` directory to `--basins` and its global `extract/pour/` directory to `--pour-points`. Every selected region must contain one polygon layer for every selected source Pfaf level at `extract/hybas_<region>/hybas_<region>_lev<NN>_v1c.shp`. The global pour directory must contain `extract/pour/hybas_pour_lev<NN>_v1.shp` for every selected source Pfaf level. The nine standard region codes are `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, and `si`.

```text
/path/to/hydrobasins/
└── extract/
    ├── hybas_af/
    │   ├── hybas_af_lev01_v1c.shp
    │   ├── ...
    │   └── hybas_af_lev12_v1c.shp
    ├── hybas_eu/
    │   ├── hybas_eu_lev01_v1c.shp
    │   ├── ...
    │   └── hybas_eu_lev12_v1c.shp
    └── pour/
        ├── hybas_pour_lev01_v1.shp
        ├── ...
        └── hybas_pour_lev12_v1.shp
```

Pour points are global per-level inputs. Do not create per-region pour-point directories or symlinks. Basin inputs use the `v1c` standard-product name and pour-point inputs use the ancillary `v1` name.

HydroRIVERS remains optional and uses a different resolver contract. For a single-region build, `--rivers` names a directory containing exactly one recursively discoverable shapefile. For `--regions` and `--all-regions`, each resolver root is `--rivers/<code>/` and must contain exactly one recursively discoverable shapefile. HydroRIVERS input must contain `HYRIV_ID`, `HYBAS_L12`, `UPLAND_SKM`, `NEXT_DOWN`, and one LineString geometry column.

From `adapters/hydrobasins`, prepare the environment:

```bash
uv sync
```

Do not generate or commit `uv.lock` as part of a documentation-only change.

## Inspect One Region

`extract` accepts only `--region`. It remains fixed to source Pfaf level 12 and does not accept `--levels`. It reads the region's canonical `hybas_<region>_lev12_v1c.shp` and the global `hybas_pour_lev12_v1.shp`, reports both paths, feature counts, CRS information, required basin-column presence, and the pour-point join key, and writes no HFX artifacts.

```bash
# Inspect one region at source Pfaf level 12; this writes no HFX files.
uv run python build_adapter.py extract \
  --region gr \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour
```

## Select Levels and Compile Regions

`build` accepts one optional `--levels` selector in source Pfaf numbering. Use a singleton such as `--levels 12` or an inclusive contiguous ascending range such as `--levels 6-12`. Omitting `--levels` selects `1-12`. The option rejects non-contiguous forms such as `1,3` and `"1 3"`, descending ranges such as `12-6`, out-of-domain values such as `0` and `13`, malformed ranges, and repeated occurrences.

HFX levels are zero-based relative to the selected range: `HFX level = source Pfaf level - min(selected range)`. Always identify whether an operational check refers to a source Pfaf level or an HFX level. The default maps source Pfaf 1 to HFX level 0 and source Pfaf 12 to HFX level 11; `6-12` maps them to HFX levels 0 through 6; singleton `12` maps to HFX level 0.

The `build` command requires exactly one of `--region`, `--regions`, or `--all-regions`; these selectors are mutually exclusive. `--regions` accepts comma-separated, whitespace-separated, or mixed code lists, preserves their authored order, and rejects duplicates. Explicit codes are resolved through filenames rather than parser-validated against the standard-code list. `--all-regions` scans `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, `si` in that fixed order and selects regions present for the chosen source Pfaf range; it errors only when none is present.

```bash
# Build one region at the default source Pfaf range 1-12 with snap.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --rivers /path/to/hydrorivers/gr \
  --out ./out/gr-pfaf1-12-snap

# Build one region for a partial nested source range without snap.
uv run python build_adapter.py build \
  --region gr \
  --levels 6-10 \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --out ./out/gr-pfaf6-10

# Build one source Pfaf level as HFX level 0.
uv run python build_adapter.py build \
  --region gr \
  --levels 12 \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --out ./out/gr-pfaf12

# Build two regions at source Pfaf levels 6-12 with snap.
uv run python build_adapter.py build \
  --regions af,eu \
  --levels 6-12 \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --rivers /path/to/hydrorivers \
  --out ./out/af-eu-pfaf6-12-snap

# Build every present standard region with the default range.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --out ./out/all-present-pfaf1-12

# Build the planetary metadata path with the default range and snap.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --rivers /path/to/hydrorivers \
  --out ./out/global-pfaf1-12-snap \
  --planetary \
  --strict-build
```

Leaving out `--rivers` produces a core-only dataset with no snap artifact or auxiliary manifest declaration. Supplying `--rivers` with a range that excludes source Pfaf level 12 is fatal with `--rivers requires --levels to include source Pfaf level 12`.

`--planetary` changes manifest metadata only. It omits `region` and writes the exact bbox `[-180, -90, 180, 90]`; it does not select, download, or compile missing regions. `--strict-build` is an independent optional guard flag that rejects antimeridian-wrap candidates instead of warning.

## Build Sequence

The build executes in this order:

1. Parse one singleton or contiguous source Pfaf range and resolve selected regions in authored order, or in fixed standard-code scan order for `--all-regions`.
2. Iterate source Pfaf levels in ascending order. For each level, load and normalize that level's global pour points once.
3. At each source Pfaf level, load and normalize every selected regional polygon layer. For levels above the coarsest selected level, assign each polygon's `parent_id` from the unique next-coarser regional `PFAF_ID` prefix. Missing or ambiguous parents are fatal, with no spatial fallback.
4. At each source Pfaf level, perform the total `HYBAS_ID` pour-point join and select one deterministic outlet for every polygon. Each level uses its own pour-point layer; coarser outlets are never derived from finest-level descendants.
5. Merge regions within each level, reject duplicate `HYBAS_ID` values, concatenate all selected levels, and reject IDs duplicated anywhere across the complete dataset.
6. Apply one global `(level ASC, hilbert ASC)` order, with `id` as the deterministic tie-break. Run the antimeridian guard over the complete unit set.
7. Build each HFX level's graph from that source layer's `NEXT_DOWN`, resolve targets across the complete merged same-level regional union, apply `NEXT_DOWN = 0` and `ENDO = 2` terminal rules, and check tree shape and acyclicity.
8. Write `catchments.parquet` and `graph.parquet` once in the same multi-level order with balanced row groups. Run GeoParquet 1.1 structural validation immediately after writing catchments.
9. When `--rivers` is present, load regional HydroRIVERS layers, retain `HYBAS_L12` joins only to source Pfaf-12 units, derive regional stem roles, globally order retained reaches, and write `aux/snap_stems.parquet` with its required covering and statistics.
10. Write `manifest.json` last. Snap metadata declares `references_levels = [12 - min(selected range)]`, which is `[11]` for the default `1-12` build.

The committed `attribute_join_scan_report.json` supplies global attribute evidence for these contracts: 3,786,218 child joins across 99 adjacent-level pairs, 3,786,228 basins covered across 12 pour-point levels, and 3,786,228 basins checked collision-free across 108 regional level layers. Every check reported zero findings.

An ordinary manifest includes the selected codes joined by commas in selection order, such as `af,eu`, and the combined geometry bbox. An `--all-regions` label follows fixed scan order for the regions present. A planetary manifest omits `region` and uses the exact bbox `[-180, -90, 180, 90]`.

For the default source Pfaf range `1-12`, snap data adds this auxiliary declaration:

```json
{
  "auxiliary": [
    {
      "schema": "hfx.aux.snap.v2",
      "artifacts": {
        "snap": "aux/snap_stems.parquet"
      },
      "metadata": {
        "name": "stems",
        "description": "Unclipped HydroRIVERS reach centerlines for HydroBASINS Pfaf-12 snapping. HydroRIVERS and HydroBASINS are HydroSHEDS products covered by the HydroSHEDS License Agreement. weight = UPLAND_SKM (km^2). stem_role = mainstem/tributary derived from NEXT_DOWN confluences.",
        "references_levels": [11],
        "weight_semantics": "drainage_area_km2"
      }
    }
  ]
}
```

For another selected range, compute the single declared HFX level as `12 - min(selected range)`. The snap join remains keyed by `HYBAS_L12` to source Pfaf-12 units.

The `auxiliary` key is absent when `--rivers` is omitted.

For both Parquet artifacts, a file with fewer than 4,096 units has one row group. A file with at least 4,096 units has balanced groups from 4,096 through 8,192 rows inclusive. Counts from 4,096 through 8,192 validly produce one balanced group.

## Validate the Dataset

Run strict, 100-percent HFX validation in text and JSON modes:

```bash
# Validate and write reports under <dataset>/validation.
uv run python build_adapter.py validate ./out/global-pfaf1-12-snap

# Validate with an explicit report directory.
uv run python build_adapter.py validate ./out/global-pfaf1-12-snap \
  --report-dir ./out/global-pfaf1-12-snap-validation
```

By default, reports are written under the dataset's `validation` directory as `validator-report.text` and `validator-report.json`. With `--report-dir`, they are written to the selected directory. A corresponding `validator-report.text.stderr` or `validator-report.json.stderr` file is written when that mode emits stderr. The subcommand invokes the repository validator with `--strict --sample-pct 100`; it does not repeat the GeoParquet validation performed during `build`.

Run the unchanged unit-test suite from `adapters/hydrobasins` with:

```bash
uv run python -m unittest discover -p "test_*.py"
```

Test and conformance coverage includes nested default and partial-range builds, same-level graph behavior, parent and outlet joins, source Pfaf-12 snap gating, and two-region merge behavior. The committed attribute scan is evidence for global join assumptions; this workflow does not claim that a source-data global build has run.

## Output and Validation Checklist

- `catchments.parquet`, `graph.parquet`, and `manifest.json` are present.
- There is one catchments row and one graph row per selected HydroBASINS unit across all selected source Pfaf levels.
- Every `HYBAS_ID` is positive and globally unique across regions and levels.
- The coarsest selected source Pfaf level is HFX level 0 and has null `parent_id`.
- Every finer selected level has a total, unique regional `PFAF_ID`-prefix parent join to the immediately preceding source Pfaf level.
- Every selected source Pfaf level uses its own global pour-point layer and has a total `HYBAS_ID` outlet join.
- Coarser outlets come from their own level's DEM-derived pour points, never from finest-level descendants, polygon geometry, or HydroRIVERS.
- Catchments and graph rows follow `(level ASC, hilbert ASC)`, with `id` as the final deterministic tie-break.
- Every graph edge stays within one HFX level; cross-region targets resolve against the complete merged union at that level.
- `NEXT_DOWN = 0` and `ENDO = 2` produce terminal rows, and every same-level graph is tree-shaped and acyclic.
- With `--rivers`, the selected source range includes source Pfaf level 12, retained `unit_id` values equal `HYBAS_L12`, and the manifest declares `references_levels = [12 - min(selected range)]`.
- For the default `1-12` snap build, source Pfaf level 12 is HFX level 11 and `references_levels` is `[11]`.
- A core-only build has no `aux/snap_stems.parquet` and no `auxiliary` manifest key.
- Both Parquet artifacts follow the balanced row-group rules.
- `catchments.parquet` passed GeoParquet 1.1 structural validation during the build.
- HFX validation passed in strict mode with a 100-percent sample.
- With `--rivers`, `aux/snap_stems.parquet` is present with exactly `id`, `unit_id`, `weight`, `stem_role`, `bbox`, and `geometry`.
- Snap `id` is a sequential signed 64-bit integer from 1 through N after global ordering; `unit_id` is signed 64-bit and equals `HYBAS_L12`; `weight` is `UPLAND_SKM` as float32.
- Snap `geometry` is the original, unclipped HydroRIVERS LineString encoded as WKB after normalization to EPSG:4326.
- Snap `bbox` is the required GeoParquet covering struct with `xmin`, `ymin`, `xmax`, and `ymax` leaves, mapped to `geometry` by GeoParquet 1.1 metadata. Row-group statistics are present for the consumer's spatial query path.
- Every retained `unit_id` refers to the HFX level corresponding to source Pfaf level 12. Reaches with unresolved `HYBAS_L12` are dropped, with the adapter logging `dropped %d HydroRIVERS reaches with HYBAS_L12 absent from the unit set`; there is no spatial fallback join.
- `stem_role` is only `mainstem`, `tributary`, or `unknown`; `distributary` is never emitted. At a regional confluence, the largest `UPLAND_SKM` continues as `mainstem`, with larger `HYRIV_ID` breaking a weight tie. A positive `NEXT_DOWN` outside the loaded regional reach set is `unknown`; terminal reaches and reaches without a competing sibling remain `mainstem`.
- Multi-region reaches are concatenated, filtered against the merged source Pfaf-12 units, globally centroid-Hilbert ordered using the merged units' total bounds, and assigned sequential IDs only afterward. Region index and source order provide deterministic tie-breaks.
- Antimeridian candidates with raw stored longitude extent strictly greater than 180 degrees warn by default and fail under `--strict-build`; the guard does not unwrap or split geometry.
- HydroBASINS, HydroBASINS Pour Points, HydroRIVERS, and compiled HFX output remain subject to the HydroSHEDS License Agreement and are not freely redistributable as stand-alone materials.

## Verify Snap and Polygon Resolution

Choose a small region and a query coordinate on or near a known HydroRIVERS reach. Build two otherwise equivalent datasets from `adapters/hydrobasins`: one with the snap input and one without it. Both commands rely on the default source Pfaf range `1-12`.

```bash
# Run from hfx/adapters/hydrobasins.
uv run python build_adapter.py build \
  --region <REGION> \
  --basins <HYDROBASINS_EXTRACT_DIR> \
  --pour-points <HYDROBASINS_EXTRACT_POUR_DIR> \
  --rivers <HYDRORIVERS_REGION_DIR> \
  --out ./out/<REGION>-pfaf1-12-snap

uv run python build_adapter.py build \
  --region <REGION> \
  --basins <HYDROBASINS_EXTRACT_DIR> \
  --pour-points <HYDROBASINS_EXTRACT_POUR_DIR> \
  --out ./out/<REGION>-pfaf1-12-core
```

Strictly validate the snap-enabled dataset from the HFX repository root:

```bash
cargo run -p hfx-cli -- \
  adapters/hydrobasins/out/<REGION>-pfaf1-12-snap \
  --strict \
  --sample-pct 100
```

From the pourpoint repository root, delineate the on-network coordinate against the snap-enabled dataset. The `--snap-radius 1000` spelling is optional because 1000 m is the default, but showing it makes the tested radius explicit. Do not pass `--no-refine` in this check because that flag changes the refinement result to `Disabled`.

```bash
cargo run --release -- delineate \
  --dataset <ABSOLUTE_PATH_TO_REGION_PFAF1_12_SNAP_DATASET> \
  --lat <LAT> \
  --lon <LON> \
  --format geojson \
  --snap-radius 1000
```

Inspect the successful feature in the returned GeoJSON FeatureCollection. It must contain a watershed geometry, a `resolution_method` beginning with `snap(`, and a `refinement` value beginning with `best_effort_skipped(` whose provenance contains `NoD8AuxDeclared`. HydroBASINS has no D8 auxiliary, so that terminal refinement result is expected.

Run the same coordinate against the core-only dataset:

```bash
cargo run --release -- delineate \
  --dataset <ABSOLUTE_PATH_TO_REGION_PFAF1_12_CORE_DATASET> \
  --lat <LAT> \
  --lon <LON> \
  --format geojson
```

This result must also contain a watershed, but its `resolution_method` begins with `pip(` because the dataset has no `hfx.aux.snap.v2` declaration.

Snap selection is dataset-level. A snap-enabled dataset does not fall back to point-in-polygon for an individual query. With the default 1000 m radius, a point farther than 1000 m from every channel returns `NoSnapCandidates`; use an on-network or near-network point for this verification.

## Licensing

HydroBASINS, HydroBASINS Pour Points, and HydroRIVERS are HydroSHEDS products covered by the same HydroSHEDS License Agreement. The agreement permits commercial, non-commercial, and internal use, but prohibits public or open redistribution of the licensed materials as a stand-alone product. Internal use of the source and compiled HFX output is permitted; do not treat either the source layers or compiled output as freely redistributable stand-alone material.

## Operational Cautions

- The guard computes each combined EPSG:4326 unit's raw stored longitude extent as `maxx - minx`; an extent strictly greater than 180 degrees is an antimeridian-wrap candidate. Default mode warns and continues writing, while `--strict-build` fails before artifacts are written.
- Antimeridian detection does not unwrap or split geometry. Operational resolution and the actual global compile and hosting remain separate operator actions.
- Source Pfaf levels 1 through 12 are supported as a singleton or contiguous ascending range. The with-lakes product is unsupported because its matching ancillary pour-points product is unavailable.
- Pour points are global per-level inputs under `extract/pour/`; do not stage per-region copies or symlinks.
- Snap dispatch is dataset-level, so a dataset declaring `hfx.aux.snap.v2` has no per-query point-in-polygon fallback. With the default 1000 m radius, a point farther than 1000 m from every channel returns `NoSnapCandidates`.
- Planetary mode changes metadata only. Selecting all present source regions and performing the actual planetary compile and hosting remain operator actions.
