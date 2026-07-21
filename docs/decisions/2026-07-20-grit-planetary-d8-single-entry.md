# GRIT planetary D8 rasters use one entry

**Status:** Proposed

**Date:** 2026-07-20

## Context

Milestone 2 evaluates one planetary `hfx.aux.d8_raster.v2` entry for the GRIT
drainage-direction and drainage-area pair. The evidence in this record uses
pinned source ref `e0269241956f5d7e4b93ef7bac1813664266141f`.

The source inventories came from these public APIs and remain saved verbatim:

- `https://zenodo.org/api/records/15715535`
- `https://zenodo.org/api/records/17435232`

### Raster inventory

The selected scope contains one global tile index, seven regional direction
archives, and seven regional width-partitioned drainage-area archives. Each
row records the API-declared byte size, checksum algorithm and value, and
content URL. Local byte-count and MD5 verification passed for every row.

| Product | Region | Filename | Bytes | Checksum | Content URL |
| --- | --- | --- | ---: | --- | --- |
| direction | AF | `GRITv1.0_drainage_direction_AF_EPSG8857.zip` | 7,965,063,751 | `md5:4af41794e00801da415a568bbf600e29` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_AF_EPSG8857.zip/content` |
| area | AF | `GRITv1.0_drainage_area_AF_EPSG8857.zip` | 30,574,144,752 | `md5:4f684d075ab05a23fb82c7360cf24311` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_AF_EPSG8857.zip/content` |
| direction | AS | `GRITv1.0_drainage_direction_AS_EPSG8857.zip` | 7,047,808,028 | `md5:94cb2984b44a95adc207369334667ca6` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_AS_EPSG8857.zip/content` |
| area | AS | `GRITv1.0_drainage_area_AS_EPSG8857.zip` | 27,270,654,029 | `md5:e028f0ae52eb01228ba78a029d812f38` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_AS_EPSG8857.zip/content` |
| direction | EU | `GRITv1.0_drainage_direction_EU_EPSG8857.zip` | 3,085,015,522 | `md5:a532b7d27b3ba99f0961e3bee2e28708` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_EU_EPSG8857.zip/content` |
| area | EU | `GRITv1.0_drainage_area_EU_EPSG8857.zip` | 13,258,415,974 | `md5:8585f7572a6263ec99e607e5e9724e82` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_EU_EPSG8857.zip/content` |
| direction | NA | `GRITv1.0_drainage_direction_NA_EPSG8857.zip` | 5,598,852,266 | `md5:a0ccd66476190a9e2ce14e9f032c8c78` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_NA_EPSG8857.zip/content` |
| area | NA | `GRITv1.0_drainage_area_NA_EPSG8857.zip` | 22,980,382,731 | `md5:b456ea5986e0dea57201b08b4c399f1e` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_NA_EPSG8857.zip/content` |
| direction | SA | `GRITv1.0_drainage_direction_SA_EPSG8857.zip` | 5,148,849,750 | `md5:11897fef9d74893382b01deba153d9ed` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_SA_EPSG8857.zip/content` |
| area | SA | `GRITv1.0_drainage_area_SA_EPSG8857.zip` | 17,780,974,206 | `md5:86fe79e011be6b79e30b2cc6a7665ed7` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_SA_EPSG8857.zip/content` |
| direction | SI | `GRITv1.0_drainage_direction_SI_EPSG8857.zip` | 3,222,668,815 | `md5:bfabfb49c51ed4f4f1dbd638ff865122` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_SI_EPSG8857.zip/content` |
| area | SI | `GRITv1.0_drainage_area_SI_EPSG8857.zip` | 15,702,689,750 | `md5:c2b92d8cf093de537d18e82a0c9ce494` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_SI_EPSG8857.zip/content` |
| direction | SP | `GRITv1.0_drainage_direction_SP_EPSG8857.zip` | 2,937,435,026 | `md5:c8c6c274e9375bf945b37699b7c5734a` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_direction_SP_EPSG8857.zip/content` |
| area | SP | `GRITv1.0_drainage_area_SP_EPSG8857.zip` | 11,394,791,809 | `md5:e86f40f14445707a46250e427cca1aec` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_drainage_area_SP_EPSG8857.zip/content` |
| index | GLOBAL | `GRITv1.0_raster_tile_index_GLOBAL_EPSG8857.gpkg` | 970,752 | `md5:7932905224f440b2d076cc104cf097a1` | `https://zenodo.org/api/records/15715535/files/GRITv1.0_raster_tile_index_GLOBAL_EPSG8857.gpkg/content` |

The direction family totals 35,005,693,158 API-declared bytes. The area
family totals 138,962,053,251 API-declared bytes.

### EU vector acquisition

The EU vector smoke build consumed these four API-discovered members:

| Filename | Bytes | Checksum | Content URL |
| --- | ---: | --- | --- |
| `GRITv1.0_segments_EU_EPSG4326.gpkg.zip` | 258,601,790 | `md5:590f3c91556a766a23d9f1eeac823736` | `https://zenodo.org/api/records/17435232/files/GRITv1.0_segments_EU_EPSG4326.gpkg.zip/content` |
| `GRITv1.0_segment_catchments_EU_EPSG4326.gpkg.zip` | 630,403,558 | `md5:c20da145d78eda3bfddea9721393f0a6` | `https://zenodo.org/api/records/17435232/files/GRITv1.0_segment_catchments_EU_EPSG4326.gpkg.zip/content` |
| `GRITv1.0_reaches_EU_EPSG4326.gpkg.zip` | 678,878,639 | `md5:416d148d9e481e53d14addeb629884f4` | `https://zenodo.org/api/records/17435232/files/GRITv1.0_reaches_EU_EPSG4326.gpkg.zip/content` |
| `GRITv1.0_reach_catchments_EU_EPSG4326.gpkg.zip` | 1,496,372,735 | `md5:7ad6a8854e80103d2ec96c23fbcabe19` | `https://zenodo.org/api/records/17435232/files/GRITv1.0_reach_catchments_EU_EPSG4326.gpkg.zip/content` |

The four members total 3,064,256,722 bytes. Acquisition downloaded each
content URL separately, checked the API byte count and MD5, and constructed
`/private/tmp/grit-d8-m2-scratch/archives/17435232-eu-outer.zip`. The local
outer archive contains exactly those four top-level members with `ZIP_STORED`.
It is 3,064,257,462 bytes and has local MD5
`e0a11cd2aa39c3c56aea6b972674e9f0`. Its `unzip -t` check passed.

### Grid evidence

Run 4 evaluated all seven regions and both product families. Each source is a
single-band EPSG:8857 tile on a common unrotated 30 m pixel basis. Fractional
column and row offsets are integral within `1e-6`. The maximum measured
residual is 0. The pinned adapter's stricter `1e-8` offset check also reports
0. The tile index contains 2,039 features, each product contains 2,039 tiles,
and index coverage matches raster coverage. Direction-area coverage
differences total zero. Positive-area source-window overlaps within each
validated family total zero, and positive-area cross-region overlaps total
zero.

The region counts are AF 397, AS 366, EU 209, NA 356, SA 246, SI 200, and SP
265. These sum to 2,039 tiles per product.

Vision amendment E1, dated 2026-07-21, records the published direction
encoding. Direction tiles use `uint8`; valid data codes are 0 through 8; code
0 is terminal; tagged family nodata is 255. The M2-S0 normative note in
`spec/aux/d8_raster/v2.md` defines this `grass` and `uint8` contract. A fully
covered tag-less direction tile is accepted after a full scan proves every
cell belongs to the valid code domain.

Vision amendment E2, dated 2026-07-21, records the published accumulation
encoding. Area tiles use `float32` values in km2 and contain real sub-km2
values. Tagged tiles use NaN nodata. Tag presence is mixed within each region.
A fully covered tag-less area tile is accepted after a NaN-aware full scan
proves every cell differs from the family's common tagged NaN nodata. M2-S0b
implemented this NaN-aware accumulation validation at pinned ref
`e0269241956f5d7e4b93ef7bac1813664266141f`.

Run 3 global direction evidence remains at
`evidence/{AF,AS,EU,NA,SA,SI,SP}-direction-complete.json`,
`logs/{AF,AS,EU,NA,SA,SI,SP}-direction-validator.log`, and
`logs/m2-s1-executor-run3.log`. New-ref pinned-validator acceptance for the EU
direction family is at `evidence/EU-direction-current.json` and
`logs/EU-direction-validator-current.log`. Acceptance for every real area
family is at `evidence/{AF,AS,EU,NA,SA,SI,SP}-area-complete.json` and
`logs/{AF,AS,EU,NA,SA,SI,SP}-area-validator-current.log`.

The complete run-4 PASS is recorded in `evidence/grid-premise.md`,
`evidence/grid-premise.json`, `evidence/raster-headers.tsv`,
`evidence/raster-headers.json`, `logs/grid-analysis-current.log`, and
`logs/m2-s1-executor-run4.log`, all rooted at
`/private/tmp/grit-d8-m2-scratch`.

### Planetary dimensions and storage planning

The planetary union transform is
`[30.0, 0.0, -15000000.0, 0.0, -30.0, 8400000.0]`. Its projected bounds are
`[-15000000.0, -6600000.0, 17100000.0, 8400000.0]`. The resulting dimensions
are 1,070,000 columns by 500,000 rows, for 535,000,000,000 cells.

The raw direction estimate is
`535,000,000,000 cells * 1 byte = 535,000,000,000 bytes`. The raw area
estimate is `535,000,000,000 cells * 4 bytes = 2,140,000,000,000 bytes`. The
compressed pair planning estimate is
`35,005,693,158 + 138,962,053,251 = 173,967,746,409 bytes`. This estimate uses
the sum of published compressed regional inputs as a storage proxy. COG
tiling, sparse union coverage, compression settings, and BigTIFF metadata
influence the realized output size.

### EU smoke build

The verified outer archive was supplied to these four staged invocations:

```bash
/usr/bin/time -p uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py \
  --root /private/tmp/grit-d8-m2-scratch \
  --outer-archive "$VECTOR_OUTER_ARCHIVE" \
  stage1 --regions EU

/usr/bin/time -p uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py \
  --root /private/tmp/grit-d8-m2-scratch \
  --outer-archive "$VECTOR_OUTER_ARCHIVE" \
  stage2 --regions EU

/usr/bin/time -p uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py \
  --root /private/tmp/grit-d8-m2-scratch \
  --outer-archive "$VECTOR_OUTER_ARCHIVE" \
  phase25 --regions EU

/usr/bin/time -p uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py \
  --root /private/tmp/grit-d8-m2-scratch \
  --outer-archive "$VECTOR_OUTER_ARCHIVE" \
  write --regions EU
```

All four invocations exited 0. Timings and measured scratch use were:

| Stage | Real seconds | User seconds | Sys seconds | Scratch KiB after stage |
| --- | ---: | ---: | ---: | ---: |
| `stage1` | 73.24 | 63.54 | 6.48 | 190,273,280 |
| `stage2` | 3.03 | 1.79 | 0.40 | 190,310,368 |
| `phase25` | 27.45 | 27.23 | 0.46 | 190,357,636 |
| `write` | 20.78 | 19.81 | 12.09 | 194,306,504 |

The output is `/private/tmp/grit-d8-m2-scratch/grit-hfx-eu-smoke`:

| Path | Bytes | MD5 |
| --- | ---: | --- |
| `manifest.json` | 1,209 | `61d12b97a16124cc3335ba7eea054c7e` |
| `catchments.parquet` | 3,000,858,709 | `6add6a4145f8b2a66c2dfe57528a502d` |
| `graph.parquet` | 64,970,178 | `20ca63a718915ec6358dfec92ea5b0b5` |
| `aux/snap_segments.parquet` | 340,130,163 | `de9185d739a483357cdcb87bc000a785` |
| `aux/snap_reaches.parquet` | 625,053,859 | `55c5fd4fbe512173dad20f13fcae2352` |

Final measured scratch use was 194,306,512 KiB, or 198,969,868,288 bytes.
The evidence for acquisition, ZIP metadata, budget checks, stage timings, and
output checksums remains under `/private/tmp/grit-d8-m2-scratch/evidence` and
`/private/tmp/grit-d8-m2-scratch/logs` for M2-S2.

## Decision

Retain the one-planetary-entry premise as the Proposed design for the M2-S2 EU
raster pilot. The planetary pair uses the measured EPSG:8857 30 m union grid,
the E1 direction encoding, and the E2 accumulation encoding. BigTIFF planning
uses the measured raw logical sizes and the 173,967,746,409-byte compressed
input proxy.

M2-S2 performs raster attachment and strict validation against the preserved
EU vector smoke dataset. Its pilot evidence determines acceptance of this
Proposed design.

## Consequences

- A single auxiliary entry gives consumers one declared planetary grid and
  one direction-area artifact pair.
- BigTIFF-capable construction and ample temporary storage are required by the
  535,000,000,000-cell logical grid.
- The verified 14 raster archives, global tile index, four EU vector members,
  local outer archive, inventories, run-4 evidence, and EU vector output remain
  in the shared scratch directory for M2-S2 and later milestone steps.
- M2-S2 supplies real EU raster-pilot attachment, strict dataset validation,
  artifact sizing, and operational evidence for the final design status.
