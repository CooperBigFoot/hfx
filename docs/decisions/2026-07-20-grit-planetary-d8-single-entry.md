# GRIT planetary D8 rasters use one entry

**Status:** Accepted

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

## EU raster pilot evidence, 2026-07-21

The pilot ran from pinned base ref
`13a91a4c921140a97f9e13687ba3a19e5a4f419f`.

### Verified inputs and invocation

The direction archive at
`/private/tmp/grit-d8-m2-scratch/archives/GRITv1.0_drainage_direction_EU_EPSG8857.zip`
was 3,085,015,522 bytes with MD5
`a532b7d27b3ba99f0961e3bee2e28708`. The area archive at
`/private/tmp/grit-d8-m2-scratch/archives/GRITv1.0_drainage_area_EU_EPSG8857.zip`
was 13,258,415,974 bytes with MD5
`8585f7572a6263ec99e607e5e9724e82`. Both values matched unique rows in the
retained inventory. Exact ZIP central-directory size and CRC-32 checks passed
for all 209 extracted members in each archive.

The pilot used one raster invocation, which exited 0:

```bash
/usr/bin/time -p uv run --project adapters/grit-v2 python adapters/grit-v2/build_adapter.py raster \
  --flow-dir-archive /private/tmp/grit-d8-m2-scratch/archives/GRITv1.0_drainage_direction_EU_EPSG8857.zip \
  --flow-acc-archive /private/tmp/grit-d8-m2-scratch/archives/GRITv1.0_drainage_area_EU_EPSG8857.zip \
  --dataset-dir /private/tmp/grit-d8-m2-scratch/grit-hfx-eu-smoke \
  --work-dir /private/tmp/grit-d8-m2-scratch/m2-s2-raster-work
```

The pre-run SHA-256 values were:

| Artifact | SHA-256 |
| --- | --- |
| `manifest.json` | `0ba0df1e4089f957d6b8d1447578e6a56219249493fa5f863765f2d25339ec72` |
| `catchments.parquet` | `7765451b8aafbf5f36ed0aa4a30a195a06689d8c748650180bd482be462d3aca` |
| `graph.parquet` | `5883ed322be5500e90ee91229bae32a2343f515b4eb3ca0ecbc40997d5fb23fc` |
| `aux/snap_segments.parquet` | `df878f2dcdc2b86e09cf8f3654f8ad5755f29bd7925ef231394993db86d7ccf5` |
| `aux/snap_reaches.parquet` | `a24adc6c68c63ba4fbbaad03d527a3c14eed08778d21b3c584bb6f9c020ce6b9` |

The amended manifest SHA-256 was
`c7912468d2c53db7cb00d29caf45ec9a4f1fa5571d86c4ccf46ff59c008675a0`.
The direction COG SHA-256 was
`e55915130e7307f75ddbbb0237adf0b11d11f550126471bb5a93f06269a139f7`.
The accumulation COG SHA-256 was
`ef1a6664f6135dee1f595b6cb2597fb58ac012307039b1d836e647114bf4f47c`.
The four vector SHA-256 lines were byte-identical before and after raster
attachment.

### COG, grid, manifest, and cell identity

`flow_dir.tif` contained one `uint8` band with nodata 255.
`flow_acc.tif` contained one `float32` band with NaN nodata, and the
NaN-aware nodata assertion passed. Both COG checks returned `valid=true`,
`errors=[]`, and `warnings=[]`. Both rasters resolved to EPSG:8857 and shared
width 300,000, height 160,000, bounds
`[-4500000.0, 3600000.0, 4500000.0, 8400000.0]`, and transform
`[30.0, 0.0, -4500000.0, 0.0, -30.0, 8400000.0]`. The pair grid comparison
passed for CRS, width, height, transform, and bounds.

The manifest contained exactly one `hfx.aux.d8_raster.v2` entry. Its
artifacts were exactly `flow_dir=aux/d8/flow_dir.tif` and
`flow_acc=aux/d8/flow_acc.tif`. Its metadata was exactly
`crs=EPSG:8857`, `flow_dir_encoding=grass`, and `flow_acc_units=km2`. The
adapter version was `grit-global-2.1.0`.

The fixed source tile was `E000000000N004800000`. The source-local window was
`Window(col_off=6128, row_off=856, width=16, height=16)`, and the EU COG
window was `Window(col_off=156128, row_off=110856, width=16, height=16)`.
Their window transforms were identical. Exact direction array comparison
passed. The source window contained nine direction zeros. Source column 6132
and row 860, window column 4 and row 4, equaled 0 in source and output while
source nodata equaled 255, proving genuine terminal data. Exact accumulation
NaN-mask and finite `float32` comparisons passed. The source window contained
83 NaNs, with finite minimum `0.0008999999845400453` and finite maximum
`0.14669999480247498`.

### Strict validation, timing, and sizing

The validator commands were:

```bash
cargo build -p hfx-cli
/usr/bin/time -p ./target/debug/hfx /private/tmp/grit-d8-m2-scratch/grit-hfx-eu-smoke --strict
```

The build exited 0. The strict command exited 0 and produced this complete
report:

```text
0 error(s), 0 warning(s), 0 info(s)
Result: VALID
```

Raster construction used 5,590.95 real, 5,392.95 user, and 91.33 sys
seconds. Strict validation used 57.66 real, 55.42 user, and 1.47 sys seconds.

| Measure | Direction | Area | Pair |
| --- | ---: | ---: | ---: |
| Source archive bytes | 3,085,015,522 | 13,258,415,974 | 16,343,431,496 |
| ZIP member bytes | 3,086,726,059 | 13,267,052,711 | 16,353,778,770 |
| Output COG bytes | 4,596,910,304 | 19,616,144,826 | 24,213,055,130 |
| Logical EU bytes | 48,000,000,000 | 192,000,000,000 | 240,000,000,000 |
| Source ZIP ratio, member/archive | 1.0005544663 | 1.0006514154 | n/a |
| COG logical compression, logical/COG | 10.4417960816 | 9.7878559576 | 9.9120081589 |
| Archive-to-COG, archive/COG | 0.6711063123 | 0.6758930509 | 0.6749842764 |
| COG-to-archive, COG/archive | 1.4900768801 | 1.4795240144 | 1.4815159923 |
| EU output bytes per tile | 21,994,786.1435 | 93,857,152.2775 | 115,851,938.4211 |
| 2,039-tile linear estimate bytes | 44,847,368,946.6794 | 191,374,733,493.8469 | 236,222,102,440.5263 |

The 2,039-tile scale factor was exactly `2039 / 209 = 9.75598086124402`.
The linear raster-build estimate was 54,545.201196172246 seconds. The measured
estimates complement the published 173,967,746,409-byte compressed archive
proxy and planetary logical raw sizes of 535,000,000,000 direction bytes and
2,140,000,000,000 area bytes.

Pre-run scratch was 198,754,492,416 total bytes and 41,130,177,503 counted
working-set bytes. The sampled peak was 489,237,876,736 total bytes and
331,613,561,823 counted working-set bytes. Post-raster scratch was
239,341,142,016 total bytes and 81,716,827,103 counted working-set bytes.
Post-cleanup scratch was 194,728,677,376 total bytes and 37,104,362,463
counted working-set bytes.

### Accepted construction and cleanup

The passing real-source cell checks, clean COG checks, exact manifest contract,
unchanged vectors, and strict `VALID` result accept one planetary D8 entry.

The VRT streaming architecture introduced by delta D4 and PR #99 uses
extracted source members as inputs to tiny per-family VRT mosaics. The measured
direction and accumulation VRTs were 71,929 and 70,887 bytes. Each VRT streams
into a staged compressed `translated.tif` in its family temporary folder. The
validated staged COG moves to its final dataset path. Planetary construction
needs no dense 535 GB direction or 2.14 TB accumulation mosaic temporary. The
planetary sizing implication is a compressed-output and transient-space plan,
instead of allocation for those dense mosaics. GDAL's planetary destination
free-space estimate may require a deliberate, documented decision during M3
planning.

Cleanup deleted
`/private/tmp/grit-d8-m2-scratch/grit-hfx-eu-smoke` and
`/private/tmp/grit-d8-m2-scratch/m2-s2-raster-work`. The verified 14 raster
archives, four vector member files, outer vector ZIP, API inventories, global
tile index, and all M2 evidence and logs remain available for M3.
