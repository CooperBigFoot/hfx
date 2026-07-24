# GRIT 2.0.0: HFX Dataset

This dataset is an HFX v0.3.0 compilation of the Global River Topology (GRIT)
vector datasets, with segment (`level=0`) and reach (`level=1`) drainage
units.

HFX spec and toolkit: https://github.com/CooperBigFoot/hfx

## Manifest facts

| Field | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | grit |
| `fabric_version` | 1.0.0 |
| `adapter_version` | grit-global-2.1.0 |
| `unit_count` | 22,337,300 |
| Total size | 299,117,889,306 bytes |
| CRS | EPSG:4326 |
| `topology` | dag |
| Auxiliaries | two `hfx.aux.snap.v2` snap indexes (segment-stems, reach-stems) and exactly one `hfx.aux.d8_raster.v2` planetary D8 entry |

The `catchments.parquet` and both snap parquet files carry the GeoParquet 1.1
`bbox` covering struct (`xmin`/`ymin`/`xmax`/`ymax`) with per-row-group leaf
statistics, enabling spatial-range pushdown.

The planetary `hfx.aux.d8_raster.v2` entry maps
`flow_dir=aux/d8/flow_dir.tif` and `flow_acc=aux/d8/flow_acc.tif`. Its metadata
object is exactly
`{"crs": "EPSG:8857", "flow_dir_encoding": "grass", "flow_acc_units": "km2"}`.
Both COGs share an `EPSG:8857` grid of `1,070,000 x 500,000` with transform
`[30.0, 0.0, -15000000.0, 0.0, -30.0, 8400000.0]`. The direction COG has one
`uint8` band with nodata `255`; the accumulation COG has one `float32` band
with NaN nodata. Both COG validators returned `valid=true`, `errors=[]`, and
`warnings=[]`.

## Objects

Base URL: https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/

- `manifest.json`
- `catchments.parquet`
- `graph.parquet`
- `aux/snap_segments.parquet`
- `aux/snap_reaches.parquet`
- `aux/d8/flow_dir.tif`
- `aux/d8/flow_acc.tif`
- `NOTICE`, `CITATION.txt`, `README.md` (attribution objects)

| Object | Bytes | SHA-256 |
| --- | ---: | --- |
| `manifest.json` | `1,426` | `02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c` |
| `aux/d8/flow_dir.tif` | `50,686,516,478` | `eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656` |
| `aux/d8/flow_acc.tif` | `205,069,870,081` | `30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b` |
| `catchments.parquet` | `32,508,030,908` | `50c987343181af8a848170cf121571e7ae815ac491b93bcf9cc07a04a9e12c59` |
| `graph.parquet` | `699,720,490` | `8f61a64fb6746213638655053118eb47c939a4f89796f4a1ef017ab0fd923e81` |
| `aux/snap_segments.parquet` | `3,674,757,248` | `cab39a2be4333cdd8e9a02b78186fa6f6ff3d55c761e670e7104605eeab4dda3` |
| `aux/snap_reaches.parquet` | `6,478,991,001` | `afd994adb2fbfdf25e09dc51b4e1441eb74e34ed5647a6a5b3b4e2a67dc9dfc3` |

The normalized post-attach identity gate matched all four vector byte counts
and SHA-256 values exactly. This records that raster attachment left the vector
artifacts byte-identical.

## License and citation

License: CC BY-NC 4.0
(https://creativecommons.org/licenses/by-nc/4.0/), inherited from both source
datasets. The combined derived HFX dataset is available for NonCommercial use
only.

Vector source dataset:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) vector datasets”. Zenodo. doi:10.5281/zenodo.17435232.

Zenodo record 17435232. DOI: 10.5281/zenodo.17435232
(https://doi.org/10.5281/zenodo.17435232)

Raster source dataset, including the drainage_direction and width-partitioned
drainage_area rasters used by this compilation:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT) raster datasets”. Zenodo. doi:10.5281/zenodo.15715535.

Zenodo record 15715535. DOI: 10.5281/zenodo.15715535
(https://doi.org/10.5281/zenodo.15715535)

Related paper:

> Wortmann, M. et al. (2025) “Global River Topology (GRIT): A Bifurcating River Hydrography”. Water Resources Research, 61, e2024WR038308. doi:10.1029/2024WR038308.

DOI: 10.1029/2024WR038308
(https://doi.org/10.1029/2024WR038308)

See `CITATION.txt` in this directory for the plain-text and BibTeX entries.

## Validating a download

```sh
git clone https://github.com/CooperBigFoot/hfx && cd hfx && cargo run -p hfx-cli -- /path/to/downloaded/grit-hfx-v0.3.0 --strict
```

Alternatively, clone this repository, run `cargo install --path crates/hfx-cli`,
then run `hfx /path/to/downloaded/grit-hfx-v0.3.0 --strict`.

Planetary artifact validation record from the validated build:

```bash
cargo build -p hfx-cli
/usr/bin/time -p ./target/debug/hfx /mnt/hfx/scratch/grit-hfx-global --strict
```

Both commands exited `0`. The complete strict report was:

```text
0 error(s), 0 warning(s), 0 info(s)
Result: VALID
```

## DAG `up_area_km2` semantics

Choice: partitioned (option a). Each segment's `up_area_km2` reflects the
source-area share routed through that segment.

Algorithm: per-segment chain anchor. Segment rows use GRIT
`drainage_area_out` directly. Reach rows are computed by anchoring each
parent segment's outlet reach to the segment `drainage_area_out`, then
walking upstream within that segment and subtracting each downstream reach's
local `area_km2`.

Consumer caveat: in DAG split-rejoin geometry, the sum of `up_area_km2` over
a flow set is not the watershed area. Consumers must use the graph plus
`level=1` reaches for true watershed accumulation.

## Known data caveats

44 reach rows have `up_area_km2=NULL`, spread across 17 segments where the
chain-anchor algorithm could not resolve the outlet. These are anomalies in
the GRIT v1.0 source topology, not defects of the HFX encoding.
`has_up_area` remains true; nulls are permitted per HFX spec. They
represent <0.001% of rows. See `adapters/grit-v2/build_adapter.py` in the
HFX repository for the detection rule.

Fallback segment IDs:

- AF: 140004152, 390037110, 480000538
- AS: 180020690
- EU: 230045414
- NA: 190003639, 220083647, 300005125, 360109293, 410046446
- SA: 9627, 120012564
- SI: 110020043, 110020254, 430009780, 580009350
- SP: 90000652

## Provenance

- Vector source: GRIT v1.0 vector datasets, Zenodo record 17435232
  (https://doi.org/10.5281/zenodo.17435232)
- Raster source: GRIT v1.0 raster datasets, Zenodo record 15715535
  (https://doi.org/10.5281/zenodo.15715535); this compilation uses
  drainage_direction and width-partitioned drainage_area
- Paper: Wortmann et al. 2025, Water Resources Research,
  doi:10.1029/2024WR038308 (https://doi.org/10.1029/2024WR038308)
- Adapter: grit-v2 (`adapters/grit-v2/build_adapter.py`, FORMAT_VERSION 0.3.0)
- HFX spec version: 0.3.0
- Bbox: planetary [-180, -90, 180, 90]
- Row count: 22,337,300 catchments across 2 levels

## Hosting

Upstream Tech sponsors the hosting infrastructure for this dataset
(infrastructure sponsor only). Upstream Tech is not the data publisher or
vendor.

## Human-gated GRIT D8 manifest amendment

**Status: HUMAN-GATED, UNFIRED. Agents never fire this action.**

This action amends the live Cloudflare R2 prefix `grit/hfx-v0.3.0/` in place.
Its public base URL is
https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/. It does not
create a sibling dataset prefix. The non-manifest objects may be staged before
publication because the live manifest references none of them.

`manifest.json` is the atomic reader-visible switch and is uploaded last.
Only a human with live `aws --profile upstream-r2` credentials may fire the
action. The human first stages the five non-manifest objects with:

```bash
bash scripts/upload-r2-grit-d8.sh --execute --profile upstream-r2 --staging /absolute/path/to/grit-hfx-v0.3.0
```

This phase proceeds only after the human types
`STAGE GRIT D8 OBJECTS`.

After verifying that staging succeeded and every pre-fire check below remains
green, the human publishes the manifest with:

```bash
bash scripts/upload-r2-grit-d8.sh --execute --publish-manifest --profile upstream-r2 --staging /absolute/path/to/grit-hfx-v0.3.0
```

This phase proceeds only after the human types
`PUBLISH GRIT D8 MANIFEST AFTER TICKET 45 RELEASE`. Agents never pass
`--execute` or `--publish-manifest`, never provide either confirmation, and
never make AWS calls.

The deterministic preview of the exact operation sequence is recorded in
`manifest-amendment-dry-run.txt`. It contains the offline `--self-test` result
and a normalized `--dry-run` over exactly the six allow-listed paths. The
preview makes zero AWS calls, lists `manifest.json` last with its gated
annotation, and reports `6 operations; zero uploads.`

### Ticket #45 reader gate: SATISFIED

The required v2-capable reader is live and verified:

- tag `pourpoint-v0.2.0` at commit
  `b7e9d990beb34aaf7359bb98482b5fab11268abc` on pourpoint `main`
- release:
  https://github.com/CooperBigFoot/pourpoint/releases/tag/pourpoint-v0.2.0
- workflow run `30082859519`: conclusion `success`; all six build jobs green;
  `Publish to PyPI` success; `Publish to TestPyPI` skipped (correct for a
  non-rc tag)
- PyPI latest = `0.2.0` with five wheels
  (`macosx_11_0_arm64`, `macosx_11_0_x86_64`,
  `manylinux_2_28_aarch64`, `manylinux_2_28_x86_64`, `win_amd64`; all
  `cp39-abi3`) plus sdist
- fresh-venv `pip install pourpoint==0.2.0`: import succeeds,
  `pourpoint.__version__ == "0.2.0"`, bundled `_data` present (26 GDAL files,
  16 PROJ files, `proj.db` exists)

A human can rerun the read-only release, tag, package, and isolated-install
checks:

```bash
gh run view 30082859519 \
  --repo CooperBigFoot/pourpoint \
  --json databaseId,conclusion,jobs,url

test "$(
  git ls-remote --tags https://github.com/CooperBigFoot/pourpoint.git \
    refs/tags/pourpoint-v0.2.0 |
    awk '{print $1}'
)" = "b7e9d990beb34aaf7359bb98482b5fab11268abc"

curl -fsS https://pypi.org/pypi/pourpoint/json |
python3 -c '
import json
import sys

payload = json.load(sys.stdin)
assert payload["info"]["version"] == "0.2.0"
files = payload["releases"]["0.2.0"]
wheels = sorted(item["filename"] for item in files if item["packagetype"] == "bdist_wheel")
sdists = [item["filename"] for item in files if item["packagetype"] == "sdist"]
assert len(wheels) == 5, wheels
assert len(sdists) == 1, sdists
expected = [
    "macosx_11_0_arm64",
    "macosx_11_0_x86_64",
    "manylinux_2_28_aarch64",
    "manylinux_2_28_x86_64",
    "win_amd64",
]
for wheel, platform in zip(wheels, expected, strict=True):
    assert "cp39-abi3" in wheel, wheel
    assert platform in wheel, (wheel, platform)
'

CHECK_ROOT="$(mktemp -d /tmp/pourpoint-0.2.0-install.XXXXXX)"
python3 -m venv "$CHECK_ROOT/venv"
"$CHECK_ROOT/venv/bin/python" -m pip install pourpoint==0.2.0
"$CHECK_ROOT/venv/bin/python" -c '
from importlib.resources import files
import pourpoint

data = files("pourpoint").joinpath("_data")
gdal = [entry for entry in data.joinpath("gdal").iterdir() if entry.is_file()]
proj = [entry for entry in data.joinpath("proj").iterdir() if entry.is_file()]
assert pourpoint.__version__ == "0.2.0"
assert len(gdal) == 26, len(gdal)
assert len(proj) == 16, len(proj)
assert data.joinpath("proj", "proj.db").is_file()
'
```

Before firing, rerun one explicit v2-read proof from the HFX repository
worktree. The fixture
`conformance/valid/tiny-with-aux-d8-projected-grass/` is tracked and declares
`hfx.aux.d8_raster.v2` with the same `EPSG:8857`, `grass`, and `km2` metadata
as the hosted GRIT entry:

```bash
V2_CHECK_ROOT="$(mktemp -d /tmp/pourpoint-0.2.0-v2-read.XXXXXX)"
python3 -m venv "$V2_CHECK_ROOT/venv"
"$V2_CHECK_ROOT/venv/bin/python" -m pip install pourpoint==0.2.0
HFX_V2_FIXTURE="$PWD/conformance/valid/tiny-with-aux-d8-projected-grass" \
"$V2_CHECK_ROOT/venv/bin/python" - <<'PY'
import math
import os
from pathlib import Path

import pourpoint

fixture = Path(os.environ["HFX_V2_FIXTURE"])
engine = pourpoint.Engine(
    str(fixture),
    snap_radius=1000.0,
    snap_threshold=500,
)
result = engine.delineate(
    lat=0.4166666666666667,
    lon=0.9833333333333333,
)
assert result.terminal_unit_id == 4
assert sorted(result.upstream_unit_ids) == [2, 3, 4]
assert result.refined_outlet is not None
assert math.isclose(
    result.refined_outlet[0],
    0.9864447364836884,
    rel_tol=0.0,
    abs_tol=0.000001,
)
assert math.isclose(
    result.refined_outlet[1],
    0.4163847890060862,
    rel_tol=0.0,
    abs_tol=0.000001,
)
assert result.geometry_wkb
print("pourpoint 0.2.0 reads hfx.aux.d8_raster.v2: refinement applied")
PY
```

The expected final line is:

```text
pourpoint 0.2.0 reads hfx.aux.d8_raster.v2: refinement applied
```

### Human pre-fire checklist

1. Confirm the ticket `#45` evidence and every read-only check above.
2. Use a real staging directory containing exactly these six files and no
   others:
   `aux/d8/flow_dir.tif`, `aux/d8/flow_acc.tif`, `NOTICE`, `CITATION.txt`,
   `README.md`, and `manifest.json`.
3. Confirm the amended manifest contains exactly one
   `hfx.aux.d8_raster.v2` entry. It must map
   `flow_dir=aux/d8/flow_dir.tif` and
   `flow_acc=aux/d8/flow_acc.tif`, with metadata exactly
   `{"crs": "EPSG:8857", "flow_dir_encoding": "grass", "flow_acc_units": "km2"}`.
4. Confirm the real staged objects match the recorded byte counts and SHA-256
   identities in this README. In particular,
   `aux/d8/flow_dir.tif` is `50,686,516,478` bytes with SHA-256
   `eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656`,
   and `aux/d8/flow_acc.tif` is `205,069,870,081` bytes with SHA-256
   `30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b`.
5. Read `manifest-amendment-dry-run.txt`. Confirm six operations, no `SKIP:`
   lines, C-locale ordering of `CITATION.txt`, `NOTICE`, `README.md`,
   `aux/d8/flow_acc.tif`, `aux/d8/flow_dir.tif`, and then `manifest.json`
   last with its `GATED` annotation. Confirm the trailer is
   `6 operations; zero uploads.`
6. Run `bash scripts/upload-r2-grit-d8.sh --self-test` and
   `LC_ALL=C bash scripts/upload-r2-grit-d8.sh --dry-run --staging
   /absolute/path/to/grit-hfx-v0.3.0`. Confirm their normalized output matches
   `manifest-amendment-dry-run.txt`.
7. Confirm live `aws --profile upstream-r2` credentials are available to the
   human operator. Review both typed confirmation phrases in the script before
   firing. Stage the five non-manifest objects first. Publish `manifest.json`
   only after the staged objects and reader proof are verified.

### Read-only post-fire assertions

Use the public HTTPS endpoint for post-fire checks. These commands make no AWS
changes.

First, assert that the public manifest contains exactly one D8 v2 entry with
the required paths and exact metadata:

```bash
BASE_URL="https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0"
curl -fsS "$BASE_URL/manifest.json" |
jq -e '
  [.auxiliary[] | select(.schema == "hfx.aux.d8_raster.v2")] as $d8 |
  ($d8 | length) == 1 and
  $d8[0].artifacts == {
    "flow_dir": "aux/d8/flow_dir.tif",
    "flow_acc": "aux/d8/flow_acc.tif"
  } and
  $d8[0].metadata == {
    "crs": "EPSG:8857",
    "flow_dir_encoding": "grass",
    "flow_acc_units": "km2"
  }
'
```

Assert that every hosted object is publicly readable:

```bash
BASE_URL="https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0"
for object in \
  manifest.json \
  catchments.parquet \
  graph.parquet \
  aux/snap_segments.parquet \
  aux/snap_reaches.parquet \
  aux/d8/flow_dir.tif \
  aux/d8/flow_acc.tif \
  NOTICE \
  CITATION.txt \
  README.md
do
  curl -fsSI "$BASE_URL/$object" >/dev/null
done
```

Download the published dataset, then assert the recorded byte counts and
SHA-256 identities:

```bash
BASE_URL="https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0"
DOWNLOAD_ROOT="/absolute/path/to/downloaded/grit-hfx-v0.3.0"
mkdir -p "$DOWNLOAD_ROOT/aux/d8"
mkdir -p "$DOWNLOAD_ROOT/aux"
for object in \
  manifest.json \
  catchments.parquet \
  graph.parquet \
  aux/snap_segments.parquet \
  aux/snap_reaches.parquet \
  aux/d8/flow_dir.tif \
  aux/d8/flow_acc.tif \
  NOTICE \
  CITATION.txt \
  README.md
do
  mkdir -p "$DOWNLOAD_ROOT/$(dirname "$object")"
  curl -fL "$BASE_URL/$object" -o "$DOWNLOAD_ROOT/$object"
done

DOWNLOAD_ROOT="$DOWNLOAD_ROOT" python3 - <<'PY'
import hashlib
import os
from pathlib import Path

root = Path(os.environ["DOWNLOAD_ROOT"])
expected = {
    "manifest.json": (
        1426,
        "02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c",
    ),
    "aux/d8/flow_dir.tif": (
        50686516478,
        "eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656",
    ),
    "aux/d8/flow_acc.tif": (
        205069870081,
        "30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b",
    ),
    "catchments.parquet": (
        32508030908,
        "50c987343181af8a848170cf121571e7ae815ac491b93bcf9cc07a04a9e12c59",
    ),
    "graph.parquet": (
        699720490,
        "8f61a64fb6746213638655053118eb47c939a4f89796f4a1ef017ab0fd923e81",
    ),
    "aux/snap_segments.parquet": (
        3674757248,
        "cab39a2be4333cdd8e9a02b78186fa6f6ff3d55c761e670e7104605eeab4dda3",
    ),
    "aux/snap_reaches.parquet": (
        6478991001,
        "afd994adb2fbfdf25e09dc51b4e1441eb74e34ed5647a6a5b3b4e2a67dc9dfc3",
    ),
}
for relative, (size, digest) in expected.items():
    path = root / relative
    assert path.stat().st_size == size, (relative, path.stat().st_size, size)
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            hasher.update(chunk)
    assert hasher.hexdigest() == digest, (relative, hasher.hexdigest(), digest)
print("recorded byte counts and SHA-256 identities match")
PY
```

Finally, run strict HFX validation against the complete downloaded directory:

```bash
cargo run -p hfx-cli -- "$DOWNLOAD_ROOT" --strict
```

The validator must exit `0` and report:

```text
0 error(s), 0 warning(s), 0 info(s)
Result: VALID
```
