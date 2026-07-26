# GeoPandas Hilbert parity result

**Date:** 2026-07-24

**Ground-truth ref:** `a3e8a2b`

**Status:** PASS

## Purpose

TDX-Hydro basin compilation on the VM uses GeoPandas 1.1.3, while the adapter's local ordering and assembly proofs used GeoPandas 1.1.4. This result verifies GeoPandas-version parity on macOS: `geometry.centroid.hilbert_distance(total_bounds=[-180, -90, 180, 90])`, using GeoPandas' default level 16, emits byte-identical `(index, key)` pairs in the local macOS recreation of the VM-version stack, the same local stack with GeoPandas 1.1.4, and the adapter's locked macOS environment.

## Corpus

- Format: contiguous zero-based index, category, and explicit big-endian two-dimensional WKB hex.
- Fixed seed: `20260724`.
- Randomized geometries: `100000`.
- Total geometries: `100064`.
- Categories: `real-polygon`, `real-line`, `world-boundary`, `clamp-adjacent`, `hilbert-tie`, `tiny-valid`, and `randomized`.
- Coverage: irregular basin-like polygons, meandering reaches, exact and next-representable world edges, one-source-cell clamp-adjacent coordinates, two known centroid-tie groups, valid tiny geometries, and a domain-wide randomized batch.
- Corpus SHA-256: `0724dd957de82776740c62608aa26c3ef713163938250ca1055ca2366fdc8810`.

The corpus contains no timestamps or host data. It was serialized once and read unchanged by all three workers.

## VM-native confirmation hashes

- VM-confirm corpus SHA-256: `0724dd957de82776740c62608aa26c3ef713163938250ca1055ca2366fdc8810`.
- VM-confirm macOS pair SHA-256: `18a41718c615b1eebc86c430c560dae1edc4e0f7bbd8f4b7e9251219eb40144b`.

These are the two committed reference values consumed by `verify_geopandas_hilbert_parity.py vm-confirm`. The macOS pair hash is the common hash of all three byte-identical macOS pair files.

## Environments

| Label | Python | GeoPandas | NumPy | pandas | Shapely | PyProj | packaging | pyogrio |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `vm-stack-geopandas-1.1.3` | 3.12.11 | 1.1.3 | 2.4.6 | 3.0.3 | 2.1.2 | 3.7.2 | 26.2 | 0.12.1 |
| `vm-stack-geopandas-1.1.4` | 3.12.11 | 1.1.4 | 2.4.6 | 3.0.3 | 2.1.2 | 3.7.2 | 26.2 | 0.12.1 |
| `adapter-lock-geopandas-1.1.4` | 3.13.8 | 1.1.4 | 2.5.1 | 3.0.3 | 2.1.2 | 3.7.2 | 26.2 | 0.13.0 |

The first two environments hold Python and every GeoPandas runtime dependency constant and vary only GeoPandas. The third environment is the adapter lock used by the existing local proofs.

GeoPandas delegates centroid calculation to its Shapely-backed geometry array. Its Hilbert implementation imports NumPy and uses NumPy for bounds midpoint scaling, clipping, uint32 conversion, and bit encoding; the public method returns a pandas Series. PyProj, packaging, and pyogrio do not enter the numerical path but were pinned in the isolated environments to remove resolver variation.

## Commands

Top-level command, run from `adapters/tdx-hydro`:

```bash
uv run --python 3.13.8 python verify_geopandas_hilbert_parity.py
```

VM stack with GeoPandas 1.1.3:

```bash
/Users/nicolaslazaro/.local/bin/uv run --no-project --python 3.12.11 --with geopandas==1.1.3 --with numpy==2.4.6 --with pandas==3.0.3 --with shapely==2.1.2 --with pyproj==3.7.2 --with packaging==26.2 --with pyogrio==0.12.1 python /Users/nicolaslazaro/Desktop/work/hfx/.worktrees/tdx-hydro-planetary-compile-and-assembly/m4-s1/adapters/tdx-hydro/verify_geopandas_hilbert_parity.py worker --corpus /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/corpus.tsv --pairs /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/vm-stack-geopandas-1.1.3.pairs.tsv --metadata /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/vm-stack-geopandas-1.1.3.metadata.json --label vm-stack-geopandas-1.1.3
```

VM stack with GeoPandas 1.1.4:

```bash
/Users/nicolaslazaro/.local/bin/uv run --no-project --python 3.12.11 --with geopandas==1.1.4 --with numpy==2.4.6 --with pandas==3.0.3 --with shapely==2.1.2 --with pyproj==3.7.2 --with packaging==26.2 --with pyogrio==0.12.1 python /Users/nicolaslazaro/Desktop/work/hfx/.worktrees/tdx-hydro-planetary-compile-and-assembly/m4-s1/adapters/tdx-hydro/verify_geopandas_hilbert_parity.py worker --corpus /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/corpus.tsv --pairs /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/vm-stack-geopandas-1.1.4.pairs.tsv --metadata /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/vm-stack-geopandas-1.1.4.metadata.json --label vm-stack-geopandas-1.1.4
```

Adapter lock:

```bash
/Users/nicolaslazaro/Desktop/work/hfx/.worktrees/tdx-hydro-planetary-compile-and-assembly/m4-s1/adapters/tdx-hydro/.venv/bin/python3 /Users/nicolaslazaro/Desktop/work/hfx/.worktrees/tdx-hydro-planetary-compile-and-assembly/m4-s1/adapters/tdx-hydro/verify_geopandas_hilbert_parity.py worker --corpus /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/corpus.tsv --pairs /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/adapter-lock-geopandas-1.1.4.pairs.tsv --metadata /var/folders/9m/29m0bx0j0rsdyqb0b6yns28w0000gn/T/hfx-hilbert-parity-4uo226uy/adapter-lock-geopandas-1.1.4.metadata.json --label adapter-lock-geopandas-1.1.4
```

VM-native confirmation required by the M4-S3 runbook and M4-S4 paid execution after VM bootstrap and before acquisition or compile:

```bash
/opt/hfx-geo/bin/python "$HFX_REPO/adapters/tdx-hydro/verify_geopandas_hilbert_parity.py" vm-confirm
```

## Results

| Output | Rows | SHA-256 |
| --- | ---: | --- |
| `vm-stack-geopandas-1.1.3` | `100064` | `18a41718c615b1eebc86c430c560dae1edc4e0f7bbd8f4b7e9251219eb40144b` |
| `vm-stack-geopandas-1.1.4` | `100064` | `18a41718c615b1eebc86c430c560dae1edc4e0f7bbd8f4b7e9251219eb40144b` |
| `adapter-lock-geopandas-1.1.4` | `100064` | `18a41718c615b1eebc86c430c560dae1edc4e0f7bbd8f4b7e9251219eb40144b` |

| Comparison | Result |
| --- | --- |
| VM stack GeoPandas 1.1.3 vs. VM stack GeoPandas 1.1.4 | BYTE-IDENTICAL |
| VM stack GeoPandas 1.1.3 vs. adapter lock GeoPandas 1.1.4 | BYTE-IDENTICAL |
| VM stack GeoPandas 1.1.4 vs. adapter lock GeoPandas 1.1.4 | BYTE-IDENTICAL |

All tie-group members emitted equal keys within each environment. The verifier completed in `4.900` seconds.

## macOS GeoPandas-version verdict

GEOPANDAS VERSION PARITY ON MACOS: PASS - all three GeoPandas Hilbert outputs are byte-identical on macOS.

## Residual risk

This proof does not close the VM-Linux-GeoPandas-1.1.3 versus local-macOS-GeoPandas-1.1.4 gap. Shapely and NumPy use platform-specific wheels with platform-specific GEOS and BLAS components. M4-S3's runbook and M4-S4's paid execution therefore require the VM-native command above after bootstrap and before acquisition or compile. Under `/opt/hfx-geo/bin/python`, it deterministically regenerates the corpus, requires its byte hash to equal the committed corpus hash, computes VM-native Hilbert pairs, and requires their byte hash to equal the committed common macOS pair hash. This removes any dependency on uncommitted or deleted corpus and pair artifacts. Any environment assertion, regeneration, document-parse, or hash mismatch aborts before acquisition and before any compile.
