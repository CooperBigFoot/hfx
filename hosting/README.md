# Upstream Tech Public Hydrologic Basin Delineation Datasets

This bucket hosts the canonical GRIT 2.0.0 HFX dataset, an HFX
v0.3.0 compilation of the Global River Topology (GRIT) vector datasets and
planetary direction and accumulation raster archive, stored under
`grit/hfx-v0.3.0/`. The manifest declares the raster objects as
`aux/d8/flow_dir.tif` and `aux/d8/flow_acc.tif`.

- [manifest.json](https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/manifest.json)
- [README.md](https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/README.md)
- [NOTICE](https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/NOTICE)
- [CITATION.txt](https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/CITATION.txt)

Attribution: Wortmann et al. 2025. Cite the vector archive (DOI
10.5281/zenodo.17435232), the [GRIT raster archive](https://doi.org/10.5281/zenodo.15715535),
and the [GRIT paper](https://doi.org/10.1029/2024WR038308). The combined hosted
dataset is licensed [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
for NonCommercial use. See the dataset NOTICE and CITATION.txt above for the
full license terms and citations.

HFX spec and toolkit: https://github.com/CooperBigFoot/hfx

Upstream Tech is the infrastructure sponsor for this bucket: it sponsors the
hosting infrastructure only and is not the data publisher or vendor.

## GRIT v0.3.0 manifest publication and rollback

The current operator entry point is `scripts/upload-r2-grit-d8.sh`. Its sole
manifest source is the reviewed package in `hosting/grit-hfx-v0.3.0/`.
Default dry-run and self-test are fully offline and make zero AWS calls:

```bash
bash scripts/upload-r2-grit-d8.sh
bash scripts/upload-r2-grit-d8.sh --self-test
bash scripts/upload-r2-grit-d8.sh --dry-run --rollback-manifest
```

Publication plans only the candidate manifest write to
`grit/hfx-v0.3.0/manifest.json`. Rollback plans only the byte-identical former
manifest write to the same key. Neither path re-uploads the D8 COGs or
attribution objects. A human may select a mutation path only with
`--execute --publish-manifest` or `--execute --rollback-manifest`, followed by
that path's distinct typed confirmation. Agents never invoke `--execute` and
never make AWS calls.

The command interface in the pinned dataset-local
`hosting/grit-hfx-v0.3.0/README.md` is retained as historical campaign
evidence because it is an identity-checked authority input. This section and
`hosting/grit-hfx-v0.3.0/manifest-amendment-dry-run.txt` define the current
manifest-only interface.
