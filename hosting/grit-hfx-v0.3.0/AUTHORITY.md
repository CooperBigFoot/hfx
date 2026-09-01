# GRIT HFX v0.3.0 publication and rollback authority

**Status: PUBLISHED AND ACCEPTED**

## Scope

This package is the local operational and historical authority for `https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/`. It covers the hosted keys `manifest.json`, `aux/d8/flow_dir.tif`, and `aux/d8/flow_acc.tif`. The HFX source ref is `c1b19aec580ad13af8235ee6037e0b9b5081933d`; the cross-repository evidence ref is `6f12abf2f7d47a31d7c1a4cdee99e30db7400bb6`.

The current canonical `manifest.json` is 1426 bytes with SHA-256 `02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c`. The former manifest is 1132 bytes with SHA-256 `0935a7bc09b7c2636786082fd9fd9a669ea1b32c6e2e4d92cb3f8da531c083c4`.

## Authority files

`manifest.former.json` is the exact embedded and formerly public body. It has zero `hfx.aux.d8_raster.v2` declarations and is byte-identical rollback material. `manifest.json` is the current canonical public body. `identity-inventory.json` records local identities and supplied hosted observations. `canonical-publication.json`, `containment.json`, and `revert-rehearsal.json` record the accepted publication and its safeguards. `verify-authority.py` verifies the package without network access. Existing `NOTICE`, `CITATION.txt`, and `README.md` remain immutable authority inputs.

## Identity strength and provenance

The orchestrator made these bounded observations on 2026-08-07 and supplied them to an executor that ran fully offline and made zero network requests:

- `manifest.json` former live body: HTTP 200, 1132 body bytes, SHA-256 `0935a7bc09b7c2636786082fd9fd9a669ea1b32c6e2e4d92cb3f8da531c083c4`, full body read and digest measured.
- `NOTICE`: HTTP 200, 1454 body bytes, SHA-256 `eac224bf0b70b1494e5abd89f80079d665150ea744a2f730593f7216ca223db3`, full body read and digest measured.
- `CITATION.txt`: HTTP 200, 2495 body bytes, SHA-256 `8c7bf86a5962bf42282bbfd401226773601c2551f79685bad9be68d3b41363ac`, full body read and digest measured.
- `README.md`: HTTP 200, 6967 body bytes, SHA-256 `2b86e8278996aa7540359e6b397c0a042f90c827e9c61730c81fca9eb3e63e56`, full body read and digest measured.
- `aux/d8/flow_dir.tif`: HTTP 200 from HEAD only, Content-Length 50686516478, ETag `"bc48d1013cf6908fb44c325dd2ad10ab-1511"`, Last-Modified `Wed, 22 Jul 2026 16:16:52 GMT`, no body read and no digest measured.
- `aux/d8/flow_acc.tif`: HTTP 200 from HEAD only, Content-Length 205069870081, ETag `"49eab3942a26036aa49e72ea33a1b724-6112"`, Last-Modified `Wed, 22 Jul 2026 16:04:21 GMT`, no body read and no digest measured.

The orchestrator transferred 12,048 total body bytes and zero COG body bytes. The COG live observations establish Content-Length plus multipart ETag only. Both live sizes match the recorded sizes. A multipart ETag is not SHA-256. The flow-direction COG's recorded historical SHA-256 is `eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656`; the flow-accumulation COG's recorded historical SHA-256 is `30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b`. These historical hashes were not re-established from hosted bodies. Full hashing would transfer 255,756,386,559 bytes and is forbidden.

At preparation time, the hosted README identity was 6967 bytes with SHA-256 `2b86e8278996aa7540359e6b397c0a042f90c827e9c61730c81fca9eb3e63e56`, and the local authority README identity was 17601 bytes with SHA-256 `4edd32a056a3631b538ba54f872345a4448d44afe37ecc641472a348a1085f82`. This is retained historical evidence, not a claim about the current hosted README body.

## Historical candidate reproduction

Before publication, the candidate originated from the accepted planetary build window `2026-07-21T16:41:05Z` through `2026-07-21T21:05:25Z` and used `created_at = "2026-07-21T21:05:12Z"`. It was not produced by preserving the former manifest's `created_at`. Directly amending the former manifest while preserving that timestamp produces 1426 bytes with SHA-256 `fb79355f85f8a52ff7d693c0152aec2262c96ff7e59a3bc9357993a8e0c6a3e1`. The measured difference between the former public fields and accepted-build fields is `created_at_only` before the pinned adapter amendment.

The amendment appends exactly this declaration:

```json
{
  "schema": "hfx.aux.d8_raster.v2",
  "artifacts": {
    "flow_dir": "aux/d8/flow_dir.tif",
    "flow_acc": "aux/d8/flow_acc.tif"
  },
  "metadata": {
    "crs": "EPSG:8857",
    "flow_dir_encoding": "grass",
    "flow_acc_units": "km2"
  }
}
```

Serialization is `json.dumps(manifest, indent=2) + "\n"`.

## Verify offline

```bash
python3 hosting/grit-hfx-v0.3.0/verify-authority.py
python3 hosting/grit-hfx-v0.3.0/verify-authority.py --self-test
```

The self-test copies the package into temporary directories and proves that separate one-bit corruptions of the candidate and former manifests are rejected.

## Current mutation boundary

This repository records the accepted public state. It does not authorize a hosted mutation. The following prohibitions remain active:

- No agent may upload, publish, roll back, delete, or otherwise modify a hosted object. A human controls every remote mutation.
- The accepted public prefix remains `grit/hfx-v0.3.0/`; no successor prefix is authorized here, and the planetary COGs must not be re-uploaded.
- `manifest.former.json` remains rollback evidence. Its presence does not authorize rollback execution.
- Dated failure evidence, rollback evidence, and historical non-refined artifacts remain immutable.
- Recorded digests, byte counts, timestamps, and serialized bodies must not be changed to fit new observations.
- Full downloads of either planetary COG for identity hashing are forbidden. Current verification may use only the bounded reads required by the accepted live-proof procedure.
- The HFX vector contract, `format_version`, `.pce/repository-contract.json`, releases, and tags are outside this authority.

## Historical preparation constraints

The authority package was created before publication under a narrower executor assignment. That executor made zero network requests, performed no remote mutation, did not run later milestones, and delivered a single five-file preparation commit. Those statements describe the completed preparation step only. They do not describe the current repository delivery shape and do not imply that publication remains pending.

The preparation step also prohibited hand-editing either manifest, preserving the former `created_at` in the candidate, changing recorded identities to fit generated bytes, and downloading any part of either COG. The accepted publication and later evidence records remain the authority for what occurred after that step.

## Historical not-touched fence

The preparation assignment used this complete not-touched fence:

```text
CONTEXT.md
.pce/repository-contract.json
Cargo.toml
Cargo.lock
CHANGELOG.md
src/**
crates/**
schemas/**
spec/**
conformance/**
examples/**
adapters/**
scripts/**
docs/decisions/**
hosting/README.md
hosting/archive/**
hosting/grit-2.0.0-rehost-v0.3.0/**
hosting/grit-2.0.0/**
hosting/grit-hfx-v0.3.0/CITATION.txt
hosting/grit-hfx-v0.3.0/NOTICE
hosting/grit-hfx-v0.3.0/README.md
hosting/grit-hfx-v0.3.0/manifest-amendment-dry-run.txt
hosting/tdx-hydro-7020000010/**
```
