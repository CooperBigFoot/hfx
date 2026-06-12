# Changelog

This file tracks TOOLKIT releases — the `hfx-core` and `hfx-validator` crates
and the `hfx` CLI — following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
conventions. See [`docs/VERSIONING.md`](./docs/VERSIONING.md) for the two-track
version policy and [`spec/CHANGELOG.md`](./spec/CHANGELOG.md) for specification
changes.

## [Unreleased]

Nothing yet.

## [0.3.0] - 2026-06-12

First curated release of the HFX toolkit under the two-track version policy
(see [`docs/VERSIONING.md`](./docs/VERSIONING.md)). `hfx-core` and
`hfx-validator` both release as 0.3.0 in lockstep and implement HFX spec
0.2.1 (frozen wire shape). Highlights cover everything since the published
crates.io snapshots (`hfx-core` 0.2.0, `hfx-validator` 0.1.26).

### Added

- HFX spec 0.2.1 with RFC 2119 normative hardening, a Version Compatibility
  section, and a dedicated spec changelog
  ([`spec/CHANGELOG.md`](./spec/CHANGELOG.md)).
- Manifest JSON schema
  ([`schemas/manifest.schema.json`](./schemas/manifest.schema.json)) and a
  conformance suite of valid/invalid fixtures exercising it.
- `hfx` validator CLI, installed by the `hfx-validator` crate.
- `grit-v2` and `merit-v2` reference adapters.
- `examples/tiny` reference dataset.
- GitHub Actions CI plus issue and pull-request templates.
- Governance docs: [`docs/VERSIONING.md`](./docs/VERSIONING.md) (two-track
  version policy), `CODE_OF_CONDUCT.md`, `GOVERNANCE.md`.

### Changed

- `hfx-validator` is now version-locked to `hfx-core` via an exact `=0.3.0`
  dependency pin; the two crates move in lockstep.

## Pre-0.3.0 history

Before the curated-release policy (adopted 2026-06), every commit carried a
patch version bump and a `v*` tag, reaching workspace version 0.2.64 across 87
tags. The crates.io snapshots from that era are `hfx-core` 0.2.0 and
`hfx-validator` 0.1.26. Curated releases begin with 0.3.0.
