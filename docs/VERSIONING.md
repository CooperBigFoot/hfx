# Versioning

HFX carries two independent version tracks:

- the **SPEC track** — the version of the HFX format itself (`format_version`)
- the **TOOLKIT track** — the version of the Rust crates and the `hfx` CLI that
  implement the spec

A spec release does not imply a toolkit release, and vice versa. This document
defines both tracks, maps toolkit releases to the spec version they implement,
and records the release procedure for the toolkit.

## Spec track (`format_version`)

The spec version is currently **0.3.0**: the 0.3.0 wire shape replaced the four
flat catchments/snap bbox columns with a single GeoParquet 1.1 `bbox` covering
struct (a breaking change over 0.2.1). SemVer semantics for `format_version` — what counts as a
compatible change versus a breaking change while MAJOR is 0 — are defined
normatively in the spec's
[Version Compatibility](../spec/HFX_SPEC.md#version-compatibility) section.
That section is the single source of truth; this document does not restate the
compatibility rules.

Spec history lives in the [spec changelog](../spec/CHANGELOG.md).

The authoritative locations for the current spec version are:

- the version header of [`spec/HFX_SPEC.md`](../spec/HFX_SPEC.md)
- the `format_version` const in
  [`schemas/manifest.schema.json`](../schemas/manifest.schema.json)
- the newest entry in [`spec/CHANGELOG.md`](../spec/CHANGELOG.md)

## Toolkit track (crates and CLI)

The toolkit is the pair of workspace crates `hfx-core` and `hfx-validator`
(the `hfx-validator` crate installs a binary named `hfx`). They move in
lockstep: both inherit the workspace version via `version.workspace = true` in
the root `Cargo.toml`. The current workspace version is 0.4.0.

Toolkit releases follow Rust SemVer over the crate API and the `hfx` CLI
surface. While MAJOR is 0, a MINOR bump signals a breaking change to either
surface. `v*` git tags belong to this track.

crates.io currently has `hfx-core` 0.3.0 and `hfx-validator` 0.3.0 — the first
curated release under this policy. The next curated release is **0.4.0** for
both crates, tagged `v0.4.0`, fired by a human maintainer.

## Spec-to-toolkit mapping

Each toolkit release implements exactly one spec version.
hfx-validator 0.4.0 implements HFX spec 0.3.0; the published 0.3.0 crates
implement the previous HFX spec 0.2.1. The legacy 0.2.0 / 0.1.26 crates target
the 0.1-era format and should not be used against current datasets.

| Toolkit release | Spec version implemented | Status |
|---|---|---|
| hfx-core 0.2.0 / hfx-validator 0.1.26 (crates.io) | legacy 0.1-era format | superseded |
| hfx-core 0.3.0 / hfx-validator 0.3.0 (crates.io) | HFX spec 0.2.1 | released |
| hfx-core 0.4.0 / hfx-validator 0.4.0 | HFX spec 0.3.0 | planned |

## Release procedure (toolkit)

Releases are intentional, maintainer-driven events. Agents never create or
push tags, and never publish to crates.io. The procedure:

1. Choose the toolkit version per Rust SemVer. While MAJOR is 0, a MINOR bump
   signals a breaking change to the crate API or the `hfx` CLI surface.
2. Run `./scripts/bump-version.sh <patch|minor|major>` to update the workspace
   version in the root `Cargo.toml`.
3. Verify that `hfx-validator`'s exact `=X.Y.Z` dependency pin on `hfx-core`
   picked up the new version — the bump script rewrites the pin, but check it
   rather than trusting it:
   `grep 'hfx-core' crates/hfx-validator/Cargo.toml`.
4. Add the release entry to the root [`CHANGELOG.md`](../CHANGELOG.md).
5. Update the spec-to-toolkit mapping table in this document.
6. Commit as `chore(release): prepare vX.Y.Z`.
7. A human maintainer creates and pushes the tag `vX.Y.Z` (agents never create
   or push tags).
8. Publish `hfx-core` first, wait for the crates.io index to pick it up, then
   publish `hfx-validator`. Publishing is human-fired.

Spec releases are independent of this procedure: the spec and toolkit tracks
version independently, so a spec change does not block a toolkit release and
vice versa.
