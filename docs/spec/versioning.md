# Versioning

HFX carries two version tracks that move on their own schedules.
The spec track versions the format.
The toolkit track versions the Rust crates and the command-line tool that read and write the format.

A spec release does not force a toolkit release.
A toolkit release does not force a spec release.
This page defines both tracks, maps each toolkit release to the spec version it implements, and records the toolkit release procedure.

## Spec track

The spec track uses `format_version`, the version of the HFX format itself.
The current `format_version` is 0.3.0.
The 0.3.0 wire shape replaced the four flat catchments and snap bounding-box columns with a single GeoParquet 1.1 `bbox` covering struct, a grouped column that stores each row's bounding box.
That change is breaking over the previous 0.2.1 format.

The specification defines the compatibility rules normatively.
Read the [version compatibility](HFX_SPEC.md#version-compatibility) section of the specification for the rules on what counts as a compatible change while the major version is 0.
This page does not restate those rules.

You can confirm the current spec version in three places:

- the version header of the [specification](HFX_SPEC.md)
- the `format_version` constant, described on the [manifest and schema](manifest.md) page
- the newest entry in the [spec changelog](CHANGELOG.md)

## Toolkit track

The toolkit track versions the two workspace crates `hfx` and `hfx-cli`.
The `hfx` crate is the library that other Rust code depends on.
The `hfx-cli` crate installs a binary named `hfx`.
The two crates move in lockstep: both read the workspace version through `version.workspace = true` in the root `Cargo.toml`.
The current workspace version is 0.5.0.

Toolkit releases follow Semantic Versioning, a scheme that signals compatibility through the version number, over the crate API and the `hfx` command-line surface.
While the major version is 0, a minor bump signals a breaking change to either surface.
The `v*` git tags belong to this track.

crates.io currently holds the 0.4.0 toolkit crates, published on 2026-07-06 under the current names `hfx` and `hfx-cli`.
The next curated release is 0.5.0 for both crates, tagged `v0.5.0`, and a human maintainer fires it.

The `v0.4.0` git tag points at a commit that predates the `hfx-core` → `hfx` rename, so it does not match the published 0.4.0 crates.
Treat the crates.io artifacts as authoritative for what 0.4.0 shipped, and tag `v0.5.0` on the release-prep commit itself.

## Spec-to-toolkit mapping

Each toolkit release implements exactly one spec version.
The following table maps published and planned toolkit releases to the spec version each one implements.

| Toolkit release | Spec version implemented | Status |
|---|---|---|
| toolkit 0.2.0 and 0.1.26 (crates.io, pre-rename names) | legacy 0.1-era format | superseded |
| toolkit 0.3.0 (crates.io, pre-rename names) | HFX spec 0.2.1 | released |
| hfx 0.4.0 and hfx-cli 0.4.0 | HFX spec 0.3.0 | released |
| hfx 0.5.0 and hfx-cli 0.5.0 | HFX spec 0.3.0 | planned |

Use a toolkit release against datasets that match its spec version.
The legacy 0.2.0 and 0.1.26 crates target the 0.1-era format, so do not use them against current datasets.

## Release procedure

A toolkit release is a maintainer-driven event.
Agents never create or push tags, and never publish to crates.io.
Follow these steps in order:

1. Choose the toolkit version under Semantic Versioning. While the major version is 0, a minor bump signals a breaking change to the crate API or the `hfx` command-line surface.
2. Run `./scripts/bump-version.sh <patch|minor|major>` to update the workspace version in the root `Cargo.toml`.
3. Confirm that the `hfx-cli` crate's exact `=X.Y.Z` dependency pin on `hfx` picked up the new version. Run `grep '^hfx =' crates/hfx-cli/Cargo.toml` and read the result.
4. Add the release entry to the root `CHANGELOG.md`.
5. Update the spec-to-toolkit mapping table on this page.
6. Commit the change as `chore(release): prepare vX.Y.Z`.
7. A human maintainer creates and pushes the tag `vX.Y.Z`.
8. Publish `hfx` first. Wait for the crates.io index to list it. Then publish `hfx-cli`. A human fires each publish.

Spec releases stay independent of this procedure.
A spec change does not block a toolkit release, and a toolkit release does not block a spec change.
