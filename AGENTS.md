# Project Instructions

A rule appears in this file only if (a) it encodes a project choice that cannot be inferred from the code, or (b) default model output violates it. Practices a model already follows unprompted, and anything rustfmt or clippy enforces mechanically, are deliberately absent.

## 0. Project Overview

`hfx` defines HFX (HydroFabric Exchange): an open specification and Rust toolkit for a compiled drainage format that lets watershed delineation engines consume any source hydrofabric through one normalized contract. Adapters compile source-specific fabrics such as HydroBASINS, GRIT, MERIT Hydro, or TDX-Hydro into HFX once, offline; engines then read HFX artifacts through the same documented format.

`spec/HFX_SPEC.md` is the primary normative artifact. The validator and the adapters serve the specification, not the other way round: when code and spec disagree, the spec is right and the code is the defect, unless the spec is being deliberately amended in that same change. Do not introduce a behavior in `crates/*` that the spec does not describe.

The sibling `../pourpoint` repository is the runtime engine that consumes these datasets. Changes to the on-disk contract are consumer-visible; treat them as breaking until proven otherwise.

## 1. Workspace Layout and Tooling

- `crates/hfx` — library: core domain types and manifest models. All domain logic lives here.
- `crates/hfx-cli` — binary: the `hfx` validator CLI, and the composition root.
- `spec/HFX_SPEC.md` — the normative contract. `schemas/` holds its machine-readable form.
- `conformance/valid/*`, `conformance/invalid/*` — validator fixtures; invalid fixtures must exit 1.
- `examples/` — reference datasets for implementers.
- `adapters/<source>/` — offline, one-way Python compile steps, one directory per source fabric.
- `docs/` with `mkdocs.yml`; `make docs` builds strict.

```bash
cargo fmt
cargo clippy --workspace --all-targets
cargo test --workspace
```

For the Python adapters under `adapters/*`, use `uv` exclusively: `uv add <package>`, `uv sync`, `uv run <command>`. Do not use `pip`, `poetry`, `conda`, or `pip-tools` directly.

Docs sources under `docs/` and `spec/` ban the em dash (U+2014) and contrastive negation ("it's not X, it's Y"). State what a thing is. See `docs/STYLE.md`.

## 2. Design Doctrine

Four rules. They are one design stance seen four ways: a module means one thing, receives exactly what it needs, in types that cannot lie, and dies rather than guess.

### 2.1 Denotation line

Before implementing a module, state in one line in its `//!` doc what it computes as a mathematical object. Carriers must be named domain types, not placeholders.

```rust
//! parse : ManifestBytes → Manifest            (pure, total on valid input)
//! validation = fold(accumulate, empty, checks)
```

If the line cannot be written, the design is not ready; say so instead of coding around it. In review, when the denotation line and the diff disagree, one of them is wrong.

### 2.2 Authority narrows

`crates/hfx-cli/src/main.rs` is the composition root: only it reads config files and environment variables, resolves paths, initializes `tracing`, and opens files or datasets. `crates/hfx` receives everything as arguments — a function there that reads an env var, constructs a `Path` from a literal, or touches global state is a violation.

At every call, pass the narrowest argument that suffices: the one manifest field, not the whole `Manifest`; the declared auxiliary entry, not the dataset.

### 2.3 Type-driven design

Encode domain invariants in the type system; invalid states must fail to compile.

- **Parse, don't validate (hard rule).** Raw input (CLI args, `manifest.json` bytes, Parquet metadata) is converted into domain types once, at the composition root. No raw primitive crosses into `crates/hfx` where a domain type exists.
- **Newtypes** wherever two values of the same primitive type could be swapped: drainage unit IDs, levels, areas, coordinates (grid vs. geographic), thresholds. Bare primitives are fine for unambiguous locals.
- **Enums over booleans.** Never `bool` for a domain state with two named possibilities: `enum Topology { Tree, Dag }`, not `is_dag: bool`. Applies to fields, parameters, and return values.
- **Typestate** for resources with a lifecycle, where calling methods out of order is a logic bug. Do not force it on plain structs.

### 2.4 Fail loud

An error is either propagated with `?` or handled at one named per-item isolation point — never discarded to make code compile. `.unwrap_or_default()` on a required value, `let Ok(x) = … else { continue }` that silently skips a broken item, and `.ok()` that drops the error are all bugs.

The one exception: a batch loop over independent items (e.g. per-entry auxiliary checks, per-basin compiles) may have exactly one isolation point that catches per-item failure, records which item failed and why, and continues. That point exists once per pipeline, not once per function.

An unreadable or unrecognized *optional* declaration must not take out the mandatory core. Cost it its own entry, name that entry in the result, and keep going.

## 3. Errors and Logging

- `crates/hfx` uses `thiserror`; `crates/hfx-cli` uses `anyhow` with `.context()`.
- Every error variant gets a doc comment stating *when* it fires, and named fields, not tuples — the message should carry the values needed to act on it (`"auxiliary schema {schema} declared at {path} is not implemented"`).
- Diagnostics go through `tracing` with structured fields (`debug!(level = unit.level, "checking")`), not format strings. `#[instrument]` on public functions, with `skip` for large args. Levels: `error` = broken, `warn` = degraded, `info` = milestones, `debug` = internals, `trace` = hot loops.
- Validator output is a user-facing contract in its own right: a diagnostic names the artifact, the row or entry, and the spec rule it violates.

## 4. Documentation

Documentation is for agents landing in the code, applied proportionally to complexity — not decoration.

- Simple module: the `//!` denotation line and a sentence suffice.
- Complex crate: `crates/foo/README.md` with purpose, a Mermaid architecture diagram (never ASCII art), a glossary of domain terms and math symbols, and the key entry-point types.
- Fallible public functions get an `# Errors` section; skip doc comments on obvious helpers and trivial getters.
- Domain language is fixed by `CONTEXT.md`. Use its canonical terms and avoid the listed aliases; a new domain term goes in that table before it goes in code.
- A spec change lands with the spec text, the JSON schema, and a conformance fixture in the same commit.

## 5. Style Residue

- Builder pattern (`with_*` returning `Self`) for config structs with more than 3 fields.
- No `use super::*`; explicit imports only.

## 6. Releases (curated)

This repository does NOT bump versions or create tags on ordinary commits. Commit with a conventional message and leave the `Cargo.toml` version field, git tags, and `CHANGELOG.md` alone. Version changes are never per-commit ceremony.

Releases are intentional, maintainer-driven events:

- **No per-commit bumps.** The workspace version changes only as part of a deliberate release, prepared with `./scripts/bump-version.sh <patch|minor|major>` in the release commit itself.
- **Human-tagged.** `v*` git tags are created by a human maintainer at release time. Agents never create or push tags.
- **CHANGELOG'd.** Every release ships with notes in the root `CHANGELOG.md` describing what changed since the previous release.
- **Two version tracks.** The SPEC track (`format_version`, currently `0.3.0`) and the TOOLKIT track (the lockstep `hfx`/`hfx-cli` workspace crates, which own the `v*` tags) evolve independently. See `docs/spec/versioning.md` for the full two-track policy and the spec-to-toolkit mapping table.

<!-- BEGIN SYNCED DOCTRINE; source-sha256=59e37fd6b3dbab27530822e6956da51bb7ae76b637e3638530f99a8b4db9038d -->
Four rules. They are one design stance seen four ways: a module means one thing, receives exactly what it needs, in types that cannot lie, and dies rather than guess.

1. **A module means one thing.**
2. **It receives exactly what it needs.**
3. **Its types cannot lie.**
4. **It dies rather than guess.**
<!-- END SYNCED DOCTRINE -->
