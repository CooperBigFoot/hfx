# Contributing

HFX is an open specification and toolkit repository. Contributions may target the specification, schemas, examples, conformance fixtures, validator, or future adapters.

## Working Principles

- Treat [`spec/HFX_SPEC.md`](./spec/HFX_SPEC.md) as the canonical development spec.
- Keep machine-readable contracts in [`schemas/`](./schemas).
- Keep implementer-facing examples in [`examples/`](./examples).
- Keep validator fixtures in [`conformance/`](./conformance).
- Keep Rust implementation code in [`crates/`](./crates).

## Spec Changes

If a change affects artifact shape, required fields, semantics, or validation behavior:

- update the specification
- update the relevant schema if one exists
- add or adjust examples and conformance fixtures when applicable
- record the rationale in [`docs/decisions/`](./docs/decisions) for non-trivial design changes
- add a short entry to the changelog

## Validator Changes

Validator behavior should follow the spec and schemas. Avoid adding validator-only policy that is not grounded in the public contract.

## Early Stage Note

This repository is still in initial scaffolding. Placeholder directories and documents are intentional; fill them in as the first working validator and example datasets take shape.
