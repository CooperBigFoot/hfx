# Governance

## Purpose

This repository is the source of truth for the HFX open specification and its reference toolkit.

## Repository Model

HFX is organized as a spec-first monorepo:

- the specification defines the contract
- schemas encode machine-readable parts of that contract
- the validator checks conformance to the contract
- adapters compile source hydrofabrics into conformant HFX datasets

## Change Policy

- The specification is authoritative for format semantics.
- Schemas and validator behavior should reflect the specification.
- Significant design choices should be captured in [`docs/decisions/`](./docs/decisions).
- Breaking format changes should be accompanied by an explicit version change in the spec.

## Release Philosophy

The repository contains the development version of the specification. Tagged releases will later serve as stable, immutable snapshots of the spec and related artifacts.
