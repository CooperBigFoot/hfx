## Summary

<!-- What does this PR change, and why? -->

## Related issue

<!-- Link the issue this PR addresses, if any. -->

## Checklist

- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace` passes
- [ ] Every `conformance/valid/*/` fixture (and `examples/tiny/`) validates with exit 0
- [ ] Every `conformance/invalid/*/` fixture is rejected with exit 1
- [ ] `check-jsonschema --schemafile schemas/manifest.schema.json conformance/valid/*/manifest.json examples/tiny/manifest.json` passes
- [ ] Spec changes are language-only — no change to the format_version 0.2.1 wire shape
- [ ] CHANGELOG.md updated if the change is user-visible
