//! Conformance fixture tests for the hfx-validator.
//!
//! Each test loads a pre-generated fixture from the `conformance/` directory
//! at the repo root and asserts the expected validator outcome.
//!
//! Fixtures are generated (and regenerated) by running:
//!
//!     uv run conformance/generate_fixtures.py

use std::path::PathBuf;

use hfx_validator::validate;

fn fixture_path(category: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("conformance")
        .join(category)
        .join(name)
}

#[test]
fn conformance_valid_tiny_passes() {
    let p = fixture_path("valid", "tiny");
    let report = validate(
        &p, /*strict=*/ false, /*skip_rasters=*/ true, /*sample_pct=*/ 100.0,
    );
    assert!(
        report.is_valid(),
        "expected valid, diagnostics: {:#?}",
        report.diagnostics()
    );
    // A schema.catchments.rg_size WARNING may be present (5 rows < 4096). Tolerated in non-strict.
}

#[test]
fn conformance_invalid_dangling_upstream_ref_fails() {
    let p = fixture_path("invalid", "dangling-upstream-ref");
    let report = validate(&p, false, true, 100.0);
    assert!(!report.is_valid(), "expected invalid, got valid");
    let has_ref_err = report
        .diagnostics()
        .iter()
        .any(|d| d.check_id == "referential.upstream_not_in_catchments");
    assert!(
        has_ref_err,
        "missing referential.upstream_not_in_catchments diagnostic; got: {:#?}",
        report.diagnostics()
    );
    let has_999 = report.diagnostics().iter().any(|d| {
        d.check_id == "referential.upstream_not_in_catchments" && d.message.contains("999")
    });
    assert!(has_999, "diagnostic message does not mention 999");
}

#[test]
fn conformance_invalid_crs_mismatch_fails() {
    let p = fixture_path("invalid", "crs-mismatch");
    let report = validate(&p, false, true, 100.0);
    assert!(!report.is_valid(), "expected invalid");
    let has_crs = report
        .diagnostics()
        .iter()
        .any(|d| d.check_id == "manifest.crs" && d.message.contains("EPSG:32632"));
    assert!(
        has_crs,
        "missing or ill-formed manifest.crs diagnostic; got: {:#?}",
        report.diagnostics()
    );
}
