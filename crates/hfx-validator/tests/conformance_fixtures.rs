//! Conformance fixture tests for the hfx-validator.

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
    let report = validate(&p, false, true, 100.0);
    assert!(
        report.is_valid(),
        "expected valid, diagnostics: {:#?}",
        report.diagnostics()
    );
}

#[test]
fn conformance_invalid_fixtures_emit_expected_diagnostic() {
    let cases = [
        (
            "dangling-upstream-ref",
            "referential.upstream_not_in_catchments",
        ),
        ("crs-mismatch", "manifest.crs"),
        ("parent-cycle", "parent.cycle_detected"),
        ("parent-level-not-coarser", "parent.level_not_coarser"),
        (
            "legacy-format-version",
            "manifest.unsupported_format_version",
        ),
    ];

    for (name, expected) in cases {
        let p = fixture_path("invalid", name);
        let report = validate(&p, false, true, 100.0);
        assert!(!report.is_valid(), "expected {name} to be invalid");
        assert!(
            report.diagnostics().iter().any(|d| d.check_id == expected),
            "fixture {name} missing {expected}; got {:#?}",
            report.diagnostics()
        );
    }
}
