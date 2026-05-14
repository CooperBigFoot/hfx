//! Minimal integration tests for the v0.2 validator pipeline.

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
fn valid_v02_fixture_has_clean_json_report() {
    let report = validate(&fixture_path("valid", "tiny"), false, true, 100.0);
    assert!(report.is_valid(), "{:#?}", report.diagnostics());

    let json: serde_json::Value = serde_json::from_str(&report.display_json()).unwrap();
    assert_eq!(json["passed"], true);
    assert_eq!(json["error_count"], 0);
}

#[test]
fn legacy_v01_manifest_is_hard_cut() {
    let report = validate(
        &fixture_path("invalid", "legacy-format-version"),
        false,
        true,
        100.0,
    );
    assert!(!report.is_valid());
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|d| d.check_id == "manifest.unsupported_format_version")
    );
}

#[test]
fn empty_directory_fails_file_presence() {
    let dir = tempfile::tempdir().unwrap();
    let report = validate(dir.path(), false, true, 100.0);
    assert!(!report.is_valid());
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|d| d.check_id == "file_presence.manifest")
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|d| d.check_id == "file_presence.catchments")
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|d| d.check_id == "file_presence.graph")
    );
}
