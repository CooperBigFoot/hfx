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
fn conformance_valid_fixtures_pass() {
    for name in [
        "tiny",
        "tiny-with-aux-d8",
        "grit-two-level",
        "grit-two-snap",
    ] {
        let p = fixture_path("valid", name);
        let report = validate(&p, false, true, 100.0);
        assert!(
            report.is_valid(),
            "expected {name} to be valid, diagnostics: {:#?}",
            report.diagnostics()
        );
    }
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
        ("legacy-graph-arrow", "graph.legacy_arrow_format"),
        ("graph-missing-bbox-cols", "schema.missing_column"),
        (
            "graph-bbox-stats-missing",
            "schema.graph.bbox_stats_missing",
        ),
        (
            "catchments-multi-level-unsorted",
            "ordering.catchments.level_unsorted",
        ),
        ("graph-level-unsorted", "ordering.graph.level_unsorted"),
        ("legacy-core-snap", "file_presence.legacy_snap_parquet"),
        ("aux-snap-bad-stem-role", "aux.snap.stem_role_invalid"),
        ("aux-snap-level-not-declared", "aux.snap.level_not_declared"),
        ("aux-snap-duplicate-name", "aux.snap.duplicate_name"),
        ("aux-snap-bad-geometry", "geometry.snap_wrong_type"),
        ("aux-snap-weight-negative", "aux.snap.weight_invalid"),
        ("v02-format-version", "manifest.unsupported_format_version"),
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
