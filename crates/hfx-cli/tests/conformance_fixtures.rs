//! Conformance fixture tests for hfx-cli.

use std::path::PathBuf;

use hfx_cli::validate;

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
        "tiny-with-two-aux-d8",
        "tiny-with-two-aux-d8-tiled",
        "tiny-with-aux-d8-projected-grass",
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
fn conformance_d8_raster_fixtures_pass_with_raster_checks_enabled() {
    for (name, strict) in [
        ("tiny-with-aux-d8", false),
        ("tiny-with-two-aux-d8", false),
        ("tiny-with-two-aux-d8-tiled", false),
        ("tiny-with-aux-d8-projected-grass", true),
    ] {
        let p = fixture_path("valid", name);
        let report = validate(&p, strict, false, 100.0);
        assert!(
            report.is_valid(),
            "expected {name} to be valid with raster checks, diagnostics: {:#?}",
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
        (
            "tiny-with-aux-d8-missing-crs",
            "auxiliary.d8_raster_metadata",
        ),
        (
            "tiny-with-aux-d8-malformed-crs",
            "auxiliary.d8_raster_metadata",
        ),
        ("tiny-with-legacy-aux-d8-v1", "manifest.auxiliary_schema"),
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
        let expected_message = match name {
            "tiny-with-aux-d8-missing-crs" => {
                Some("missing or non-string D8 raster v2 metadata field \"crs\"")
            }
            "tiny-with-aux-d8-malformed-crs" => Some("invalid D8 raster v2 crs"),
            _ => None,
        };
        if let Some(expected_message) = expected_message {
            assert!(
                report.diagnostics().iter().any(|diagnostic| {
                    diagnostic.check_id == expected && diagnostic.message.contains(expected_message)
                }),
                "fixture {name} missing {expected} message fragment {expected_message:?}; got {:#?}",
                report.diagnostics()
            );
        }
    }
}

#[test]
fn conformance_d8_no_overlap_fixture_emits_expected_diagnostic() {
    let p = fixture_path("invalid", "tiny-with-aux-d8-no-overlap");
    let report = validate(&p, false, false, 100.0);

    assert!(
        !report.is_valid(),
        "expected non-overlapping D8 fixture to be invalid"
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|diag| diag.check_id == "raster.extent_no_overlap"),
        "expected raster.extent_no_overlap diagnostic; got {:#?}",
        report.diagnostics()
    );
}

#[test]
fn conformance_bad_second_d8_fixture_reports_named_entry() {
    let p = fixture_path("invalid", "tiny-with-bad-second-aux-d8");
    let report = validate(&p, false, false, 100.0);

    assert!(
        !report.is_valid(),
        "expected malformed D8 fixture to be invalid"
    );
    assert!(
        report.diagnostics().iter().any(|diag| {
            diag.check_id == "raster.flow_dir_dtype" && diag.message.contains("bad_second:")
        }),
        "expected raster.flow_dir_dtype diagnostic tagged with bad_second; got {:#?}",
        report.diagnostics()
    );
}

#[test]
fn conformance_d8_crs_mismatch_fixture_emits_expected_diagnostic() {
    let p = fixture_path("invalid", "tiny-with-aux-d8-crs-mismatch");
    let report = validate(&p, false, false, 100.0);

    assert!(
        !report.is_valid(),
        "expected D8 CRS mismatch fixture to be invalid"
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.check_id == "raster.crs_mismatch"),
        "expected raster.crs_mismatch diagnostic; got {:#?}",
        report.diagnostics()
    );
}

#[test]
fn conformance_d8_disallowed_dtype_fixture_emits_expected_diagnostic() {
    let p = fixture_path("invalid", "tiny-with-aux-d8-disallowed-dtype");
    let report = validate(&p, false, false, 100.0);

    assert!(
        !report.is_valid(),
        "expected D8 disallowed dtype fixture to be invalid"
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.check_id == "raster.flow_dir_dtype"),
        "expected raster.flow_dir_dtype diagnostic; got {:#?}",
        report.diagnostics()
    );
}

#[test]
fn conformance_d8_missing_nodata_fixture_emits_expected_diagnostic() {
    let p = fixture_path("invalid", "tiny-with-aux-d8-missing-nodata");
    let report = validate(&p, false, false, 100.0);

    assert!(
        !report.is_valid(),
        "expected D8 missing nodata fixture to be invalid"
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.check_id == "raster.flow_acc_nodata"),
        "expected raster.flow_acc_nodata diagnostic; got {:#?}",
        report.diagnostics()
    );
}

#[test]
fn conformance_d8_cells_int32_fixture_emits_expected_diagnostic() {
    let p = fixture_path("invalid", "tiny-with-aux-d8-cells-int32");
    let report = validate(&p, false, false, 100.0);

    assert!(
        !report.is_valid(),
        "expected D8 cells int32 fixture to be invalid"
    );
    assert!(
        report
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.check_id == "raster.flow_acc_units_dtype"),
        "expected raster.flow_acc_units_dtype diagnostic; got {:#?}",
        report.diagnostics()
    );
}
