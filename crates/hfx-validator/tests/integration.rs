//! Integration tests for the full hfx-validator pipeline.
//!
//! Each test creates a temporary directory, writes minimal HFX fixture files
//! programmatically, and then calls `hfx_validator::validate()` to exercise
//! the complete read → check → report pipeline.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use arrow::array::{BinaryArray, Float32Array};

// ---------------------------------------------------------------------------
// WKB factory helpers
// ---------------------------------------------------------------------------

/// Build a minimal valid little-endian WKB Polygon (1 ring, 4 points).
fn make_wkb_polygon() -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1u8); // little-endian
    wkb.extend_from_slice(&3u32.to_le_bytes()); // type = Polygon
    wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
    wkb.extend_from_slice(&4u32.to_le_bytes()); // 4 points
    for &(x, y) in &[(0.0_f64, 0.0_f64), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)] {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

// ---------------------------------------------------------------------------
// File writers
// ---------------------------------------------------------------------------

/// Write a valid `catchments.parquet` with the given IDs and bbox-enclosing
/// bounding boxes. Each row gets the same polygon WKB as geometry.
fn write_catchments_parquet(dir: &Path, ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("area_km2", DataType::Float32, false),
        Field::new("up_area_km2", DataType::Float32, true),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("geometry", DataType::Binary, false),
    ]));

    let n = ids.len();
    let wkb = make_wkb_polygon();
    let geometries: Vec<&[u8]> = (0..n).map(|_| wkb.as_slice()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float32Array::from(vec![1.0_f32; n])),
            Arc::new(Float32Array::from(vec![Some(10.0_f32); n])),
            // bbox: [0, 0, 1, 1] — all catchments share this box (well within manifest bbox)
            Arc::new(Float32Array::from(vec![0.0_f32; n])),
            Arc::new(Float32Array::from(vec![0.0_f32; n])),
            Arc::new(Float32Array::from(vec![1.0_f32; n])),
            Arc::new(Float32Array::from(vec![1.0_f32; n])),
            Arc::new(BinaryArray::from_vec(geometries)),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(dir.join("catchments.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Build a `ListArray<Int64>` from a slice of rows.
fn make_list_int64(rows: &[Vec<i64>]) -> ListArray {
    let values: Vec<i64> = rows.iter().flat_map(|r| r.iter().copied()).collect();
    let values_array = Arc::new(Int64Array::from(values));

    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    for row in rows {
        offsets.push(*offsets.last().unwrap() + row.len() as i32);
    }

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let item_field = Arc::new(Field::new("item", DataType::Int64, true));
    ListArray::new(item_field, offsets, values_array, None)
}

/// Write a valid `graph.arrow` with the given IDs and upstream-ID lists.
fn write_graph_arrow(dir: &Path, ids: &[i64], upstream_ids: &[Vec<i64>]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "upstream_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
    ]));

    let upstream_array = make_list_int64(upstream_ids);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(upstream_array),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(dir.join("graph.arrow")).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

/// Write a valid `manifest.json` with the given options.
fn write_manifest(dir: &Path, atom_count: u64, has_snap: bool, has_rasters: bool) {
    let manifest = serde_json::json!({
        "format_version": "0.1",
        "fabric_name": "test-fabric",
        "crs": "EPSG:4326",
        "has_up_area": true,
        "has_rasters": has_rasters,
        "has_snap": has_snap,
        "terminal_sink_id": 0,
        "topology": "tree",
        // bbox large enough to enclose catchments at [0,0,1,1]
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": atom_count,
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "v1.0"
    });
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Dataset helpers
// ---------------------------------------------------------------------------

/// Create a minimal valid HFX dataset with 3 catchments in a linear chain.
///
/// Graph: 1 ← 2 ← 3  (3 is the outlet; no snap; no rasters)
fn create_valid_dataset(dir: &Path) {
    // IDs 1, 2, 3
    let ids = &[1_i64, 2, 3];
    // Linear chain: 3 receives 2, 2 receives 1, 1 is a headwater
    let upstream = vec![vec![], vec![1_i64], vec![2_i64]];

    write_manifest(dir, 3, false, false);
    write_catchments_parquet(dir, ids);
    write_graph_arrow(dir, ids, &upstream);
}

/// Create a minimal invalid dataset: the manifest has wrong format_version type.
fn create_invalid_manifest_dataset(dir: &Path) {
    // Write a manifest with a clearly wrong format_version value that fails
    // the manifest checks (0.99 is not a known version).
    let manifest = serde_json::json!({
        "format_version": "99.0",
        "fabric_name": "test-fabric",
        "crs": "EPSG:4326",
        "has_up_area": true,
        "has_rasters": false,
        "has_snap": false,
        "terminal_sink_id": 0,
        "topology": "tree",
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": 3,
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "v1.0"
    });
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();

    // Reuse valid data files so only the manifest is the problem.
    let ids = &[1_i64, 2, 3];
    let upstream = vec![vec![], vec![1_i64], vec![2_i64]];
    write_catchments_parquet(dir, ids);
    write_graph_arrow(dir, ids, &upstream);
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[test]
fn valid_dataset_passes() {
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    assert!(
        report.is_valid(),
        "expected valid dataset to pass; errors:\n{}",
        report.display_text()
    );
}

#[test]
fn invalid_manifest_fails() {
    let dir = tempfile::tempdir().unwrap();
    create_invalid_manifest_dataset(dir.path());

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    assert!(
        !report.is_valid(),
        "expected invalid dataset to fail; got:\n{}",
        report.display_text()
    );
    assert!(
        report.error_count() > 0,
        "expected at least one error diagnostic"
    );
}

#[test]
fn empty_directory_fails() {
    let dir = tempfile::tempdir().unwrap();
    // No files at all — must fail (file_presence check)
    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    assert!(
        !report.is_valid(),
        "empty directory should fail; got:\n{}",
        report.display_text()
    );
}

#[test]
fn json_output_is_valid_json() {
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    let json_str = report.display_json();
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).expect("display_json must produce valid JSON");
    assert_eq!(
        parsed["passed"], true,
        "valid dataset should have passed=true in JSON"
    );
    assert!(
        parsed["diagnostics"].is_array(),
        "JSON must contain a diagnostics array"
    );
}

#[test]
fn json_output_for_invalid_dataset_has_passed_false() {
    let dir = tempfile::tempdir().unwrap();
    create_invalid_manifest_dataset(dir.path());

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    let json_str = report.display_json();
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).expect("display_json must produce valid JSON");
    assert_eq!(
        parsed["passed"], false,
        "invalid dataset should have passed=false in JSON"
    );
}

#[test]
fn strict_mode_does_not_break_valid_dataset() {
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    // With strict=true warnings are promoted to errors.  A truly minimal valid
    // dataset should still produce no warnings that get promoted (small parquet
    // row-group warning may or may not fire; either outcome is acceptable so
    // long as the function doesn't panic).
    let _report = hfx_validator::validate(dir.path(), true, true, 100.0);
    // Just verifying no panic.
}

#[test]
fn skip_rasters_true_with_valid_dataset_passes() {
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    assert!(
        report.is_valid(),
        "skip_rasters=true with valid dataset should pass; got:\n{}",
        report.display_text()
    );
}

#[test]
fn mismatched_graph_and_catchments_fails() {
    let dir = tempfile::tempdir().unwrap();

    // catchments has IDs [1, 2, 3] but graph has IDs [1, 2, 4] — ID 3 missing
    // from graph, ID 4 present in graph but not in catchments.
    write_manifest(dir.path(), 3, false, false);
    write_catchments_parquet(dir.path(), &[1_i64, 2, 3]);
    write_graph_arrow(
        dir.path(),
        &[1_i64, 2, 4],
        &[vec![], vec![1_i64], vec![2_i64]],
    );

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    assert!(
        !report.is_valid(),
        "ID mismatch should fail; got:\n{}",
        report.display_text()
    );

    let errors: Vec<_> = report
        .diagnostics()
        .iter()
        .filter(|d| {
            d.check_id == "referential.catchment_not_in_graph"
                || d.check_id == "referential.graph_not_in_catchments"
        })
        .collect();
    assert!(
        !errors.is_empty(),
        "expected referential integrity errors for ID mismatch"
    );
}

#[test]
fn cyclic_graph_fails() {
    let dir = tempfile::tempdir().unwrap();

    // IDs [1, 2, 3]; form a cycle: 1←2←3←1
    write_manifest(dir.path(), 3, false, false);
    write_catchments_parquet(dir.path(), &[1_i64, 2, 3]);
    write_graph_arrow(
        dir.path(),
        &[1_i64, 2, 3],
        &[vec![3_i64], vec![1_i64], vec![2_i64]],
    );

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    assert!(
        !report.is_valid(),
        "cyclic graph should fail; got:\n{}",
        report.display_text()
    );

    let cycle_diags: Vec<_> = report
        .diagnostics()
        .iter()
        .filter(|d| d.check_id == "graph.cycle_detected")
        .collect();
    assert!(
        !cycle_diags.is_empty(),
        "expected graph.cycle_detected diagnostic"
    );
}

#[test]
fn sample_pct_zero_percent_still_runs() {
    // 0% sampling — geometry checks sample at least 1 row so this should still
    // complete without panicking.
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    let _report = hfx_validator::validate(dir.path(), false, true, 0.0);
}

#[test]
fn report_counts_are_consistent() {
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    let report = hfx_validator::validate(dir.path(), false, true, 100.0);
    let expected_total = report.error_count() + report.warning_count() + report.info_count();
    assert_eq!(
        expected_total,
        report.diagnostics().len(),
        "error + warning + info counts must sum to total diagnostics"
    );
}

#[test]
fn binary_json_stdout_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    create_valid_dataset(dir.path());

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_hfx"))
        .arg(dir.path())
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to run hfx binary");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    assert!(
        trimmed.starts_with('{'),
        "stdout must start with '{{' for JSON mode, got first 100 chars: {:?}",
        &trimmed[..trimmed.len().min(100)]
    );
    let parsed: serde_json::Value =
        serde_json::from_str(trimmed).expect("stdout must be valid JSON");
    assert!(
        parsed.get("passed").is_some(),
        "JSON must have 'passed' field"
    );
    assert!(
        parsed.get("diagnostics").is_some(),
        "JSON must have 'diagnostics' field"
    );
}
