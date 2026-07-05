//! GATE-1 spike test (milestone alden-feedback, step s04).
//!
//! Proves the `parquet` crate (workspace pin v54) reads Parquet row-group
//! statistics on the LEAF fields of a struct `bbox` column
//! {xmin, ymin, xmax, ymax}. The struct parquet is built in-process with
//! `arrow` (no Python, no committed binary) so the gate is hermetic.
//!
//! IMPORTANT: this test does NOT touch the validator's production read path
//! (reader/catchments.rs still expects flat bbox until s07). It exercises the
//! parquet + arrow crates directly to de-risk the s07 leaf-stats migration.
//!
//! Note for s07: struct leaves collapse their `name()` to the leaf name
//! ("xmin"); the full path "bbox.xmin" is only available via
//! `column(i).path().string()`. Match on the path, not on `name()`.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Int64Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Full dotted paths of the four covering-bbox struct leaves.
const LEAF_PATHS: [&str; 4] = ["bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax"];

fn write_struct_bbox_parquet(path: &Path) {
    let leaf_fields: Fields = vec![
        Arc::new(Field::new("xmin", DataType::Float32, false)),
        Arc::new(Field::new("ymin", DataType::Float32, false)),
        Arc::new(Field::new("xmax", DataType::Float32, false)),
        Arc::new(Field::new("ymax", DataType::Float32, false)),
    ]
    .into();

    let xmin = Arc::new(Float32Array::from(vec![0.0f32, 1.0, 2.0, 3.0])) as ArrayRef;
    let ymin = Arc::new(Float32Array::from(vec![0.5f32, 1.5, 2.5, 3.5])) as ArrayRef;
    let xmax = Arc::new(Float32Array::from(vec![1.0f32, 2.0, 3.0, 4.0])) as ArrayRef;
    let ymax = Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5, 4.5])) as ArrayRef;

    let bbox = StructArray::new(leaf_fields.clone(), vec![xmin, ymin, xmax, ymax], None);
    let id = Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("bbox", DataType::Struct(leaf_fields), false),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![id, Arc::new(bbox) as ArrayRef]).unwrap();

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(2)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn struct_bbox_leaf_row_group_stats_are_present() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("proto_struct_bbox.parquet");
    write_struct_bbox_parquet(&path);

    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let meta = reader.metadata();
    assert!(
        meta.num_row_groups() >= 2,
        "expected at least two row groups"
    );

    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        let schema_desc = rg.schema_descr();
        for leaf in LEAF_PATHS {
            let col_idx = (0..schema_desc.num_columns())
                .find(|&i| schema_desc.column(i).path().string() == leaf)
                .unwrap_or_else(|| panic!("leaf column '{leaf}' not found in schema"));
            let stats = rg
                .column(col_idx)
                .statistics()
                .unwrap_or_else(|| panic!("row group {rg_idx}: leaf '{leaf}' has no statistics"));
            assert!(
                stats.min_bytes_opt().is_some(),
                "row group {rg_idx}: leaf '{leaf}' missing min"
            );
            assert!(
                stats.max_bytes_opt().is_some(),
                "row group {rg_idx}: leaf '{leaf}' missing max"
            );
        }
    }
}
