//! bounded_geometry : ParquetRows → retained payload and diagnostic equivalence proofs.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, UInt32Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;

use hfx_cli::check::geometry::{check_catchment_geometries, check_snap_geometries};
use hfx_cli::dataset::{GeometryRetention, GeometrySelection};
use hfx_cli::diagnostic::{Category, Diagnostic, Location};
use hfx_cli::reader::{catchments, snap};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/valid")
        .join(name)
}

fn write_rows(path: &Path, source: &Path, rows: usize) {
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(source).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let seed = reader.next().unwrap().unwrap();
    let indices = UInt32Array::from(vec![0; rows]);
    let geometry_index = seed.schema().index_of("geometry").unwrap();
    // Every row is invalid, with different failure layers across batch boundaries.
    let values: Vec<&[u8]> = (0..rows)
        .map(|row| match row % 3 {
            0 => &b""[..],
            1 if source.file_name().unwrap() == "segment_stems.parquet" => {
                &b"\x01\x01\x00\x00\x00"[..]
            }
            1 => &b"\x01\x03\x00\x00\x00"[..],
            _ => &b"\x01\x07\x00\x00\x00"[..],
        })
        .collect();
    let mut columns: Vec<ArrayRef> = seed
        .columns()
        .iter()
        .map(|column| take(column, &indices, None).unwrap())
        .collect();
    columns[geometry_index] = Arc::new(BinaryArray::from_vec(values));
    let batch = RecordBatch::try_new(seed.schema(), columns).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(9000)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn fingerprints(diags: &[Diagnostic]) -> Vec<String> {
    diags.iter().map(|d| format!("{d:?}")).collect()
}

#[test]
fn every_catchment_row_checked_across_batches_without_retained_payloads() {
    for rows in [0, 1, 18003] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        write_rows(&path, &fixture("tiny/catchments.parquet"), rows);
        let (buffered, before) = catchments::read_catchments(&path);
        let (checked, after) =
            catchments::read_catchments_with_geometry(&path, GeometrySelection::ValidateAll);
        assert_eq!(fingerprints(&before), fingerprints(&after));
        let buffered = buffered.unwrap();
        let checked = checked.unwrap();
        assert_eq!(checked.row_count, rows);
        let GeometryRetention::Checked(stored) = &checked.geometry else {
            panic!("WKB retained")
        };
        assert_eq!(stored.len(), rows);
        let expected = check_catchment_geometries(&buffered, 100.0);
        assert_eq!(fingerprints(stored), fingerprints(&expected));
        for (index, diagnostic) in stored.iter().enumerate() {
            assert!(matches!(diagnostic.location, Location::Row { index: row } if row == index));
        }
        let sampled = check_catchment_geometries(&buffered, 1.0);
        assert_eq!(
            sampled.len(),
            if rows == 0 {
                0
            } else {
                (rows as f64 / 100.0).ceil() as usize
            }
        );
        let full = hfx_cli::validate(dir.path(), false, true, 100.0);
        let geometry_start = full
            .diagnostics()
            .iter()
            .position(|d| d.category == Category::Geometry);
        if let Some(start) = geometry_start {
            assert!(
                full.diagnostics()[start..]
                    .iter()
                    .all(|d| d.category == Category::Geometry)
            );
        }
        let legacy_dataset = hfx_cli::reader::read_dataset_with_options(dir.path(), true);
        let legacy = hfx_cli::check::run_checks(&legacy_dataset, false, true, 100.0);
        assert_eq!(fingerprints(full.diagnostics()), fingerprints(&legacy));
        let mut expected_strict = hfx_cli::report::ValidationReport::new(legacy);
        expected_strict.promote_warnings();
        let strict = hfx_cli::validate(dir.path(), true, true, 100.0);
        assert_eq!(
            fingerprints(strict.diagnostics()),
            fingerprints(expected_strict.diagnostics())
        );
    }
}

#[test]
fn every_snap_row_checked_without_retained_payloads() {
    let source = fixture("grit-two-snap/snap/segment_stems.parquet");
    for rows in [0, 1, 18003] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.parquet");
        write_rows(&path, &source, rows);
        let (buffered, before) = snap::read_snap(&path, "test entry");
        let (checked, after) =
            snap::read_snap_with_geometry(&path, "test entry", GeometrySelection::ValidateAll);
        assert_eq!(fingerprints(&before), fingerprints(&after));
        let checked = checked.unwrap();
        let GeometryRetention::Checked(stored) = &checked.geometry else {
            panic!("WKB retained")
        };
        assert_eq!(stored.len(), rows);
        assert_eq!(
            fingerprints(stored),
            fingerprints(&check_snap_geometries(&buffered.unwrap()))
        );
    }
}

#[test]
fn corrupted_parquet_discards_pending_geometry_diagnostics() {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::io::{Seek, SeekFrom, Write};

    for (source, artifact) in [
        ("tiny/catchments.parquet", "catchments"),
        ("grit-two-snap/snap/segment_stems.parquet", "snap"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.parquet");
        write_rows(&path, &fixture(source), 45003);
        let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        let meta = reader.metadata();
        let offsets: Vec<u64> = (1..meta.num_row_groups())
            .map(|i| {
                let rg = meta.row_group(i);
                rg.column(rg.num_columns() - 1).byte_range().0
            })
            .collect();
        drop(reader);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        for offset in offsets {
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&[255; 32]).unwrap();
        }
        drop(file);
        let (missing, diagnostics) = if artifact == "catchments" {
            let (data, diags) =
                catchments::read_catchments_with_geometry(&path, GeometrySelection::ValidateAll);
            (data.is_none(), diags)
        } else {
            let (data, diags) = snap::read_snap_with_geometry(
                &path,
                "broken entry",
                GeometrySelection::ValidateAll,
            );
            (data.is_none(), diags)
        };
        assert!(missing, "corrupt file must abort");
        assert!(
            diagnostics
                .iter()
                .any(|d| d.check_id.ends_with("batch_read_aborted"))
        );
        assert!(diagnostics.iter().all(|d| d.category != Category::Geometry));
    }
}

#[test]
fn null_geometry_declaration_fails_before_extraction_in_both_modes() {
    use arrow::datatypes::Schema;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("null.parquet");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(fixture("tiny/catchments.parquet")).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let seed = reader.next().unwrap().unwrap();
    let index = seed.schema().index_of("geometry").unwrap();
    let mut fields = seed.schema().fields().to_vec();
    fields[index] = Arc::new(fields[index].as_ref().clone().with_nullable(true));
    let schema = Arc::new(Schema::new(fields));
    let mut columns = seed.columns().to_vec();
    columns[index] = Arc::new(BinaryArray::from(vec![None::<&[u8]>; seed.num_rows()]));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let (before, before_diags) = catchments::read_catchments(&path);
    let (after, after_diags) =
        catchments::read_catchments_with_geometry(&path, GeometrySelection::ValidateAll);
    assert!(before.is_none() && after.is_none());
    assert!(!before_diags.is_empty());
    assert_eq!(fingerprints(&before_diags), fingerprints(&after_diags));
    assert!(after_diags.iter().all(|d| d.category != Category::Geometry));
}

#[test]
fn only_exact_full_coverage_selects_streaming() {
    assert!(matches!(
        GeometrySelection::from_sample_pct(100.0),
        GeometrySelection::ValidateAll
    ));
    for pct in [0.0, 1.0, 99.99, 100.01, f64::NAN, f64::INFINITY, -1.0] {
        assert!(matches!(
            GeometrySelection::from_sample_pct(pct),
            GeometrySelection::Buffered
        ));
    }
}
