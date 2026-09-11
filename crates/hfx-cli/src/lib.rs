//! HFX dataset validator library.

pub mod check;
pub mod dataset;
pub mod diagnostic;
pub mod reader;
pub mod report;

use std::path::Path;

use report::ValidationReport;

/// Validate an HFX dataset directory, returning a full report.
#[tracing::instrument(skip_all, fields(dir = %dir.display()))]
pub fn validate(dir: &Path, strict: bool, skip_rasters: bool, sample_pct: f64) -> ValidationReport {
    let dataset = reader::read_dataset_with_geometry(
        dir,
        skip_rasters,
        dataset::GeometrySelection::from_sample_pct(sample_pct),
    );
    let diagnostics = check::run_checks(&dataset, strict, skip_rasters, sample_pct);
    let mut report = ValidationReport::new(diagnostics);
    if strict {
        report.promote_warnings();
    }
    report
}

#[cfg(test)]
mod bounded_geometry_tests {
    #[test]
    fn full_parquet_read_retains_no_geometry_payload() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/valid/tiny");
        let dataset = crate::reader::read_dataset_with_geometry(
            &path,
            true,
            crate::dataset::GeometrySelection::ValidateAll,
        );
        let data = dataset.catchments.expect("real Parquet fixture must read");
        assert!(data.row_count > 0);
        let retained_bytes: usize = match &data.geometry {
            crate::dataset::GeometryRetention::Buffered(payloads) => {
                payloads.iter().map(Vec::len).sum()
            }
            crate::dataset::GeometryRetention::Checked(_) => 0,
        };
        assert_eq!(retained_bytes, 0, "full reader retained WKB payload bytes");
    }
    #[test]
    fn multi_batch_reader_instruments_actual_payload_copies_and_checks() {
        use crate::dataset::{GEOMETRY_CHECKED_ROWS, GEOMETRY_COPIED_BYTES, GeometrySelection};
        use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
        use std::fs::File;
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../conformance/valid/tiny/catchments.parquet");
        let seed = ParquetRecordBatchReaderBuilder::try_new(File::open(source).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), seed.schema(), None).unwrap();
        for _ in 0..4000 {
            writer.write(&seed).unwrap();
        }
        writer.close().unwrap();
        let rows = seed.num_rows() * 4000;
        assert!(rows > 8192);
        GEOMETRY_COPIED_BYTES.with(|count| count.set(0));
        GEOMETRY_CHECKED_ROWS.with(|count| count.set(0));
        let (checked, _) = crate::reader::catchments::read_catchments_with_geometry(
            &path,
            GeometrySelection::ValidateAll,
        );
        assert_eq!(checked.unwrap().row_count, rows);
        GEOMETRY_COPIED_BYTES
            .with(|count| assert_eq!(count.get(), 0, "stream copied WKB at an Arrow row boundary"));
        GEOMETRY_CHECKED_ROWS.with(|count| assert_eq!(count.get(), rows));
        let (buffered, _) = crate::reader::catchments::read_catchments(&path);
        let crate::dataset::GeometryRetention::Buffered(payloads) = buffered.unwrap().geometry
        else {
            panic!("buffered API changed")
        };
        let retained: usize = payloads.iter().map(Vec::len).sum();
        assert!(retained > 0);
        GEOMETRY_COPIED_BYTES.with(|count| assert_eq!(count.get(), retained));
    }
}
