//! HFX dataset validator library.

pub mod diagnostic;
pub mod report;
pub mod dataset;
pub mod reader;
pub mod check;

use std::path::Path;

use report::ValidationReport;

/// Validate an HFX dataset directory, returning a full report.
pub fn validate(dir: &Path, strict: bool, skip_rasters: bool, sample_pct: f64) -> ValidationReport {
    let dataset = reader::read_dataset(dir);
    let diagnostics = check::run_checks(&dataset, strict, skip_rasters, sample_pct);
    let mut report = ValidationReport::new(diagnostics);
    if strict {
        report.promote_warnings();
    }
    report
}
