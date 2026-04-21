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
    let dataset = reader::read_dataset(dir);
    let diagnostics = check::run_checks(&dataset, strict, skip_rasters, sample_pct);
    let mut report = ValidationReport::new(diagnostics);
    if strict {
        report.promote_warnings();
    }
    report
}
