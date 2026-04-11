//! Validation check modules.

pub mod file_presence;
pub mod manifest;
pub mod schema;
pub mod ids;
pub mod referential;
pub mod graph;
pub mod values;
pub mod geometry;
pub mod raster;

use crate::dataset::ParsedDataset;
use crate::diagnostic::Diagnostic;

/// Run all validation checks on a parsed dataset.
///
/// Checks are executed in phase order. Later phases may be skipped
/// if earlier phases indicate that required data is missing.
pub fn run_checks(dataset: &ParsedDataset, _strict: bool, _skip_rasters: bool, _sample_pct: f64) -> Vec<Diagnostic> {
    let mut all = Vec::new();

    // Include any diagnostics from the read phase
    all.extend(dataset.read_diagnostics.iter().cloned());

    // TODO: Step 1 will add file_presence and manifest checks
    // TODO: Step 2 will add schema, id, geometry, raster checks
    // TODO: Step 3 will add referential integrity and graph checks
    // TODO: Step 4 will wire everything together

    all
}
