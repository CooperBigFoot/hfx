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

    // Include any diagnostics from the read phase (B1/B2/B3 schema errors come from here).
    all.extend(dataset.read_diagnostics.iter().cloned());

    // Phase 1a: file presence
    let raw_manifest_ref = dataset.raw_manifest.as_ref();
    all.extend(file_presence::check_file_presence(&dataset.files, raw_manifest_ref));

    // Phase 1b: manifest field validation (only when successfully deserialized)
    if let Some(raw) = raw_manifest_ref {
        all.extend(manifest::check_manifest(raw));
    }

    // Phase 2: schema checks (B4-B6)
    all.extend(schema::check_schemas(dataset));

    // TODO: Step 3 will add referential integrity and graph checks
    // TODO: Step 4 will wire everything together

    all
}
