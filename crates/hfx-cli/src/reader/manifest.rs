//! Manifest reader: reads and deserializes `manifest.json` from an HFX dataset directory.

use std::path::Path;

use std::collections::BTreeMap;

use tracing::{debug, warn};

use crate::diagnostic::{Artifact, Category, Diagnostic};

/// Raw deserialized form of `manifest.json`.
///
/// Every field is `Option<T>` so the validator can emit field-level diagnostics
/// for missing fields rather than failing fast on the first absent key.
#[derive(Debug, serde::Deserialize)]
pub struct RawManifest {
    pub format_version: Option<String>,
    pub fabric_name: Option<String>,
    pub fabric_version: Option<String>,
    pub crs: Option<String>,
    pub has_up_area: Option<bool>,
    pub topology: Option<String>,
    pub region: Option<String>,
    pub bbox: Option<Vec<f64>>,
    pub unit_count: Option<u64>,
    pub created_at: Option<String>,
    pub adapter_version: Option<String>,
    pub auxiliary: Option<Vec<RawAuxEntry>>,
}

/// Raw deserialized form of one `auxiliary[]` entry.
#[derive(Debug, serde::Deserialize)]
pub struct RawAuxEntry {
    pub schema: Option<String>,
    pub artifacts: Option<BTreeMap<String, String>>,
    pub metadata: Option<serde_json::Value>,
}

/// Read `manifest.json` at `path` and return (raw JSON value, raw struct, diagnostics).
///
/// The three-tuple design lets callers keep partial information at each stage:
/// - Stage 1 failure (file read): all three are empty / None.
/// - Stage 2 failure (JSON parse): `json_value` is None, struct is None.
/// - Stage 3 failure (serde shape): `json_value` is Some, struct is None.
/// - Success: both `json_value` and `raw` are Some, diagnostics is empty.
pub fn read_manifest(
    path: &Path,
) -> (
    Option<serde_json::Value>,
    Option<RawManifest>,
    Vec<Diagnostic>,
) {
    debug!(path = %path.display(), "reading manifest.json");

    // Stage 1: read file bytes.
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot read manifest.json");
            return (
                None,
                None,
                vec![Diagnostic::error(
                    "manifest.read",
                    Category::Manifest,
                    Artifact::Manifest,
                    format!("cannot read manifest.json: {err}"),
                )],
            );
        }
    };

    // Stage 2: parse as JSON Value.
    let json_value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "manifest.json is not valid JSON");
            return (
                None,
                None,
                vec![Diagnostic::error(
                    "manifest.json_parse",
                    Category::Manifest,
                    Artifact::Manifest,
                    format!("manifest.json is not valid JSON: {err}"),
                )],
            );
        }
    };

    // Stage 3: deserialize into RawManifest.
    let raw: RawManifest = match serde_json::from_value(json_value.clone()) {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, "manifest.json has unexpected shape");
            return (
                Some(json_value),
                None,
                vec![Diagnostic::error(
                    "manifest.deserialize",
                    Category::Manifest,
                    Artifact::Manifest,
                    format!("manifest.json has unexpected shape: {err}"),
                )],
            );
        }
    };

    debug!("manifest.json read and deserialized successfully");
    (Some(json_value), Some(raw), vec![])
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::read_manifest;

    #[test]
    fn bbox_literal_is_correctly_rounded_through_manifest_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        fs::write(&path, r#"{"bbox":[0,0,-11.442605018615723,0]}"#).unwrap();

        let (_, raw, diagnostics) = read_manifest(&path);

        assert!(diagnostics.is_empty(), "{diagnostics:#?}");
        let parsed = raw.unwrap().bbox.unwrap()[2];
        let expected = "-11.442605018615723".parse::<f64>().unwrap();
        assert_eq!(expected.to_bits(), 0xc026e29d20000000);
        assert_eq!(parsed.to_bits(), expected.to_bits());
    }
}
