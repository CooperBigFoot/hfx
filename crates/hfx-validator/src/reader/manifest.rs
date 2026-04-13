//! Manifest reader: reads and deserializes `manifest.json` from an HFX dataset directory.

use std::path::Path;

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
    pub fabric_level: Option<u32>,
    pub crs: Option<String>,
    pub has_up_area: Option<bool>,
    pub has_rasters: Option<bool>,
    pub has_snap: Option<bool>,
    pub flow_dir_encoding: Option<String>,
    pub terminal_sink_id: Option<i64>,
    pub topology: Option<String>,
    pub region: Option<String>,
    pub bbox: Option<Vec<f64>>,
    pub atom_count: Option<u64>,
    pub created_at: Option<String>,
    pub adapter_version: Option<String>,
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
) -> (Option<serde_json::Value>, Option<RawManifest>, Vec<Diagnostic>) {
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
    use super::*;
    use std::io::Write as _;

    fn write_temp(content: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::File::create(&path).unwrap().write_all(content).unwrap();
        (dir, path)
    }

    #[test]
    fn valid_manifest_deserializes() {
        let json = serde_json::json!({
            "format_version": "0.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "has_up_area": true,
            "has_rasters": false,
            "has_snap": false,
            "terminal_sink_id": 0,
            "topology": "tree",
            "bbox": [-180.0, -90.0, 180.0, 90.0],
            "atom_count": 42,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "v1.0"
        });
        let (_dir, path) = write_temp(json.to_string().as_bytes());

        let (val, raw, diags) = read_manifest(&path);
        assert!(val.is_some(), "should have JSON value");
        assert!(raw.is_some(), "should have RawManifest");
        assert!(diags.is_empty(), "no diagnostics expected");

        let raw = raw.unwrap();
        assert_eq!(raw.format_version.as_deref(), Some("0.1"));
        assert_eq!(raw.fabric_name.as_deref(), Some("testfabric"));
        assert_eq!(raw.atom_count, Some(42));
        assert_eq!(raw.has_up_area, Some(true));
        assert_eq!(raw.has_rasters, Some(false));
    }

    #[test]
    fn missing_file_produces_error_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");

        let (val, raw, diags) = read_manifest(&path);
        assert!(val.is_none());
        assert!(raw.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "manifest.read");
    }

    #[test]
    fn invalid_json_produces_error_diagnostic() {
        let (_dir, path) = write_temp(b"{ not valid json }");

        let (val, raw, diags) = read_manifest(&path);
        assert!(val.is_none());
        assert!(raw.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "manifest.json_parse");
    }

    #[test]
    fn wrong_shape_produces_error_and_keeps_json_value() {
        // fabric_level must be a u32; giving it a string breaks deserialization.
        let bad = serde_json::json!({ "fabric_level": "not-a-number" });
        let (_dir, path) = write_temp(bad.to_string().as_bytes());

        let (val, raw, diags) = read_manifest(&path);
        assert!(val.is_some(), "json_value preserved even on shape error");
        assert!(raw.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "manifest.deserialize");
    }

    #[test]
    fn all_optional_fields_absent_still_deserializes() {
        // An empty JSON object is valid for RawManifest — all fields are Option.
        let (_dir, path) = write_temp(b"{}");

        let (val, raw, diags) = read_manifest(&path);
        assert!(val.is_some());
        let raw = raw.expect("empty object should deserialize fine");
        assert!(diags.is_empty());
        assert!(raw.format_version.is_none());
        assert!(raw.atom_count.is_none());
    }
}
