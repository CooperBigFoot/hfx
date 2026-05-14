//! Auxiliary declaration checks.

use std::path::Path;

use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::manifest::{RawAuxEntry, RawManifest};

/// Validate auxiliary declarations structurally and dispatch blessed schemas.
pub fn check_auxiliary(raw: &RawManifest, dataset_root: &Path) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let Some(entries) = raw.auxiliary.as_ref() else {
        return diags;
    };

    for (idx, entry) in entries.iter().enumerate() {
        check_entry(idx, entry, dataset_root, &mut diags);
    }

    diags
}

fn check_entry(idx: usize, entry: &RawAuxEntry, dataset_root: &Path, diags: &mut Vec<Diagnostic>) {
    let Some(artifacts) = entry.artifacts.as_ref() else {
        return;
    };

    for (key, rel_path) in artifacts {
        let path = dataset_root.join(rel_path);
        if rel_path.starts_with('/') || rel_path.contains("..") {
            diags.push(
                Diagnostic::error(
                    "auxiliary.path_escape",
                    Category::FilePresence,
                    Artifact::Manifest,
                    format!("auxiliary[{idx}] artifact {key:?} path {rel_path:?} is not a safe relative path"),
                )
                .at(Location::Field {
                    name: "auxiliary".into(),
                }),
            );
        } else if !path.exists() {
            diags.push(
                Diagnostic::error(
                    "auxiliary.missing_artifact",
                    Category::FilePresence,
                    Artifact::Manifest,
                    format!("auxiliary[{idx}] artifact {key:?} path {rel_path:?} is missing"),
                )
                .at(Location::Field {
                    name: "auxiliary".into(),
                }),
            );
        }
    }

    if entry.schema.as_deref() == Some("hfx.aux.d8_raster.v1") {
        check_d8_raster(idx, entry, diags);
    }
}

fn check_d8_raster(idx: usize, entry: &RawAuxEntry, diags: &mut Vec<Diagnostic>) {
    let artifacts = entry.artifacts.as_ref();
    for key in ["flow_dir", "flow_acc"] {
        if !artifacts.is_some_and(|a| a.contains_key(key)) {
            diags.push(
                Diagnostic::error(
                    "auxiliary.d8_raster_missing_artifact_key",
                    Category::Manifest,
                    Artifact::Manifest,
                    format!("hfx.aux.d8_raster.v1 auxiliary[{idx}] missing artifact key {key:?}"),
                )
                .at(Location::Field {
                    name: "auxiliary".into(),
                }),
            );
        }
    }

    let encoding = entry
        .metadata
        .as_ref()
        .and_then(|m| m.get("flow_dir_encoding"))
        .and_then(serde_json::Value::as_str);
    if !matches!(encoding, Some("esri" | "taudem")) {
        diags.push(
            Diagnostic::error(
                "auxiliary.d8_raster_flow_dir_encoding",
                Category::Manifest,
                Artifact::Manifest,
                format!("hfx.aux.d8_raster.v1 auxiliary[{idx}] requires flow_dir_encoding \"esri\" or \"taudem\""),
            )
            .at(Location::Field {
                name: "auxiliary".into(),
            }),
        );
    }
}
