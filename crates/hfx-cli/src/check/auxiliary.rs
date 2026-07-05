//! Auxiliary declaration checks.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::check::{geometry, ids, referential};
use crate::dataset::{CatchmentsData, SnapData};
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader;
use crate::reader::manifest::{RawAuxEntry, RawManifest};

/// Validate auxiliary declarations structurally and dispatch blessed schemas.
pub fn check_auxiliary(
    raw: &RawManifest,
    dataset_root: &Path,
    catchments: Option<&CatchmentsData>,
) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let Some(entries) = raw.auxiliary.as_ref() else {
        return diags;
    };

    let mut snap_names = HashSet::new();

    for (idx, entry) in entries.iter().enumerate() {
        if entry.schema.as_deref() == Some("hfx.aux.snap.v2")
            && let Some(name) = snap_name(entry)
            && !snap_names.insert(name.to_string())
        {
            diags.push(
                Diagnostic::error(
                    "aux.snap.duplicate_name",
                    Category::Manifest,
                    Artifact::Manifest,
                    format!("hfx.aux.snap.v2 metadata.name {name:?} is duplicated"),
                )
                .at(Location::Field {
                    name: "auxiliary".into(),
                }),
            );
        }
        check_entry(idx, entry, dataset_root, catchments, &mut diags);
    }

    diags
}

fn check_entry(
    idx: usize,
    entry: &RawAuxEntry,
    dataset_root: &Path,
    catchments: Option<&CatchmentsData>,
    diags: &mut Vec<Diagnostic>,
) {
    let Some(artifacts) = entry.artifacts.as_ref() else {
        return;
    };
    let label = entry_label(idx, entry);

    for (key, rel_path) in artifacts {
        let path = dataset_root.join(rel_path);
        if rel_path.starts_with('/') || rel_path.contains("..") {
            diags.push(
                Diagnostic::error(
                    "auxiliary.path_escape",
                    Category::FilePresence,
                    Artifact::Manifest,
                    format!(
                        "{label} artifact {key:?} path {rel_path:?} is not a safe relative path"
                    ),
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
                    format!("{label} artifact {key:?} path {rel_path:?} is missing"),
                )
                .at(Location::Field {
                    name: "auxiliary".into(),
                }),
            );
        }
    }

    if entry.schema.as_deref() == Some("hfx.aux.d8_raster.v1") {
        check_d8_raster(idx, entry, diags);
    } else if entry.schema.as_deref() == Some("hfx.aux.snap.v2") {
        check_snap_v2(idx, entry, dataset_root, catchments, diags);
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

fn check_snap_v2(
    idx: usize,
    entry: &RawAuxEntry,
    dataset_root: &Path,
    catchments: Option<&CatchmentsData>,
    diags: &mut Vec<Diagnostic>,
) {
    let label = snap_label(idx, entry);
    let Some(rel_path) = entry.artifacts.as_ref().and_then(|a| a.get("snap")) else {
        return;
    };
    if rel_path.starts_with('/') || rel_path.contains("..") {
        return;
    }

    let path = dataset_root.join(rel_path);
    let (snap, read_diags) = reader::snap::read_snap(&path, &label);
    diags.extend(read_diags);

    let Some(snap) = snap else {
        return;
    };

    let mut snap_diags = check_snap_data_for_aux(&snap);
    snap_diags.extend(check_snap_stem_roles(&snap));
    snap_diags.extend(geometry::check_snap_geometries(&snap));

    if let Some(catchments) = catchments {
        snap_diags.extend(referential::check_snap_refs(catchments, &snap));
        snap_diags.extend(check_snap_declared_levels(catchments, &snap, entry));
    }

    prefix_diagnostics(&label, &mut snap_diags);
    diags.extend(snap_diags);
}

fn snap_label(idx: usize, entry: &RawAuxEntry) -> String {
    snap_name(entry)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("auxiliary[{idx}]"))
}

fn snap_name(entry: &RawAuxEntry) -> Option<&str> {
    entry
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("name"))
        .and_then(serde_json::Value::as_str)
}

fn entry_label(idx: usize, entry: &RawAuxEntry) -> String {
    if entry.schema.as_deref() == Some("hfx.aux.snap.v2") {
        snap_label(idx, entry)
    } else {
        format!("auxiliary[{idx}]")
    }
}

fn references_levels(entry: &RawAuxEntry) -> HashSet<i16> {
    entry
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("references_levels"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_i64)
        .filter_map(|level| i16::try_from(level).ok())
        .collect()
}

fn check_snap_stem_roles(snap: &SnapData) -> Vec<Diagnostic> {
    snap.stem_roles
        .iter()
        .enumerate()
        .filter_map(|(row, role)| {
            let role = role.as_deref()?;
            (!matches!(role, "mainstem" | "tributary" | "distributary" | "unknown")).then(|| {
                Diagnostic::error(
                    "aux.snap.stem_role_invalid",
                    Category::IdConstraint,
                    Artifact::Snap,
                    format!("snap stem_role {role:?} at row {row} is not a supported value"),
                )
                .at(Location::Row { index: row })
            })
        })
        .collect()
}

fn check_snap_data_for_aux(snap: &SnapData) -> Vec<Diagnostic> {
    let mut diags = ids::check_snap_data(snap);
    for diag in &mut diags {
        if diag.check_id == "ids.snap_weight" {
            diag.check_id = "aux.snap.weight_invalid";
        }
    }
    diags
}

fn check_snap_declared_levels(
    catchments: &CatchmentsData,
    snap: &SnapData,
    entry: &RawAuxEntry,
) -> Vec<Diagnostic> {
    let declared = references_levels(entry);
    let levels_by_id: HashMap<i64, i16> = catchments
        .ids
        .iter()
        .copied()
        .zip(catchments.levels.iter().copied())
        .collect();

    snap.unit_ids
        .iter()
        .enumerate()
        .filter_map(|(row, unit_id)| {
            let level = levels_by_id.get(unit_id)?;
            (!declared.contains(level)).then(|| {
                Diagnostic::error(
                    "aux.snap.level_not_declared",
                    Category::ReferentialIntegrity,
                    Artifact::CrossFile,
                    format!(
                        "snap unit_id {unit_id} at row {row} references level {level}, \
                         which is not declared in metadata.references_levels"
                    ),
                )
                .at(Location::Row { index: row })
            })
        })
        .collect()
}

fn prefix_diagnostics(label: &str, diags: &mut [Diagnostic]) {
    for diag in diags {
        if !diag.message.starts_with(label) {
            diag.message = format!("{label}: {}", std::mem::take(&mut diag.message));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dataset::{CatchmentsData, SnapData};
    use crate::diagnostic::{Artifact, Severity};
    use crate::reader::manifest::RawAuxEntry;

    use super::{
        check_auxiliary, check_snap_data_for_aux, check_snap_declared_levels, check_snap_stem_roles,
    };

    fn snap_with_roles(stem_roles: Vec<Option<&str>>) -> SnapData {
        let row_count = stem_roles.len();
        SnapData {
            row_count,
            ids: (1..=row_count as i64).collect(),
            unit_ids: vec![1; row_count],
            weights: vec![1.0; row_count],
            stem_roles: stem_roles
                .into_iter()
                .map(|role| role.map(str::to_owned))
                .collect(),
            bboxes: vec![None; row_count],
            geometry_wkb: vec![Vec::new(); row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn catchments_with_levels(ids: Vec<i64>, levels: Vec<i16>) -> CatchmentsData {
        let row_count = ids.len();
        CatchmentsData {
            row_count,
            ids,
            levels,
            parent_ids: vec![None; row_count],
            areas_km2: vec![1.0; row_count],
            outlet_lons: vec![0.0; row_count],
            outlet_lats: vec![0.0; row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            up_area_null_count: row_count,
            first_up_area_non_null_row: None,
            up_area_total: row_count,
            geometry_wkb: vec![Vec::new(); row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    #[test]
    fn unsupported_snap_stem_role_emits_aux_diagnostic() {
        let diagnostics = check_snap_stem_roles(&snap_with_roles(vec![Some("primary")]));

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "aux.snap.stem_role_invalid"
                && diag.severity == Severity::Error
                && diag.artifact == Artifact::Snap
        }));
    }

    #[test]
    fn invalid_snap_weight_emits_aux_diagnostic() {
        let snap = SnapData {
            weights: vec![-1.0],
            ..snap_with_roles(vec![Some("mainstem")])
        };
        let diagnostics = check_snap_data_for_aux(&snap);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "aux.snap.weight_invalid"
                && diag.severity == Severity::Error
                && diag.artifact == Artifact::Snap
        }));
    }

    #[test]
    fn snap_unit_referencing_undeclared_level_emits_aux_diagnostic() {
        let snap = SnapData {
            unit_ids: vec![2],
            ..snap_with_roles(vec![Some("mainstem")])
        };
        let catchments = catchments_with_levels(vec![2], vec![1]);
        let entry = RawAuxEntry {
            schema: Some("hfx.aux.snap.v2".to_string()),
            artifacts: None,
            metadata: Some(serde_json::json!({
                "references_levels": [0]
            })),
        };

        let diagnostics = check_snap_declared_levels(&catchments, &snap, &entry);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "aux.snap.level_not_declared"
                && diag.severity == Severity::Error
                && diag.artifact == Artifact::CrossFile
        }));
    }

    #[test]
    fn duplicate_snap_metadata_name_emits_aux_diagnostic() {
        let raw = crate::reader::manifest::RawManifest {
            format_version: None,
            fabric_name: None,
            fabric_version: None,
            crs: None,
            has_up_area: None,
            topology: None,
            region: None,
            bbox: None,
            unit_count: None,
            created_at: None,
            adapter_version: None,
            auxiliary: Some(vec![
                RawAuxEntry {
                    schema: Some("hfx.aux.snap.v2".to_string()),
                    artifacts: Some(Default::default()),
                    metadata: Some(serde_json::json!({"name": "reach-stems"})),
                },
                RawAuxEntry {
                    schema: Some("hfx.aux.snap.v2".to_string()),
                    artifacts: Some(Default::default()),
                    metadata: Some(serde_json::json!({"name": "reach-stems"})),
                },
            ]),
        };

        let diagnostics = check_auxiliary(&raw, std::path::Path::new("."), None);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "aux.snap.duplicate_name"
                && diag.severity == Severity::Error
                && diag.artifact == Artifact::Manifest
        }));
    }
}
