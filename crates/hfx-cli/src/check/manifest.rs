//! Manifest field validation for HFX v0.3.0.

use std::collections::BTreeMap;

use hfx::{
    AuxiliaryDecl, AuxiliarySchemaId, BoundingBox, Crs, FormatVersion, ManifestBuilder, Topology,
    UnitCount,
};
use tracing::debug;

use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::manifest::{RawAuxEntry, RawManifest};

/// Check every field in `raw` against the HFX v0.3.0 manifest contract.
pub fn check_manifest(raw: &RawManifest) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    check_format_version(raw, &mut diags);
    if raw.format_version.as_deref().is_some_and(|v| v != "0.3.0") {
        debug!(
            count = diags.len(),
            "manifest short-circuited on unsupported format"
        );
        return diags;
    }

    check_fabric_name(raw, &mut diags);
    check_crs(raw, &mut diags);
    check_topology(raw, &mut diags);
    check_has_up_area(raw, &mut diags);
    check_bbox(raw, &mut diags);
    check_unit_count(raw, &mut diags);
    check_created_at(raw, &mut diags);
    check_adapter_version(raw, &mut diags);
    check_auxiliary(raw, &mut diags);

    debug!(count = diags.len(), "manifest field checks complete");
    diags
}

/// Attempt to construct an [`hfx::Manifest`] from a `RawManifest`.
pub fn try_build_manifest(raw: &RawManifest) -> Option<hfx::Manifest> {
    if raw.format_version.as_deref()? != "0.3.0" {
        return None;
    }

    let format_version = raw
        .format_version
        .as_deref()?
        .parse::<FormatVersion>()
        .ok()?;
    let fabric_name = raw.fabric_name.as_deref()?;
    let crs = raw.crs.as_deref()?.parse::<Crs>().ok()?;
    let topology = raw.topology.as_deref()?.parse::<Topology>().ok()?;
    let unit_count = UnitCount::new(raw.unit_count?).ok()?;
    let created_at = raw.created_at.as_deref()?;
    let adapter_version = raw.adapter_version.as_deref()?;
    let bbox_vec = raw.bbox.as_deref()?;
    if bbox_vec.len() != 4 {
        return None;
    }
    let bbox = BoundingBox::new(
        bbox_vec[0] as f32,
        bbox_vec[1] as f32,
        bbox_vec[2] as f32,
        bbox_vec[3] as f32,
    )
    .ok()?;

    let mut builder = ManifestBuilder::new(
        format_version,
        fabric_name,
        crs,
        topology,
        bbox,
        unit_count,
        created_at,
        adapter_version,
    )
    .ok()?;

    if raw.has_up_area == Some(true) {
        builder = builder.with_up_area();
    }
    if let Some(fv) = raw.fabric_version.as_deref() {
        builder = builder.with_fabric_version(fv);
    }
    if let Some(region) = raw.region.as_deref() {
        builder = builder.with_region(region);
    }
    if let Some(entries) = raw.auxiliary.as_ref() {
        for entry in entries {
            let schema = entry.schema.as_deref()?.parse::<AuxiliarySchemaId>().ok()?;
            let artifacts = entry.artifacts.clone().unwrap_or_default();
            let auxiliary = AuxiliaryDecl::new(schema, artifacts).ok()?;
            builder = builder.with_auxiliary(auxiliary);
        }
    }

    Some(builder.build())
}

fn check_format_version(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.format_version.as_deref() {
        None => push_field(
            diags,
            "manifest.format_version",
            "format_version",
            "missing required field: format_version",
        ),
        Some("0.3.0") => {}
        Some(v) => diags.push(
            Diagnostic::error(
                "manifest.unsupported_format_version",
                Category::Manifest,
                Artifact::Manifest,
                format!(
                    "dataset is v{v}; this validator accepts only HFX 0.3.0 - upgrade your producer"
                ),
            )
            .at(Location::Field {
                name: "format_version".into(),
            }),
        ),
    }
}

fn check_fabric_name(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.fabric_name.as_deref() {
        None => push_field(
            diags,
            "manifest.fabric_name",
            "fabric_name",
            "missing required field: fabric_name",
        ),
        Some("") => push_field(
            diags,
            "manifest.fabric_name",
            "fabric_name",
            "fabric_name must not be empty",
        ),
        Some(name) if !is_valid_fabric_name(name) => push_field(
            diags,
            "manifest.fabric_name",
            "fabric_name",
            format!("fabric_name {name:?} does not match ^[a-z][a-z0-9_-]*$"),
        ),
        _ => {}
    }
}

fn check_crs(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.crs.as_deref() {
        None => push_field(diags, "manifest.crs", "crs", "missing required field: crs"),
        Some("EPSG:4326") => {}
        Some(v) => push_field(
            diags,
            "manifest.crs",
            "crs",
            format!("crs must be \"EPSG:4326\", got {v:?}"),
        ),
    }
}

fn check_topology(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.topology.as_deref() {
        None => push_field(
            diags,
            "manifest.topology",
            "topology",
            "missing required field: topology",
        ),
        Some("tree" | "dag") => {}
        Some(v) => push_field(
            diags,
            "manifest.topology",
            "topology",
            format!("topology must be \"tree\" or \"dag\", got {v:?}"),
        ),
    }
}

fn check_has_up_area(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    if raw.has_up_area.is_none() {
        push_field(
            diags,
            "manifest.has_up_area",
            "has_up_area",
            "missing required field: has_up_area",
        );
    }
}

fn check_bbox(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.bbox.as_deref() {
        None => push_field(
            diags,
            "manifest.bbox",
            "bbox",
            "missing required field: bbox",
        ),
        Some(coords) if coords.len() != 4 => push_field(
            diags,
            "manifest.bbox",
            "bbox",
            format!(
                "bbox must have exactly 4 numbers [minx, miny, maxx, maxy], got {}",
                coords.len()
            ),
        ),
        Some(coords) => {
            let (minx, miny, maxx, maxy) = (coords[0], coords[1], coords[2], coords[3]);
            if minx >= maxx {
                push_field(
                    diags,
                    "manifest.bbox",
                    "bbox",
                    format!("bbox minx ({minx}) must be less than maxx ({maxx})"),
                );
            }
            if miny >= maxy {
                push_field(
                    diags,
                    "manifest.bbox",
                    "bbox",
                    format!("bbox miny ({miny}) must be less than maxy ({maxy})"),
                );
            }
        }
    }
}

fn check_unit_count(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.unit_count {
        None => push_field(
            diags,
            "manifest.unit_count",
            "unit_count",
            "missing required field: unit_count",
        ),
        Some(0) => push_field(
            diags,
            "manifest.unit_count",
            "unit_count",
            "unit_count must be >= 1",
        ),
        _ => {}
    }
}

fn check_created_at(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.created_at.as_deref() {
        None => push_field(
            diags,
            "manifest.created_at",
            "created_at",
            "missing required field: created_at",
        ),
        Some("") => push_field(
            diags,
            "manifest.created_at",
            "created_at",
            "created_at must not be empty",
        ),
        Some(ts) if !is_valid_rfc3339(ts) => push_field(
            diags,
            "manifest.created_at",
            "created_at",
            format!("created_at {ts:?} is not a valid RFC 3339 timestamp"),
        ),
        _ => {}
    }
}

fn check_adapter_version(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.adapter_version.as_deref() {
        None => push_field(
            diags,
            "manifest.adapter_version",
            "adapter_version",
            "missing required field: adapter_version",
        ),
        Some("") => push_field(
            diags,
            "manifest.adapter_version",
            "adapter_version",
            "adapter_version must not be empty",
        ),
        _ => {}
    }
}

fn check_auxiliary(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    let Some(entries) = raw.auxiliary.as_ref() else {
        return;
    };
    for (idx, entry) in entries.iter().enumerate() {
        check_auxiliary_entry(idx, entry, diags);
    }
}

fn check_auxiliary_entry(idx: usize, entry: &RawAuxEntry, diags: &mut Vec<Diagnostic>) {
    match entry.schema.as_deref() {
        None => push_field(
            diags,
            "manifest.auxiliary_schema",
            "auxiliary",
            format!("auxiliary[{idx}] missing required field: schema"),
        ),
        Some(schema) if schema.parse::<AuxiliarySchemaId>().is_err() => push_field(
            diags,
            "manifest.auxiliary_schema",
            "auxiliary",
            format!("auxiliary[{idx}] has malformed schema id {schema:?}"),
        ),
        _ => {}
    }

    match entry.artifacts.as_ref() {
        None => push_field(
            diags,
            "manifest.auxiliary_artifacts",
            "auxiliary",
            format!("auxiliary[{idx}] missing required field: artifacts"),
        ),
        Some(artifacts) if artifacts.is_empty() => push_field(
            diags,
            "manifest.auxiliary_artifacts",
            "auxiliary",
            format!("auxiliary[{idx}] artifacts must not be empty"),
        ),
        Some(artifacts) => check_auxiliary_artifacts(idx, artifacts, diags),
    }

    if !entry
        .metadata
        .as_ref()
        .is_some_and(serde_json::Value::is_object)
    {
        push_field(
            diags,
            "manifest.auxiliary_metadata",
            "auxiliary",
            format!("auxiliary[{idx}] metadata must be an object"),
        );
    }
}

fn check_auxiliary_artifacts(
    idx: usize,
    artifacts: &BTreeMap<String, String>,
    diags: &mut Vec<Diagnostic>,
) {
    for (key, path) in artifacts {
        if key.is_empty() {
            push_field(
                diags,
                "manifest.auxiliary_artifact_key",
                "auxiliary",
                format!("auxiliary[{idx}] artifact key must not be empty"),
            );
        }
        if path.is_empty() {
            push_field(
                diags,
                "manifest.auxiliary_artifact_path",
                "auxiliary",
                format!("auxiliary[{idx}] artifact path for {key:?} must not be empty"),
            );
        }
    }
}

fn push_field(
    diags: &mut Vec<Diagnostic>,
    check_id: &'static str,
    field: &'static str,
    message: impl Into<String>,
) {
    diags.push(
        Diagnostic::error(check_id, Category::Manifest, Artifact::Manifest, message).at(
            Location::Field {
                name: field.to_string(),
            },
        ),
    );
}

fn is_valid_fabric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn is_valid_rfc3339(s: &str) -> bool {
    if s.len() < 20 {
        return false;
    }
    let bytes = s.as_bytes();
    bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes.get(10) == Some(&b'T')
        && bytes.get(13) == Some(&b':')
        && bytes.get(16) == Some(&b':')
        && (s.ends_with('Z') || s.rfind(['+', '-']).is_some_and(|idx| idx > 18))
}
