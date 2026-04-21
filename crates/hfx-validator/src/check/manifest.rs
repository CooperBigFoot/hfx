//! Manifest field validation: checks every field in `RawManifest` against the spec.

use std::str::FromStr;

use tracing::debug;

use hfx_core::{
    AtomCount, BoundingBox, Crs, FlowDirEncoding, FormatVersion, ManifestBuilder, Topology,
};

use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::manifest::RawManifest;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Check every field in `raw` against the HFX v0.1 specification.
///
/// Returns a flat list of diagnostics — one per violated rule. Fields that are
/// absent produce their own diagnostic, so all issues are visible in a single
/// pass.
pub fn check_manifest(raw: &RawManifest) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    check_format_version(raw, &mut diags);
    check_fabric_name(raw, &mut diags);
    check_crs(raw, &mut diags);
    check_terminal_sink_id(raw, &mut diags);
    check_topology(raw, &mut diags);
    check_has_up_area(raw, &mut diags);
    check_has_rasters(raw, &mut diags);
    check_has_snap(raw, &mut diags);
    check_bbox(raw, &mut diags);
    check_atom_count(raw, &mut diags);
    check_created_at(raw, &mut diags);
    check_adapter_version(raw, &mut diags);
    check_flow_dir_encoding(raw, &mut diags);

    debug!(count = diags.len(), "manifest field checks complete");
    diags
}

/// Attempt to construct an [`hfx_core::Manifest`] from a `RawManifest`.
///
/// This is best-effort: if any required field is absent or invalid, returns
/// `None`. Field-level diagnostics are already produced by [`check_manifest`].
pub fn try_build_manifest(raw: &RawManifest) -> Option<hfx_core::Manifest> {
    let format_version = raw
        .format_version
        .as_deref()?
        .parse::<FormatVersion>()
        .ok()?;
    let fabric_name = raw.fabric_name.as_deref()?;
    let crs = raw.crs.as_deref()?.parse::<Crs>().ok()?;
    let topology = raw.topology.as_deref()?.parse::<Topology>().ok()?;
    let terminal_sink_id = raw.terminal_sink_id?;
    let atom_count = AtomCount::new(raw.atom_count?).ok()?;
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
        terminal_sink_id,
        bbox,
        atom_count,
        created_at,
        adapter_version,
    )
    .ok()?;

    if raw.has_up_area == Some(true) {
        builder = builder.with_up_area();
    }

    if raw.has_rasters == Some(true) {
        let encoding = raw
            .flow_dir_encoding
            .as_deref()
            .and_then(|s| FlowDirEncoding::from_str(s).ok())?;
        builder = builder.with_rasters(encoding);
    }

    if raw.has_snap == Some(true) {
        builder = builder.with_snap();
    }

    if let Some(fv) = raw.fabric_version.as_deref() {
        builder = builder.with_fabric_version(fv);
    }
    if let Some(fl) = raw.fabric_level {
        builder = builder.with_fabric_level(fl);
    }
    if let Some(region) = raw.region.as_deref() {
        builder = builder.with_region(region);
    }

    Some(builder.build())
}

// ---------------------------------------------------------------------------
// Per-field checks
// ---------------------------------------------------------------------------

fn check_format_version(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.format_version.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.format_version",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: format_version",
            )
            .at(Location::Field {
                name: "format_version".into(),
            }),
        ),
        Some(v) if v != "0.1" => diags.push(
            Diagnostic::error(
                "manifest.format_version",
                Category::Manifest,
                Artifact::Manifest,
                format!("format_version must be \"0.1\", got {v:?}"),
            )
            .at(Location::Field {
                name: "format_version".into(),
            }),
        ),
        _ => {}
    }
}

/// Fabric name must be present, non-empty, and match `^[a-z][a-z0-9_-]*$`.
fn check_fabric_name(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.fabric_name.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.fabric_name",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: fabric_name",
            )
            .at(Location::Field {
                name: "fabric_name".into(),
            }),
        ),
        Some("") => diags.push(
            Diagnostic::error(
                "manifest.fabric_name",
                Category::Manifest,
                Artifact::Manifest,
                "fabric_name must not be empty",
            )
            .at(Location::Field {
                name: "fabric_name".into(),
            }),
        ),
        Some(name) if !is_valid_fabric_name(name) => diags.push(
            Diagnostic::error(
                "manifest.fabric_name",
                Category::Manifest,
                Artifact::Manifest,
                format!("fabric_name {name:?} does not match ^[a-z][a-z0-9_-]*$"),
            )
            .at(Location::Field {
                name: "fabric_name".into(),
            }),
        ),
        _ => {}
    }
}

fn check_crs(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.crs.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.crs",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: crs",
            )
            .at(Location::Field { name: "crs".into() }),
        ),
        Some(v) if v != "EPSG:4326" => diags.push(
            Diagnostic::error(
                "manifest.crs",
                Category::Manifest,
                Artifact::Manifest,
                format!("crs must be \"EPSG:4326\", got {v:?}"),
            )
            .at(Location::Field { name: "crs".into() }),
        ),
        _ => {}
    }
}

fn check_terminal_sink_id(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.terminal_sink_id {
        None => diags.push(
            Diagnostic::error(
                "manifest.terminal_sink_id",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: terminal_sink_id",
            )
            .at(Location::Field {
                name: "terminal_sink_id".into(),
            }),
        ),
        Some(v) if v != 0 => diags.push(
            Diagnostic::error(
                "manifest.terminal_sink_id",
                Category::Manifest,
                Artifact::Manifest,
                format!("terminal_sink_id must be 0, got {v}"),
            )
            .at(Location::Field {
                name: "terminal_sink_id".into(),
            }),
        ),
        _ => {}
    }
}

fn check_topology(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.topology.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.topology",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: topology",
            )
            .at(Location::Field {
                name: "topology".into(),
            }),
        ),
        Some(v) if v != "tree" && v != "dag" => diags.push(
            Diagnostic::error(
                "manifest.topology",
                Category::Manifest,
                Artifact::Manifest,
                format!("topology must be \"tree\" or \"dag\", got {v:?}"),
            )
            .at(Location::Field {
                name: "topology".into(),
            }),
        ),
        _ => {}
    }
}

fn check_has_up_area(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    if raw.has_up_area.is_none() {
        diags.push(
            Diagnostic::error(
                "manifest.has_up_area",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: has_up_area",
            )
            .at(Location::Field {
                name: "has_up_area".into(),
            }),
        );
    }
}

fn check_has_rasters(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    if raw.has_rasters.is_none() {
        diags.push(
            Diagnostic::error(
                "manifest.has_rasters",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: has_rasters",
            )
            .at(Location::Field {
                name: "has_rasters".into(),
            }),
        );
    }
}

fn check_has_snap(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    if raw.has_snap.is_none() {
        diags.push(
            Diagnostic::error(
                "manifest.has_snap",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: has_snap",
            )
            .at(Location::Field {
                name: "has_snap".into(),
            }),
        );
    }
}

fn check_bbox(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.bbox.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.bbox",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: bbox",
            )
            .at(Location::Field {
                name: "bbox".into(),
            }),
        ),
        Some(coords) if coords.len() != 4 => diags.push(
            Diagnostic::error(
                "manifest.bbox",
                Category::Manifest,
                Artifact::Manifest,
                format!(
                    "bbox must have exactly 4 numbers [minx, miny, maxx, maxy], got {}",
                    coords.len()
                ),
            )
            .at(Location::Field {
                name: "bbox".into(),
            }),
        ),
        Some(coords) => {
            let (minx, miny, maxx, maxy) = (coords[0], coords[1], coords[2], coords[3]);
            if minx >= maxx {
                diags.push(
                    Diagnostic::error(
                        "manifest.bbox",
                        Category::Manifest,
                        Artifact::Manifest,
                        format!("bbox minx ({minx}) must be less than maxx ({maxx})"),
                    )
                    .at(Location::Field {
                        name: "bbox".into(),
                    }),
                );
            }
            if miny >= maxy {
                diags.push(
                    Diagnostic::error(
                        "manifest.bbox",
                        Category::Manifest,
                        Artifact::Manifest,
                        format!("bbox miny ({miny}) must be less than maxy ({maxy})"),
                    )
                    .at(Location::Field {
                        name: "bbox".into(),
                    }),
                );
            }
        }
    }
}

fn check_atom_count(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.atom_count {
        None => diags.push(
            Diagnostic::error(
                "manifest.atom_count",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: atom_count",
            )
            .at(Location::Field {
                name: "atom_count".into(),
            }),
        ),
        Some(0) => diags.push(
            Diagnostic::error(
                "manifest.atom_count",
                Category::Manifest,
                Artifact::Manifest,
                "atom_count must be >= 1",
            )
            .at(Location::Field {
                name: "atom_count".into(),
            }),
        ),
        _ => {}
    }
}

fn check_created_at(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.created_at.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.created_at",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: created_at",
            )
            .at(Location::Field {
                name: "created_at".into(),
            }),
        ),
        Some("") => diags.push(
            Diagnostic::error(
                "manifest.created_at",
                Category::Manifest,
                Artifact::Manifest,
                "created_at must not be empty",
            )
            .at(Location::Field {
                name: "created_at".into(),
            }),
        ),
        Some(ts) if !is_valid_rfc3339(ts) => diags.push(
            Diagnostic::error(
                "manifest.created_at",
                Category::Manifest,
                Artifact::Manifest,
                format!("created_at {ts:?} is not a valid RFC 3339 timestamp"),
            )
            .at(Location::Field {
                name: "created_at".into(),
            }),
        ),
        _ => {}
    }
}

fn check_adapter_version(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    match raw.adapter_version.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.adapter_version",
                Category::Manifest,
                Artifact::Manifest,
                "missing required field: adapter_version",
            )
            .at(Location::Field {
                name: "adapter_version".into(),
            }),
        ),
        Some("") => diags.push(
            Diagnostic::error(
                "manifest.adapter_version",
                Category::Manifest,
                Artifact::Manifest,
                "adapter_version must not be empty",
            )
            .at(Location::Field {
                name: "adapter_version".into(),
            }),
        ),
        _ => {}
    }
}

/// When `has_rasters == true`, `flow_dir_encoding` must be present and valid.
fn check_flow_dir_encoding(raw: &RawManifest, diags: &mut Vec<Diagnostic>) {
    if raw.has_rasters != Some(true) {
        return;
    }
    match raw.flow_dir_encoding.as_deref() {
        None => diags.push(
            Diagnostic::error(
                "manifest.flow_dir_encoding",
                Category::Manifest,
                Artifact::Manifest,
                "flow_dir_encoding is required when has_rasters is true",
            )
            .at(Location::Field {
                name: "flow_dir_encoding".into(),
            }),
        ),
        Some(enc) if enc != "esri" && enc != "taudem" => diags.push(
            Diagnostic::error(
                "manifest.flow_dir_encoding",
                Category::Manifest,
                Artifact::Manifest,
                format!("flow_dir_encoding must be \"esri\" or \"taudem\", got {enc:?}"),
            )
            .at(Location::Field {
                name: "flow_dir_encoding".into(),
            }),
        ),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if `name` matches `^[a-z][a-z0-9_-]*$`.
fn is_valid_fabric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        // Must start with a lowercase ASCII letter.
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    // Remaining characters: lowercase letters, digits, underscore, hyphen.
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// Lightweight RFC 3339 format check (no external deps).
///
/// Accepts strings of the form `YYYY-MM-DDThh:mm:ss[.frac][offset]` where
/// offset is `Z`, `+HH:MM`, or `-HH:MM`. The optional fractional seconds
/// part (`.` followed by one or more digits) is skipped. Date/time components
/// are checked for plausible numeric ranges; full calendar validity (leap
/// years, etc.) is not enforced.
fn is_valid_rfc3339(s: &str) -> bool {
    // Minimum: "2000-01-01T00:00:00Z" (20 chars)
    if s.len() < 20 {
        return false;
    }

    let bytes = s.as_bytes();

    // Positions: YYYY-MM-DDThh:mm:ss
    //            0123456789012345678
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return false;
    }

    // Parse numeric segments.
    let year = parse_digits(&bytes[0..4]);
    let month = parse_digits(&bytes[5..7]);
    let day = parse_digits(&bytes[8..10]);
    let hour = parse_digits(&bytes[11..13]);
    let min = parse_digits(&bytes[14..16]);
    let sec = parse_digits(&bytes[17..19]);

    let (Some(month), Some(day), Some(hour), Some(min), Some(sec)) = (month, day, hour, min, sec)
    else {
        return false;
    };
    if year.is_none() {
        return false;
    }

    if !(1..=12).contains(&month) {
        return false;
    }
    if !(1..=31).contains(&day) {
        return false;
    }
    if hour > 23 {
        return false;
    }
    if min > 59 {
        return false;
    }
    // Allow leap seconds (60).
    if sec > 60 {
        return false;
    }

    // After the seconds field (byte 19), there may be an optional fractional
    // part: `.` followed by one or more ASCII digits.
    let mut offset_start = 19usize;
    if bytes.get(offset_start) == Some(&b'.') {
        offset_start += 1; // skip the dot
        // skip all following digit bytes
        while offset_start < bytes.len() && bytes[offset_start].is_ascii_digit() {
            offset_start += 1;
        }
        // Must have consumed at least one digit after the dot.
        if offset_start == 20 {
            return false;
        }
    }

    // Timezone: Z | +HH:MM | -HH:MM
    let offset_part = &s[offset_start..];
    is_valid_rfc3339_offset(offset_part)
}

fn is_valid_rfc3339_offset(s: &str) -> bool {
    if s == "Z" || s == "z" {
        return true;
    }
    // +HH:MM or -HH:MM  (5 chars)
    if s.len() != 6 {
        return false;
    }
    let b = s.as_bytes();
    if b[0] != b'+' && b[0] != b'-' {
        return false;
    }
    if b[3] != b':' {
        return false;
    }
    let hh = parse_digits(&b[1..3]);
    let mm = parse_digits(&b[4..6]);
    match (hh, mm) {
        (Some(h), Some(m)) => h <= 23 && m <= 59,
        _ => false,
    }
}

/// Parse a slice of ASCII digit bytes into a `u32`. Returns `None` on any
/// non-digit byte.
fn parse_digits(b: &[u8]) -> Option<u32> {
    let mut acc: u32 = 0;
    for &byte in b {
        if !byte.is_ascii_digit() {
            return None;
        }
        acc = acc * 10 + u32::from(byte - b'0');
    }
    Some(acc)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Build a fully valid RawManifest for use as a baseline.
    fn valid_raw() -> RawManifest {
        RawManifest {
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            has_rasters: Some(false),
            has_snap: Some(false),
            flow_dir_encoding: None,
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(100),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        }
    }

    fn errors_with_id(diags: &[Diagnostic], id: &str) -> usize {
        diags.iter().filter(|d| d.check_id == id).count()
    }

    // --- valid baseline ---

    #[test]
    fn valid_manifest_produces_no_errors() {
        let diags = check_manifest(&valid_raw());
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    // --- format_version ---

    #[test]
    fn missing_format_version_is_error() {
        let mut raw = valid_raw();
        raw.format_version = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.format_version"), 1);
    }

    #[test]
    fn wrong_format_version_is_error() {
        let mut raw = valid_raw();
        raw.format_version = Some("0.2".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.format_version"), 1);
    }

    // --- fabric_name ---

    #[test]
    fn missing_fabric_name_is_error() {
        let mut raw = valid_raw();
        raw.fabric_name = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.fabric_name"), 1);
    }

    #[test]
    fn empty_fabric_name_is_error() {
        let mut raw = valid_raw();
        raw.fabric_name = Some("".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.fabric_name"), 1);
    }

    #[test]
    fn uppercase_fabric_name_is_error() {
        let mut raw = valid_raw();
        raw.fabric_name = Some("HydroBASINS".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.fabric_name"), 1);
    }

    #[test]
    fn fabric_name_with_space_is_error() {
        let mut raw = valid_raw();
        raw.fabric_name = Some("hydro basins".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.fabric_name"), 1);
    }

    #[test]
    fn fabric_name_starting_with_digit_is_error() {
        let mut raw = valid_raw();
        raw.fabric_name = Some("123abc".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.fabric_name"), 1);
    }

    #[test]
    fn fabric_name_with_hyphens_and_underscores_is_valid() {
        let mut raw = valid_raw();
        raw.fabric_name = Some("hydro-basins_v2".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.fabric_name"), 0);
    }

    // --- crs ---

    #[test]
    fn missing_crs_is_error() {
        let mut raw = valid_raw();
        raw.crs = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.crs"), 1);
    }

    #[test]
    fn wrong_crs_is_error() {
        let mut raw = valid_raw();
        raw.crs = Some("EPSG:32632".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.crs"), 1);
    }

    // --- terminal_sink_id ---

    #[test]
    fn missing_terminal_sink_id_is_error() {
        let mut raw = valid_raw();
        raw.terminal_sink_id = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.terminal_sink_id"), 1);
    }

    #[test]
    fn nonzero_terminal_sink_id_is_error() {
        let mut raw = valid_raw();
        raw.terminal_sink_id = Some(1);
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.terminal_sink_id"), 1);
    }

    // --- topology ---

    #[test]
    fn missing_topology_is_error() {
        let mut raw = valid_raw();
        raw.topology = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.topology"), 1);
    }

    #[test]
    fn invalid_topology_is_error() {
        let mut raw = valid_raw();
        raw.topology = Some("cyclic".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.topology"), 1);
    }

    #[test]
    fn topology_dag_is_valid() {
        let mut raw = valid_raw();
        raw.topology = Some("dag".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.topology"), 0);
    }

    // --- has_up_area / has_rasters / has_snap ---

    #[test]
    fn missing_has_up_area_is_error() {
        let mut raw = valid_raw();
        raw.has_up_area = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.has_up_area"), 1);
    }

    #[test]
    fn missing_has_rasters_is_error() {
        let mut raw = valid_raw();
        raw.has_rasters = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.has_rasters"), 1);
    }

    #[test]
    fn missing_has_snap_is_error() {
        let mut raw = valid_raw();
        raw.has_snap = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.has_snap"), 1);
    }

    // --- bbox ---

    #[test]
    fn missing_bbox_is_error() {
        let mut raw = valid_raw();
        raw.bbox = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.bbox"), 1);
    }

    #[test]
    fn bbox_wrong_length_is_error() {
        let mut raw = valid_raw();
        raw.bbox = Some(vec![0.0, 1.0, 2.0]);
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.bbox"), 1);
    }

    #[test]
    fn bbox_inverted_x_is_error() {
        let mut raw = valid_raw();
        raw.bbox = Some(vec![10.0, -5.0, 5.0, 5.0]);
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.bbox"), 1);
    }

    #[test]
    fn bbox_inverted_y_is_error() {
        let mut raw = valid_raw();
        raw.bbox = Some(vec![-10.0, 5.0, 10.0, -5.0]);
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.bbox"), 1);
    }

    // --- atom_count ---

    #[test]
    fn missing_atom_count_is_error() {
        let mut raw = valid_raw();
        raw.atom_count = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.atom_count"), 1);
    }

    #[test]
    fn zero_atom_count_is_error() {
        let mut raw = valid_raw();
        raw.atom_count = Some(0);
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.atom_count"), 1);
    }

    // --- created_at RFC 3339 ---

    #[test]
    fn missing_created_at_is_error() {
        let mut raw = valid_raw();
        raw.created_at = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.created_at"), 1);
    }

    #[test]
    fn valid_rfc3339_utc_passes() {
        assert!(is_valid_rfc3339("2026-01-01T00:00:00Z"));
    }

    #[test]
    fn valid_rfc3339_positive_offset_passes() {
        assert!(is_valid_rfc3339("2026-04-11T12:30:00+05:30"));
    }

    #[test]
    fn valid_rfc3339_negative_offset_passes() {
        assert!(is_valid_rfc3339("2026-04-11T12:30:00-08:00"));
    }

    #[test]
    fn invalid_rfc3339_no_t_separator_fails() {
        assert!(!is_valid_rfc3339("2026-01-01 00:00:00Z"));
    }

    #[test]
    fn invalid_rfc3339_date_only_fails() {
        assert!(!is_valid_rfc3339("2026-01-01"));
    }

    #[test]
    fn invalid_rfc3339_bad_month_fails() {
        assert!(!is_valid_rfc3339("2026-13-01T00:00:00Z"));
    }

    #[test]
    fn invalid_rfc3339_bad_hour_fails() {
        assert!(!is_valid_rfc3339("2026-01-01T25:00:00Z"));
    }

    #[test]
    fn invalid_rfc3339_non_digits_fail() {
        assert!(!is_valid_rfc3339("YYYY-MM-DDThh:mm:ssZ"));
    }

    #[test]
    fn valid_rfc3339_fractional_seconds_three_digits_passes() {
        assert!(is_valid_rfc3339("2026-01-01T00:00:00.123Z"));
    }

    #[test]
    fn valid_rfc3339_fractional_seconds_one_digit_passes() {
        assert!(is_valid_rfc3339("2026-01-01T00:00:00.0Z"));
    }

    #[test]
    fn valid_rfc3339_fractional_seconds_with_offset_passes() {
        assert!(is_valid_rfc3339("2026-01-01T12:30:00.456+05:30"));
    }

    #[test]
    fn invalid_rfc3339_produces_diagnostic() {
        let mut raw = valid_raw();
        raw.created_at = Some("not-a-timestamp".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.created_at"), 1);
    }

    // --- adapter_version ---

    #[test]
    fn missing_adapter_version_is_error() {
        let mut raw = valid_raw();
        raw.adapter_version = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.adapter_version"), 1);
    }

    #[test]
    fn empty_adapter_version_is_error() {
        let mut raw = valid_raw();
        raw.adapter_version = Some("".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.adapter_version"), 1);
    }

    // --- flow_dir_encoding conditional ---

    #[test]
    fn has_rasters_true_without_encoding_is_error() {
        let mut raw = valid_raw();
        raw.has_rasters = Some(true);
        raw.flow_dir_encoding = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.flow_dir_encoding"), 1);
    }

    #[test]
    fn has_rasters_true_with_invalid_encoding_is_error() {
        let mut raw = valid_raw();
        raw.has_rasters = Some(true);
        raw.flow_dir_encoding = Some("d8".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.flow_dir_encoding"), 1);
    }

    #[test]
    fn has_rasters_true_with_esri_encoding_is_valid() {
        let mut raw = valid_raw();
        raw.has_rasters = Some(true);
        raw.flow_dir_encoding = Some("esri".into());
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.flow_dir_encoding"), 0);
    }

    #[test]
    fn has_rasters_false_flow_dir_encoding_not_required() {
        let mut raw = valid_raw();
        raw.has_rasters = Some(false);
        raw.flow_dir_encoding = None;
        let diags = check_manifest(&raw);
        assert_eq!(errors_with_id(&diags, "manifest.flow_dir_encoding"), 0);
    }

    // --- try_build_manifest ---

    #[test]
    fn try_build_from_valid_raw_succeeds() {
        let raw = valid_raw();
        assert!(try_build_manifest(&raw).is_some());
    }

    #[test]
    fn try_build_from_invalid_raw_returns_none() {
        let mut raw = valid_raw();
        raw.format_version = None;
        assert!(try_build_manifest(&raw).is_none());
    }
}
