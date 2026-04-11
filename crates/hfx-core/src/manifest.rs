//! HFX dataset manifest types.

use crate::geo::BoundingBox;
use crate::raster::FlowDirEncoding;

/// Graph topology class declared in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Topology {
    /// Strictly convergent: each atom has at most one downstream neighbor.
    Tree,
    /// Directed acyclic graph with possible bifurcations.
    Dag,
}

/// HFX format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatVersion {
    /// HFX specification version 0.1.
    V0_1,
}

/// Whether upstream area values are precomputed in catchments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpAreaAvailability {
    /// `up_area_km2` column is populated for all atoms.
    Precomputed,
    /// `up_area_km2` is null; engine computes from graph traversal.
    NotAvailable,
}

/// Whether optional raster artifacts are present.
///
/// When rasters are present, the flow direction encoding is guaranteed to
/// be specified — this invariant is encoded in the type system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RasterAvailability {
    /// Both `flow_dir.tif` and `flow_acc.tif` are present.
    Present(FlowDirEncoding),
    /// No rasters. Engine skips raster refinement.
    Absent,
}

/// Whether the optional snap artifact is present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnapAvailability {
    /// `snap.parquet` is present. Engine uses tiered snap ranking.
    Present,
    /// No snap file. Engine uses point-in-polygon on catchments.
    Absent,
}

/// Errors from constructing a [`Manifest`].
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// Returned when atom count is zero.
    #[error("atom count must be at least 1")]
    ZeroAtomCount,

    /// Returned when fabric name is empty.
    #[error("fabric name must not be empty")]
    EmptyFabricName,

    /// Returned when adapter version is empty.
    #[error("adapter version must not be empty")]
    EmptyAdapterVersion,

    /// Returned when created_at timestamp is empty.
    #[error("created_at timestamp must not be empty")]
    EmptyCreatedAt,
}

/// Non-zero count of catchment atoms in a dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AtomCount(u64);

impl AtomCount {
    /// Constructs an `AtomCount` from a raw `u64`, rejecting zero.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`ManifestError::ZeroAtomCount`] | `raw` is 0 |
    pub fn new(raw: u64) -> Result<Self, ManifestError> {
        if raw == 0 {
            return Err(ManifestError::ZeroAtomCount);
        }
        Ok(Self(raw))
    }

    /// Returns the raw atom count value.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Parsed and validated HFX dataset manifest.
///
/// Constructed exclusively via [`ManifestBuilder`]. All required invariants
/// are enforced at build time.
#[derive(Debug, Clone, PartialEq)]
pub struct Manifest {
    format_version: FormatVersion,
    fabric_name: String,
    fabric_version: Option<String>,
    fabric_level: Option<u32>,
    crs: String,
    up_area: UpAreaAvailability,
    rasters: RasterAvailability,
    snap: SnapAvailability,
    topology: Topology,
    region: Option<String>,
    bbox: BoundingBox,
    atom_count: AtomCount,
    created_at: String,
    adapter_version: String,
}

impl Manifest {
    /// Returns the HFX format version declared in this manifest.
    pub fn format_version(&self) -> FormatVersion {
        self.format_version
    }

    /// Returns the source fabric name (e.g. `"HydroBASINS"`).
    pub fn fabric_name(&self) -> &str {
        &self.fabric_name
    }

    /// Returns the optional fabric version string, if declared.
    pub fn fabric_version(&self) -> Option<&str> {
        self.fabric_version.as_deref()
    }

    /// Returns the optional Pfafstetter level of the fabric, if declared.
    pub fn fabric_level(&self) -> Option<u32> {
        self.fabric_level
    }

    /// Returns the coordinate reference system string (always `"EPSG:4326"` in v0.1).
    pub fn crs(&self) -> &str {
        &self.crs
    }

    /// Returns whether upstream area values are precomputed in this dataset.
    pub fn up_area(&self) -> UpAreaAvailability {
        self.up_area
    }

    /// Returns the raster artifact availability for this dataset.
    pub fn rasters(&self) -> RasterAvailability {
        self.rasters
    }

    /// Returns the snap artifact availability for this dataset.
    pub fn snap(&self) -> SnapAvailability {
        self.snap
    }

    /// Returns the declared graph topology of this dataset.
    pub fn topology(&self) -> Topology {
        self.topology
    }

    /// Returns the optional region label for this dataset, if declared.
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// Returns a reference to the dataset's spatial bounding box.
    pub fn bbox(&self) -> &BoundingBox {
        &self.bbox
    }

    /// Returns the non-zero count of catchment atoms in this dataset.
    pub fn atom_count(&self) -> AtomCount {
        self.atom_count
    }

    /// Returns the ISO 8601 creation timestamp string.
    ///
    /// Full timestamp parsing and validation is a validator concern; this
    /// field is stored as-is from the manifest.
    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    /// Returns the adapter version string that compiled this dataset.
    pub fn adapter_version(&self) -> &str {
        &self.adapter_version
    }
}

/// Builder for [`Manifest`].
///
/// Required fields are supplied to [`ManifestBuilder::new`] and validated
/// immediately. Optional fields are set via chainable `with_*` methods.
/// Call [`ManifestBuilder::build`] to produce the final [`Manifest`].
pub struct ManifestBuilder {
    fabric_name: String,
    topology: Topology,
    bbox: BoundingBox,
    atom_count: AtomCount,
    created_at: String,
    adapter_version: String,
    up_area: UpAreaAvailability,
    rasters: RasterAvailability,
    snap: SnapAvailability,
    fabric_version: Option<String>,
    fabric_level: Option<u32>,
    region: Option<String>,
}

impl ManifestBuilder {
    /// Creates a new builder, validating all required fields immediately.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`ManifestError::EmptyFabricName`] | `fabric_name` is empty |
    /// | [`ManifestError::EmptyAdapterVersion`] | `adapter_version` is empty |
    /// | [`ManifestError::EmptyCreatedAt`] | `created_at` is empty |
    pub fn new(
        fabric_name: String,
        topology: Topology,
        bbox: BoundingBox,
        atom_count: AtomCount,
        created_at: String,
        adapter_version: String,
    ) -> Result<Self, ManifestError> {
        if fabric_name.is_empty() {
            return Err(ManifestError::EmptyFabricName);
        }
        if adapter_version.is_empty() {
            return Err(ManifestError::EmptyAdapterVersion);
        }
        if created_at.is_empty() {
            return Err(ManifestError::EmptyCreatedAt);
        }

        Ok(Self {
            fabric_name,
            topology,
            bbox,
            atom_count,
            created_at,
            adapter_version,
            up_area: UpAreaAvailability::NotAvailable,
            rasters: RasterAvailability::Absent,
            snap: SnapAvailability::Absent,
            fabric_version: None,
            fabric_level: None,
            region: None,
        })
    }

    /// Declares that `up_area_km2` is precomputed for all atoms in this dataset.
    pub fn with_up_area(mut self) -> Self {
        self.up_area = UpAreaAvailability::Precomputed;
        self
    }

    /// Declares that `flow_dir.tif` and `flow_acc.tif` are present, using the
    /// given `encoding` convention.
    pub fn with_rasters(mut self, encoding: FlowDirEncoding) -> Self {
        self.rasters = RasterAvailability::Present(encoding);
        self
    }

    /// Declares that `snap.parquet` is present for tiered snap ranking.
    pub fn with_snap(mut self) -> Self {
        self.snap = SnapAvailability::Present;
        self
    }

    /// Sets the optional fabric version string.
    pub fn with_fabric_version(mut self, v: String) -> Self {
        self.fabric_version = Some(v);
        self
    }

    /// Sets the optional Pfafstetter level of the source fabric.
    pub fn with_fabric_level(mut self, level: u32) -> Self {
        self.fabric_level = Some(level);
        self
    }

    /// Sets the optional region label for this dataset.
    pub fn with_region(mut self, region: String) -> Self {
        self.region = Some(region);
        self
    }

    /// Consumes the builder and returns a validated [`Manifest`].
    ///
    /// This method is infallible: all validation occurs in [`ManifestBuilder::new`].
    /// The format version is pinned to [`FormatVersion::V0_1`] and the CRS is
    /// set to `"EPSG:4326"`.
    pub fn build(self) -> Manifest {
        Manifest {
            format_version: FormatVersion::V0_1,
            fabric_name: self.fabric_name,
            fabric_version: self.fabric_version,
            fabric_level: self.fabric_level,
            crs: String::from("EPSG:4326"),
            up_area: self.up_area,
            rasters: self.rasters,
            snap: self.snap,
            topology: self.topology,
            region: self.region,
            bbox: self.bbox,
            atom_count: self.atom_count,
            created_at: self.created_at,
            adapter_version: self.adapter_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geo::BoundingBox;
    use crate::raster::FlowDirEncoding;

    fn test_bbox() -> BoundingBox {
        BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap()
    }

    fn test_atom_count(n: u64) -> AtomCount {
        AtomCount::new(n).unwrap()
    }

    fn minimal_builder() -> ManifestBuilder {
        ManifestBuilder::new(
            String::from("HydroBASINS"),
            Topology::Tree,
            test_bbox(),
            test_atom_count(100),
            String::from("2026-01-01T00:00:00Z"),
            String::from("hfx-adapter-v1"),
        )
        .unwrap()
    }

    // --- AtomCount ---

    #[test]
    fn atom_count_new_one_succeeds() {
        let count = AtomCount::new(1).unwrap();
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn atom_count_new_zero_fails_with_zero_atom_count() {
        let err = AtomCount::new(0).unwrap_err();
        assert!(matches!(err, ManifestError::ZeroAtomCount));
    }

    #[test]
    fn atom_count_new_u64_max_succeeds() {
        let count = AtomCount::new(u64::MAX).unwrap();
        assert_eq!(count.get(), u64::MAX);
    }

    // --- ManifestBuilder validation ---

    #[test]
    fn builder_empty_fabric_name_fails() {
        let err = ManifestBuilder::new(
            String::from(""),
            Topology::Tree,
            test_bbox(),
            test_atom_count(1),
            String::from("2026-01-01T00:00:00Z"),
            String::from("v1"),
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::EmptyFabricName));
    }

    #[test]
    fn builder_empty_adapter_version_fails() {
        let err = ManifestBuilder::new(
            String::from("HydroBASINS"),
            Topology::Tree,
            test_bbox(),
            test_atom_count(1),
            String::from("2026-01-01T00:00:00Z"),
            String::from(""),
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::EmptyAdapterVersion));
    }

    #[test]
    fn builder_empty_created_at_fails() {
        let err = ManifestBuilder::new(
            String::from("HydroBASINS"),
            Topology::Tree,
            test_bbox(),
            test_atom_count(1),
            String::from(""),
            String::from("v1"),
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::EmptyCreatedAt));
    }

    // --- Minimal manifest defaults ---

    #[test]
    fn minimal_manifest_has_expected_defaults() {
        let manifest = minimal_builder().build();

        assert_eq!(manifest.up_area(), UpAreaAvailability::NotAvailable);
        assert_eq!(manifest.rasters(), RasterAvailability::Absent);
        assert_eq!(manifest.snap(), SnapAvailability::Absent);
        assert_eq!(manifest.format_version(), FormatVersion::V0_1);
        assert_eq!(manifest.crs(), "EPSG:4326");
        assert_eq!(manifest.fabric_version(), None);
        assert_eq!(manifest.fabric_level(), None);
        assert_eq!(manifest.region(), None);
    }

    // --- Optional field builders ---

    #[test]
    fn with_up_area_sets_precomputed() {
        let manifest = minimal_builder().with_up_area().build();
        assert_eq!(manifest.up_area(), UpAreaAvailability::Precomputed);
    }

    #[test]
    fn with_rasters_esri_sets_present_esri() {
        let manifest = minimal_builder().with_rasters(FlowDirEncoding::Esri).build();
        assert_eq!(manifest.rasters(), RasterAvailability::Present(FlowDirEncoding::Esri));
    }

    #[test]
    fn with_snap_sets_present() {
        let manifest = minimal_builder().with_snap().build();
        assert_eq!(manifest.snap(), SnapAvailability::Present);
    }

    #[test]
    fn all_optional_fields_set_come_through() {
        let manifest = minimal_builder()
            .with_up_area()
            .with_rasters(FlowDirEncoding::Taudem)
            .with_snap()
            .with_fabric_version(String::from("v2024"))
            .with_fabric_level(8)
            .with_region(String::from("North America"))
            .build();

        assert_eq!(manifest.up_area(), UpAreaAvailability::Precomputed);
        assert_eq!(manifest.rasters(), RasterAvailability::Present(FlowDirEncoding::Taudem));
        assert_eq!(manifest.snap(), SnapAvailability::Present);
        assert_eq!(manifest.fabric_version(), Some("v2024"));
        assert_eq!(manifest.fabric_level(), Some(8));
        assert_eq!(manifest.region(), Some("North America"));
        assert_eq!(manifest.format_version(), FormatVersion::V0_1);
        assert_eq!(manifest.crs(), "EPSG:4326");
    }
}
