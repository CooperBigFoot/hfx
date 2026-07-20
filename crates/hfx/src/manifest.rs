//! HFX dataset manifest types.

use std::str::FromStr;

use crate::auxiliary::AuxiliaryDecl;
use crate::geo::BoundingBox;

/// Graph topology class declared in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Topology {
    /// Strictly convergent: each unit has at most one downstream neighbor.
    Tree,
    /// Directed acyclic graph with possible bifurcations.
    Dag,
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Topology::Tree => write!(f, "tree"),
            Topology::Dag => write!(f, "dag"),
        }
    }
}

impl FromStr for Topology {
    type Err = ManifestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tree" => Ok(Topology::Tree),
            "dag" => Ok(Topology::Dag),
            _ => Err(ManifestError::UnsupportedTopology {
                value: s.to_owned(),
            }),
        }
    }
}

/// HFX format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatVersion {
    /// HFX specification version 0.1.
    V0_1,
    /// HFX specification version 0.2.1.
    V0_2_1,
    /// HFX specification version 0.3.0.
    V0_3_0,
}

impl std::fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatVersion::V0_1 => write!(f, "0.1"),
            FormatVersion::V0_2_1 => write!(f, "0.2.1"),
            FormatVersion::V0_3_0 => write!(f, "0.3.0"),
        }
    }
}

impl FromStr for FormatVersion {
    type Err = ManifestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0.1" => Ok(FormatVersion::V0_1),
            "0.2.1" => Ok(FormatVersion::V0_2_1),
            "0.3.0" => Ok(FormatVersion::V0_3_0),
            _ => Err(ManifestError::UnsupportedFormatVersion {
                value: s.to_owned(),
            }),
        }
    }
}

/// Coordinate reference system for all HFX vector data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Crs {
    /// WGS84 geographic coordinates. The only CRS supported in HFX v0.2.1.
    Epsg4326,
}

impl std::fmt::Display for Crs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Crs::Epsg4326 => write!(f, "EPSG:4326"),
        }
    }
}

impl FromStr for Crs {
    type Err = ManifestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EPSG:4326" => Ok(Crs::Epsg4326),
            _ => Err(ManifestError::UnsupportedCrs {
                value: s.to_owned(),
            }),
        }
    }
}

/// Whether upstream area values are precomputed in catchments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpAreaAvailability {
    /// `up_area_km2` column is populated for applicable units.
    Precomputed,
    /// `up_area_km2` is null; engine computes from graph traversal.
    NotAvailable,
}

/// Errors from constructing a [`Manifest`].
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// Returned when unit count is zero.
    #[error("unit count must be at least 1")]
    ZeroUnitCount,

    /// Returned when fabric name is empty.
    #[error("fabric name must not be empty")]
    EmptyFabricName,

    /// Returned when adapter version is empty.
    #[error("adapter version must not be empty")]
    EmptyAdapterVersion,

    /// Returned when created_at timestamp is empty.
    #[error("created_at timestamp must not be empty")]
    EmptyCreatedAt,

    /// Returned when fabric_name contains uppercase characters.
    #[error("fabric name must be lowercase, got {value:?}")]
    NonLowercaseFabricName {
        /// The invalid fabric name.
        value: String,
    },

    /// Returned when an unsupported CRS string is provided.
    #[error("unsupported CRS: {value:?}, expected \"EPSG:4326\"")]
    UnsupportedCrs {
        /// The unrecognized CRS string.
        value: String,
    },

    /// Returned when an unsupported format version is provided.
    #[error("unsupported format version: {value:?}, expected \"0.3.0\"")]
    UnsupportedFormatVersion {
        /// The unrecognized version string.
        value: String,
    },

    /// Returned when an unsupported topology string is provided.
    #[error("unsupported topology: {value:?}, expected \"tree\" or \"dag\"")]
    UnsupportedTopology {
        /// The unrecognized topology string.
        value: String,
    },
}

/// Non-zero count of drainage units in a dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnitCount(u64);

impl UnitCount {
    /// Constructs a `UnitCount` from a raw `u64`, rejecting zero.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`ManifestError::ZeroUnitCount`] | `raw` is 0 |
    pub fn new(raw: u64) -> Result<Self, ManifestError> {
        if raw == 0 {
            return Err(ManifestError::ZeroUnitCount);
        }
        Ok(Self(raw))
    }

    /// Returns the raw unit count value.
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
    crs: Crs,
    up_area: UpAreaAvailability,
    topology: Topology,
    region: Option<String>,
    bbox: BoundingBox,
    unit_count: UnitCount,
    created_at: String,
    adapter_version: String,
    auxiliary: Vec<AuxiliaryDecl>,
}

impl Manifest {
    /// Returns the HFX format version declared in this manifest.
    pub fn format_version(&self) -> FormatVersion {
        self.format_version
    }

    /// Returns the source fabric name (e.g. `"example-fabric"`).
    pub fn fabric_name(&self) -> &str {
        &self.fabric_name
    }

    /// Returns the optional fabric version string, if declared.
    pub fn fabric_version(&self) -> Option<&str> {
        self.fabric_version.as_deref()
    }

    /// Returns the coordinate reference system for this dataset.
    pub fn crs(&self) -> Crs {
        self.crs
    }

    /// Returns whether upstream area values are precomputed in this dataset.
    pub fn up_area(&self) -> UpAreaAvailability {
        self.up_area
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

    /// Returns the non-zero count of drainage units in this dataset.
    pub fn unit_count(&self) -> UnitCount {
        self.unit_count
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

    /// Returns manifest-declared auxiliary artifact blocks.
    pub fn auxiliary(&self) -> &[AuxiliaryDecl] {
        &self.auxiliary
    }
}

/// Builder for [`Manifest`].
///
/// Required fields are supplied to [`ManifestBuilder::new`] and validated
/// immediately. Optional fields are set via chainable `with_*` methods.
/// Call [`ManifestBuilder::build`] to produce the final [`Manifest`].
#[derive(Debug)]
pub struct ManifestBuilder {
    format_version: FormatVersion,
    fabric_name: String,
    crs: Crs,
    topology: Topology,
    bbox: BoundingBox,
    unit_count: UnitCount,
    created_at: String,
    adapter_version: String,
    up_area: UpAreaAvailability,
    fabric_version: Option<String>,
    region: Option<String>,
    auxiliary: Vec<AuxiliaryDecl>,
}

impl ManifestBuilder {
    /// Creates a new builder, validating all required fields immediately.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`ManifestError::EmptyFabricName`] | `fabric_name` is empty |
    /// | [`ManifestError::NonLowercaseFabricName`] | `fabric_name` contains uppercase characters |
    /// | [`ManifestError::EmptyAdapterVersion`] | `adapter_version` is empty |
    /// | [`ManifestError::EmptyCreatedAt`] | `created_at` is empty |
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        format_version: FormatVersion,
        fabric_name: impl Into<String>,
        crs: Crs,
        topology: Topology,
        bbox: BoundingBox,
        unit_count: UnitCount,
        created_at: impl Into<String>,
        adapter_version: impl Into<String>,
    ) -> Result<Self, ManifestError> {
        let fabric_name = fabric_name.into();
        let created_at = created_at.into();
        let adapter_version = adapter_version.into();

        if fabric_name.is_empty() {
            return Err(ManifestError::EmptyFabricName);
        }
        if fabric_name.chars().any(|c| c.is_uppercase()) {
            return Err(ManifestError::NonLowercaseFabricName { value: fabric_name });
        }
        if adapter_version.is_empty() {
            return Err(ManifestError::EmptyAdapterVersion);
        }
        if created_at.is_empty() {
            return Err(ManifestError::EmptyCreatedAt);
        }

        Ok(Self {
            format_version,
            fabric_name,
            crs,
            topology,
            bbox,
            unit_count,
            created_at,
            adapter_version,
            up_area: UpAreaAvailability::NotAvailable,
            fabric_version: None,
            region: None,
            auxiliary: Vec::new(),
        })
    }

    /// Declares that `up_area_km2` is precomputed for applicable units.
    pub fn with_up_area(mut self) -> Self {
        self.up_area = UpAreaAvailability::Precomputed;
        self
    }

    /// Sets the optional fabric version string.
    pub fn with_fabric_version(mut self, v: impl Into<String>) -> Self {
        self.fabric_version = Some(v.into());
        self
    }

    /// Sets the optional region label for this dataset.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Appends a manifest-declared auxiliary artifact block.
    pub fn with_auxiliary(mut self, auxiliary: AuxiliaryDecl) -> Self {
        self.auxiliary.push(auxiliary);
        self
    }

    /// Consumes the builder and returns a validated [`Manifest`].
    ///
    /// This method is infallible: all validation occurs in [`ManifestBuilder::new`].
    pub fn build(self) -> Manifest {
        Manifest {
            format_version: self.format_version,
            fabric_name: self.fabric_name,
            fabric_version: self.fabric_version,
            crs: self.crs,
            up_area: self.up_area,
            topology: self.topology,
            region: self.region,
            bbox: self.bbox,
            unit_count: self.unit_count,
            created_at: self.created_at,
            adapter_version: self.adapter_version,
            auxiliary: self.auxiliary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auxiliary::{AuxiliarySchemaId, BlessedAuxSchema};
    use crate::geo::BoundingBox;
    use std::collections::BTreeMap;

    fn test_bbox() -> BoundingBox {
        BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap()
    }

    fn test_unit_count(n: u64) -> UnitCount {
        UnitCount::new(n).unwrap()
    }

    fn minimal_builder() -> ManifestBuilder {
        ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "testfabric",
            Crs::Epsg4326,
            Topology::Tree,
            test_bbox(),
            test_unit_count(100),
            "2026-01-01T00:00:00Z",
            "hfx-adapter-v1",
        )
        .unwrap()
    }

    // --- UnitCount ---

    #[test]
    fn unit_count_new_one_succeeds() {
        let count = UnitCount::new(1).unwrap();
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn unit_count_new_zero_fails_with_zero_unit_count() {
        let err = UnitCount::new(0).unwrap_err();
        assert!(matches!(err, ManifestError::ZeroUnitCount));
    }

    #[test]
    fn unit_count_new_u64_max_succeeds() {
        let count = UnitCount::new(u64::MAX).unwrap();
        assert_eq!(count.get(), u64::MAX);
    }

    // --- ManifestBuilder validation ---

    #[test]
    fn builder_empty_fabric_name_fails() {
        let err = ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "",
            Crs::Epsg4326,
            Topology::Tree,
            test_bbox(),
            test_unit_count(1),
            "2026-01-01T00:00:00Z",
            "v1",
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::EmptyFabricName));
    }

    #[test]
    fn builder_empty_adapter_version_fails() {
        let err = ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "testfabric",
            Crs::Epsg4326,
            Topology::Tree,
            test_bbox(),
            test_unit_count(1),
            "2026-01-01T00:00:00Z",
            "",
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::EmptyAdapterVersion));
    }

    #[test]
    fn builder_empty_created_at_fails() {
        let err = ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "testfabric",
            Crs::Epsg4326,
            Topology::Tree,
            test_bbox(),
            test_unit_count(1),
            "",
            "v1",
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::EmptyCreatedAt));
    }

    #[test]
    fn fabric_name_uppercase_fails() {
        let err = ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "HydroBASINS",
            Crs::Epsg4326,
            Topology::Tree,
            test_bbox(),
            test_unit_count(1),
            "2026-01-01T00:00:00Z",
            "v1",
        )
        .err()
        .unwrap();
        assert!(matches!(err, ManifestError::NonLowercaseFabricName { .. }));
    }

    #[test]
    fn fabric_name_lowercase_succeeds() {
        let result = ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "testfabric",
            Crs::Epsg4326,
            Topology::Tree,
            test_bbox(),
            test_unit_count(1),
            "2026-01-01T00:00:00Z",
            "v1",
        );
        assert!(result.is_ok());
    }

    // --- Minimal manifest defaults ---

    #[test]
    fn minimal_manifest_has_expected_defaults() {
        let manifest = minimal_builder().build();

        assert_eq!(manifest.up_area(), UpAreaAvailability::NotAvailable);
        assert_eq!(manifest.format_version(), FormatVersion::V0_2_1);
        assert_eq!(manifest.crs(), Crs::Epsg4326);
        assert_eq!(manifest.fabric_version(), None);
        assert_eq!(manifest.region(), None);
        assert_eq!(manifest.auxiliary(), &[]);
    }

    #[test]
    fn crs_getter_returns_enum() {
        let manifest = minimal_builder().build();
        assert_eq!(manifest.crs(), Crs::Epsg4326);
    }

    #[test]
    fn unit_count_getter_returns_unit_count() {
        let manifest = minimal_builder().build();
        assert_eq!(manifest.unit_count(), test_unit_count(100));
    }

    // --- Optional field builders ---

    #[test]
    fn with_up_area_sets_precomputed() {
        let manifest = minimal_builder().with_up_area().build();
        assert_eq!(manifest.up_area(), UpAreaAvailability::Precomputed);
    }

    #[test]
    fn all_optional_fields_set_come_through() {
        let mut artifacts = BTreeMap::new();
        artifacts.insert("flow_dir".to_string(), "flow_dir.tif".to_string());
        artifacts.insert("flow_acc".to_string(), "flow_acc.tif".to_string());
        let auxiliary = AuxiliaryDecl::new(
            AuxiliarySchemaId::Blessed(BlessedAuxSchema::D8RasterV2),
            artifacts,
        )
        .unwrap();

        let manifest = minimal_builder()
            .with_up_area()
            .with_fabric_version("v2024")
            .with_region("North America")
            .with_auxiliary(auxiliary.clone())
            .build();

        assert_eq!(manifest.up_area(), UpAreaAvailability::Precomputed);
        assert_eq!(manifest.fabric_version(), Some("v2024"));
        assert_eq!(manifest.region(), Some("North America"));
        assert_eq!(manifest.auxiliary(), &[auxiliary]);
        assert_eq!(manifest.format_version(), FormatVersion::V0_2_1);
        assert_eq!(manifest.crs(), Crs::Epsg4326);
    }

    // --- Display / FromStr roundtrips ---

    #[test]
    fn topology_display_roundtrip() {
        assert_eq!(Topology::Tree.to_string(), "tree");
        assert_eq!(Topology::Dag.to_string(), "dag");
        assert_eq!("tree".parse::<Topology>().unwrap(), Topology::Tree);
        assert_eq!("dag".parse::<Topology>().unwrap(), Topology::Dag);
    }

    #[test]
    fn format_version_display_roundtrip() {
        assert_eq!(FormatVersion::V0_1.to_string(), "0.1");
        assert_eq!("0.1".parse::<FormatVersion>().unwrap(), FormatVersion::V0_1);
        assert_eq!(FormatVersion::V0_2_1.to_string(), "0.2.1");
        assert_eq!(
            "0.2.1".parse::<FormatVersion>().unwrap(),
            FormatVersion::V0_2_1
        );
        assert_eq!(FormatVersion::V0_3_0.to_string(), "0.3.0");
        assert_eq!(
            "0.3.0".parse::<FormatVersion>().unwrap(),
            FormatVersion::V0_3_0
        );
    }

    #[test]
    fn crs_display_roundtrip() {
        assert_eq!(Crs::Epsg4326.to_string(), "EPSG:4326");
        assert_eq!("EPSG:4326".parse::<Crs>().unwrap(), Crs::Epsg4326);
    }

    // --- FromStr error cases ---

    #[test]
    fn topology_fromstr_invalid() {
        let err = "invalid".parse::<Topology>().unwrap_err();
        assert!(matches!(err, ManifestError::UnsupportedTopology { .. }));
    }

    #[test]
    fn crs_fromstr_invalid() {
        let err = "EPSG:32632".parse::<Crs>().unwrap_err();
        assert!(matches!(err, ManifestError::UnsupportedCrs { .. }));
    }

    #[test]
    fn format_version_fromstr_invalid() {
        let err = "1.0".parse::<FormatVersion>().unwrap_err();
        assert!(matches!(
            err,
            ManifestError::UnsupportedFormatVersion { .. }
        ));
    }
}
