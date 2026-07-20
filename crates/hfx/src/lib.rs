//! Shared types and validation primitives for HFX artifacts.
//!
//! This crate defines the canonical in-memory representation of HFX
//! domain concepts. All types enforce their invariants at construction
//! time ("parse, don't validate"). No I/O — deserialization from
//! Parquet/JSON/GeoTIFF is the responsibility of downstream crates.

pub mod area;
pub mod auxiliary;
pub mod catchment;
pub mod geo;
pub mod graph;
pub mod id;
pub mod level;
pub mod manifest;
pub mod raster;
pub mod snap;

// --- Re-exports for ergonomic imports ---

pub use area::{AreaKm2, MeasureError, Weight};
pub use auxiliary::{AuxiliaryDecl, AuxiliaryError, AuxiliarySchemaId, BlessedAuxSchema};
pub use catchment::CatchmentUnit;
pub use geo::{BoundingBox, GeoError, Latitude, Longitude, OutletCoord, WkbGeometry};
pub use graph::{AdjacencyRow, DrainageGraph, GraphError};
pub use id::{IdError, SnapId, UnitId};
pub use level::{Level, LevelError};
pub use manifest::{
    Crs, FormatVersion, Manifest, ManifestBuilder, ManifestError, Topology, UnitCount,
    UpAreaAvailability,
};
pub use raster::{
    D8RasterMetadataV2, D8RasterMetadataV2Error, EpsgCode, EpsgCodeError, FlowAccumulationUnits,
    FlowAccumulationUnitsError, FlowDirEncoding, FlowDirEncodingError,
};
pub use snap::{SnapError, SnapTarget, StemRole};

/// Trait for types that carry a spatial bounding box.
///
/// Enables generic spatial filtering over catchments or any future artifact row type.
pub trait HasBbox {
    /// Return a reference to the bounding box.
    fn bbox(&self) -> &BoundingBox;
}

impl HasBbox for CatchmentUnit {
    fn bbox(&self) -> &BoundingBox {
        catchment::CatchmentUnit::bbox(self)
    }
}

/// Trait for types identified by a [`UnitId`].
///
/// Enables generic operations over catchments, graph rows, and snap targets.
pub trait HasUnitId {
    /// Return the unit identifier.
    fn unit_id(&self) -> UnitId;
}

impl HasUnitId for CatchmentUnit {
    fn unit_id(&self) -> UnitId {
        self.id()
    }
}

impl HasUnitId for AdjacencyRow {
    fn unit_id(&self) -> UnitId {
        self.id()
    }
}

impl HasUnitId for SnapTarget {
    fn unit_id(&self) -> UnitId {
        self.unit_id()
    }
}
