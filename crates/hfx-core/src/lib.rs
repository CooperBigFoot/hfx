//! Shared types and validation primitives for HFX artifacts.
//!
//! This crate defines the canonical in-memory representation of HFX
//! domain concepts. All types enforce their invariants at construction
//! time ("parse, don't validate"). No I/O — deserialization from
//! Parquet/Arrow/JSON/GeoTIFF is the responsibility of downstream crates.

pub mod area;
pub mod catchment;
pub mod geo;
pub mod graph;
pub mod id;
pub mod manifest;
pub mod raster;
pub mod snap;

// --- Re-exports for ergonomic imports ---

pub use area::{AreaKm2, MeasureError, Weight};
pub use catchment::CatchmentAtom;
pub use geo::{BoundingBox, GeoError, Latitude, Longitude, WkbGeometry};
pub use graph::{AdjacencyRow, DrainageGraph, GraphError};
pub use id::{AtomId, IdError, SnapId};
pub use manifest::{
    AtomCount, FormatVersion, Manifest, ManifestBuilder, ManifestError, RasterAvailability,
    SnapAvailability, Topology, UpAreaAvailability,
};
pub use raster::FlowDirEncoding;
pub use snap::{MainstemStatus, SnapTarget};

/// Trait for types that carry a spatial bounding box.
///
/// Enables generic spatial filtering over catchments, snap targets,
/// or any future artifact row type.
pub trait HasBbox {
    /// Return a reference to the bounding box.
    fn bbox(&self) -> &BoundingBox;
}

impl HasBbox for CatchmentAtom {
    fn bbox(&self) -> &BoundingBox {
        catchment::CatchmentAtom::bbox(self)
    }
}

impl HasBbox for SnapTarget {
    fn bbox(&self) -> &BoundingBox {
        snap::SnapTarget::bbox(self)
    }
}

/// Trait for types identified by an [`AtomId`].
///
/// Enables generic operations over catchments and graph rows.
pub trait HasAtomId {
    /// Return the atom identifier.
    fn atom_id(&self) -> AtomId;
}

impl HasAtomId for CatchmentAtom {
    fn atom_id(&self) -> AtomId {
        self.id()
    }
}

impl HasAtomId for AdjacencyRow {
    fn atom_id(&self) -> AtomId {
        self.id()
    }
}
