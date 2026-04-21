//! Catchment atom domain type.

use crate::area::AreaKm2;
use crate::geo::{BoundingBox, WkbGeometry};
use crate::id::AtomId;

/// A single catchment atom — the fundamental spatial unit of an HFX dataset.
///
/// Every field is validated at construction time via the primitive newtypes
/// ([`AtomId`], [`AreaKm2`], [`BoundingBox`], [`WkbGeometry`]); `CatchmentAtom`
/// itself performs no additional validation.
#[derive(Debug, Clone, PartialEq)]
pub struct CatchmentAtom {
    id: AtomId,
    area: AreaKm2,
    upstream_area: Option<AreaKm2>,
    bbox: BoundingBox,
    geometry: WkbGeometry,
}

impl CatchmentAtom {
    /// Construct a `CatchmentAtom` from its constituent validated fields.
    ///
    /// All arguments are already domain-typed, so no further validation is
    /// performed here — invalid states are unrepresentable by construction.
    pub fn new(
        id: AtomId,
        area: AreaKm2,
        upstream_area: Option<AreaKm2>,
        bbox: BoundingBox,
        geometry: WkbGeometry,
    ) -> Self {
        Self {
            id,
            area,
            upstream_area,
            bbox,
            geometry,
        }
    }

    /// Return the catchment's unique identifier.
    pub fn id(&self) -> AtomId {
        self.id
    }

    /// Return the local drainage area of this catchment in km².
    pub fn area(&self) -> AreaKm2 {
        self.area
    }

    /// Return the total upstream contributing area in km², if known.
    ///
    /// `None` indicates the value is absent in the source hydrofabric.
    pub fn upstream_area(&self) -> Option<AreaKm2> {
        self.upstream_area
    }

    /// Return a reference to the axis-aligned bounding box of the catchment.
    pub fn bbox(&self) -> &BoundingBox {
        &self.bbox
    }

    /// Return a reference to the WKB geometry of the catchment polygon.
    pub fn geometry(&self) -> &WkbGeometry {
        &self.geometry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_atom_id(raw: i64) -> AtomId {
        AtomId::new(raw).unwrap()
    }

    fn test_bbox() -> BoundingBox {
        BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap()
    }

    fn test_wkb() -> WkbGeometry {
        WkbGeometry::new(vec![0x01, 0x02, 0x03]).unwrap()
    }

    fn test_area(km2: f32) -> AreaKm2 {
        AreaKm2::new(km2).unwrap()
    }

    #[test]
    fn valid_catchment_atom_getters_return_expected_values() {
        let id = test_atom_id(42);
        let area = test_area(100.0);
        let upstream_area = Some(test_area(500.0));
        let bbox = test_bbox();
        let geometry = test_wkb();

        let atom = CatchmentAtom::new(id, area, upstream_area, bbox, geometry.clone());

        assert_eq!(atom.id(), id);
        assert_eq!(atom.area(), area);
        assert_eq!(atom.upstream_area(), upstream_area);
        assert_eq!(atom.bbox(), &bbox);
        assert_eq!(atom.geometry(), &geometry);
    }

    #[test]
    fn upstream_area_none_returns_none() {
        let atom = CatchmentAtom::new(
            test_atom_id(1),
            test_area(50.0),
            None,
            test_bbox(),
            test_wkb(),
        );

        assert_eq!(atom.upstream_area(), None);
    }

    #[test]
    fn upstream_area_some_returns_some() {
        let up_area = test_area(999.9);
        let atom = CatchmentAtom::new(
            test_atom_id(1),
            test_area(50.0),
            Some(up_area),
            test_bbox(),
            test_wkb(),
        );

        assert_eq!(atom.upstream_area(), Some(up_area));
    }
}
