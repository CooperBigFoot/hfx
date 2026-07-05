//! Drainage-unit domain type.

use crate::area::AreaKm2;
use crate::geo::{BoundingBox, OutletCoord, WkbGeometry};
use crate::id::UnitId;
use crate::level::Level;

/// A single drainage unit in an HFX dataset.
///
/// Every field is validated at construction time via the primitive newtypes
/// ([`UnitId`], [`Level`], [`AreaKm2`], [`OutletCoord`], [`BoundingBox`],
/// [`WkbGeometry`]); `CatchmentUnit`
/// itself performs no additional validation.
#[derive(Debug, Clone, PartialEq)]
pub struct CatchmentUnit {
    id: UnitId,
    level: Level,
    parent_id: Option<UnitId>,
    area: AreaKm2,
    upstream_area: Option<AreaKm2>,
    outlet: OutletCoord,
    bbox: BoundingBox,
    geometry: WkbGeometry,
    source_id: Option<String>,
    level_label: Option<String>,
}

impl CatchmentUnit {
    /// Construct a `CatchmentUnit` from its constituent validated fields.
    ///
    /// All arguments are already domain-typed, so no further validation is
    /// performed here — invalid states are unrepresentable by construction.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: UnitId,
        level: Level,
        parent_id: Option<UnitId>,
        area: AreaKm2,
        upstream_area: Option<AreaKm2>,
        outlet: OutletCoord,
        bbox: BoundingBox,
        geometry: WkbGeometry,
        source_id: Option<String>,
        level_label: Option<String>,
    ) -> Self {
        Self {
            id,
            level,
            parent_id,
            area,
            upstream_area,
            outlet,
            bbox,
            geometry,
            source_id,
            level_label,
        }
    }

    /// Return the unit's unique identifier.
    pub fn id(&self) -> UnitId {
        self.id
    }

    /// Return the dataset-local level of this unit.
    pub fn level(&self) -> Level {
        self.level
    }

    /// Return the optional containing coarser unit ID.
    pub fn parent_id(&self) -> Option<UnitId> {
        self.parent_id
    }

    /// Return the local drainage area of this unit in km².
    pub fn area(&self) -> AreaKm2 {
        self.area
    }

    /// Return the total upstream contributing area in km², if known.
    ///
    /// `None` indicates the value is absent in the source hydrofabric.
    pub fn upstream_area(&self) -> Option<AreaKm2> {
        self.upstream_area
    }

    /// Return the unit outlet coordinate.
    pub fn outlet(&self) -> OutletCoord {
        self.outlet
    }

    /// Return a reference to the axis-aligned bounding box of the catchment.
    pub fn bbox(&self) -> &BoundingBox {
        &self.bbox
    }

    /// Return a reference to the WKB geometry of the catchment polygon.
    pub fn geometry(&self) -> &WkbGeometry {
        &self.geometry
    }

    /// Return the optional source-fabric identifier.
    pub fn source_id(&self) -> Option<&str> {
        self.source_id.as_deref()
    }

    /// Return the optional source-fabric level label.
    pub fn level_label(&self) -> Option<&str> {
        self.level_label.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_unit_id(raw: i64) -> UnitId {
        UnitId::new(raw).unwrap()
    }

    fn test_level(raw: i16) -> Level {
        Level::new(raw).unwrap()
    }

    fn test_outlet() -> OutletCoord {
        OutletCoord::new(0.0, 0.0).unwrap()
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
    fn valid_catchment_unit_getters_return_expected_values() {
        let id = test_unit_id(42);
        let level = test_level(1);
        let parent_id = Some(test_unit_id(7));
        let area = test_area(100.0);
        let upstream_area = Some(test_area(500.0));
        let outlet = test_outlet();
        let bbox = test_bbox();
        let geometry = test_wkb();

        let unit = CatchmentUnit::new(
            id,
            level,
            parent_id,
            area,
            upstream_area,
            outlet,
            bbox,
            geometry.clone(),
            Some("src-42".to_string()),
            Some("l1".to_string()),
        );

        assert_eq!(unit.id(), id);
        assert_eq!(unit.level(), level);
        assert_eq!(unit.parent_id(), parent_id);
        assert_eq!(unit.area(), area);
        assert_eq!(unit.upstream_area(), upstream_area);
        assert_eq!(unit.outlet(), outlet);
        assert_eq!(unit.bbox(), &bbox);
        assert_eq!(unit.geometry(), &geometry);
        assert_eq!(unit.source_id(), Some("src-42"));
        assert_eq!(unit.level_label(), Some("l1"));
    }

    #[test]
    fn upstream_area_none_returns_none() {
        let unit = CatchmentUnit::new(
            test_unit_id(1),
            test_level(0),
            None,
            test_area(50.0),
            None,
            test_outlet(),
            test_bbox(),
            test_wkb(),
            None,
            None,
        );

        assert_eq!(unit.upstream_area(), None);
    }

    #[test]
    fn upstream_area_some_returns_some() {
        let up_area = test_area(999.9);
        let unit = CatchmentUnit::new(
            test_unit_id(1),
            test_level(0),
            None,
            test_area(50.0),
            Some(up_area),
            test_outlet(),
            test_bbox(),
            test_wkb(),
            None,
            None,
        );

        assert_eq!(unit.upstream_area(), Some(up_area));
    }
}
