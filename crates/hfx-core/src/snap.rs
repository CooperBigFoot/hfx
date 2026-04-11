//! Snap target domain types.

use crate::area::Weight;
use crate::geo::{BoundingBox, WkbGeometry};
use crate::id::{AtomId, SnapId};

/// Indicates whether a snap target lies on the mainstem channel or a
/// tributary/distributary of the drainage network.
///
/// Using an enum rather than a `bool` makes call sites self-documenting and
/// prevents accidental inversion of the flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MainstemStatus {
    /// This feature is on the mainstem channel.
    Mainstem,
    /// This feature is on a tributary or distributary.
    Tributary,
}

/// A candidate location to which a pour point may be snapped.
///
/// Each `SnapTarget` belongs to exactly one [`CatchmentAtom`] (via
/// `catchment_id`) and carries a proportional [`Weight`] used when multiple
/// targets compete within the same catchment. The [`MainstemStatus`] lets
/// snapping algorithms prefer mainstem reaches over tributaries.
///
/// All fields are validated at construction time via their primitive newtypes;
/// `SnapTarget` itself performs no additional validation.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapTarget {
    id: SnapId,
    catchment_id: AtomId,
    weight: Weight,
    mainstem_status: MainstemStatus,
    bbox: BoundingBox,
    geometry: WkbGeometry,
}

impl SnapTarget {
    /// Construct a `SnapTarget` from its constituent validated fields.
    ///
    /// All arguments are already domain-typed, so no further validation is
    /// performed here — invalid states are unrepresentable by construction.
    pub fn new(
        id: SnapId,
        catchment_id: AtomId,
        weight: Weight,
        mainstem_status: MainstemStatus,
        bbox: BoundingBox,
        geometry: WkbGeometry,
    ) -> Self {
        Self { id, catchment_id, weight, mainstem_status, bbox, geometry }
    }

    /// Return the snap target's unique identifier.
    pub fn id(&self) -> SnapId {
        self.id
    }

    /// Return the identifier of the catchment atom this target belongs to.
    pub fn catchment_id(&self) -> AtomId {
        self.catchment_id
    }

    /// Return the proportional weight used for allocation across competing targets.
    pub fn weight(&self) -> Weight {
        self.weight
    }

    /// Return whether this target lies on the mainstem or a tributary.
    pub fn mainstem_status(&self) -> MainstemStatus {
        self.mainstem_status
    }

    /// Return a reference to the axis-aligned bounding box of this snap target.
    pub fn bbox(&self) -> &BoundingBox {
        &self.bbox
    }

    /// Return a reference to the WKB geometry of this snap target (typically a
    /// linestring or point).
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

    fn test_snap_id(raw: i64) -> SnapId {
        SnapId::new(raw).unwrap()
    }

    fn test_bbox() -> BoundingBox {
        BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap()
    }

    fn test_wkb() -> WkbGeometry {
        WkbGeometry::new(vec![0x01, 0x02, 0x03]).unwrap()
    }

    fn test_weight(raw: f32) -> Weight {
        Weight::new(raw).unwrap()
    }

    #[test]
    fn mainstem_status_variants_are_not_equal() {
        assert_ne!(MainstemStatus::Mainstem, MainstemStatus::Tributary);
    }

    #[test]
    fn mainstem_status_can_be_copied_and_compared() {
        let status = MainstemStatus::Mainstem;
        let copy = status;
        assert_eq!(status, copy);

        let tributary = MainstemStatus::Tributary;
        let copy2 = tributary;
        assert_eq!(tributary, copy2);
    }

    #[test]
    fn snap_target_getters_return_expected_values() {
        let snap_id = test_snap_id(7);
        let catchment_id = test_atom_id(3);
        let weight = test_weight(0.75);
        let mainstem_status = MainstemStatus::Mainstem;
        let bbox = test_bbox();
        let geometry = test_wkb();

        let target = SnapTarget::new(
            snap_id,
            catchment_id,
            weight,
            mainstem_status,
            bbox,
            geometry.clone(),
        );

        assert_eq!(target.id(), snap_id);
        assert_eq!(target.catchment_id(), catchment_id);
        assert_eq!(target.weight(), weight);
        assert_eq!(target.mainstem_status(), MainstemStatus::Mainstem);
        assert_eq!(target.bbox(), &bbox);
        assert_eq!(target.geometry(), &geometry);
    }

    #[test]
    fn catchment_id_returns_atom_id_passed_to_constructor() {
        let catchment_id = test_atom_id(99);
        let target = SnapTarget::new(
            test_snap_id(1),
            catchment_id,
            test_weight(1.0),
            MainstemStatus::Tributary,
            test_bbox(),
            test_wkb(),
        );

        assert_eq!(target.catchment_id(), catchment_id);
    }
}
