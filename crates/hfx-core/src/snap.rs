//! Snap target domain types.

use crate::area::Weight;
use crate::geo::{BoundingBox, WkbGeometry};
use crate::id::{SnapId, UnitId};

/// Errors from constructing snap-target domain values.
#[derive(Debug, thiserror::Error)]
pub enum SnapError {
    /// Returned when a stem role string is not supported by HFX v0.2.
    #[error("unsupported stem role: {value:?}")]
    UnsupportedStemRole {
        /// The unsupported raw value.
        value: String,
    },
}

/// Indicates whether a snap target lies on the mainstem channel, a tributary,
/// or an unknown stem role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StemRole {
    /// This feature is on the mainstem channel.
    Mainstem,
    /// This feature is on a tributary or distributary.
    Tributary,
    /// The producer does not know or does not declare the stem role.
    Unknown,
}

impl std::fmt::Display for StemRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StemRole::Mainstem => write!(f, "mainstem"),
            StemRole::Tributary => write!(f, "tributary"),
            StemRole::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::str::FromStr for StemRole {
    type Err = SnapError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mainstem" => Ok(StemRole::Mainstem),
            "tributary" => Ok(StemRole::Tributary),
            "unknown" => Ok(StemRole::Unknown),
            _ => Err(SnapError::UnsupportedStemRole {
                value: s.to_owned(),
            }),
        }
    }
}

/// A candidate location to which a pour point may be snapped.
///
/// Each `SnapTarget` belongs to exactly one drainage unit and carries a
/// proportional [`Weight`] used when multiple targets compete within the same
/// unit.
///
/// All fields are validated at construction time via their primitive newtypes;
/// `SnapTarget` itself performs no additional validation.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapTarget {
    id: SnapId,
    unit_id: UnitId,
    weight: Weight,
    stem_role: Option<StemRole>,
    bbox: Option<BoundingBox>,
    geometry: WkbGeometry,
}

impl SnapTarget {
    /// Construct a `SnapTarget` from its constituent validated fields.
    ///
    /// All arguments are already domain-typed, so no further validation is
    /// performed here — invalid states are unrepresentable by construction.
    pub fn new(
        id: SnapId,
        unit_id: UnitId,
        weight: Weight,
        stem_role: Option<StemRole>,
        bbox: Option<BoundingBox>,
        geometry: WkbGeometry,
    ) -> Self {
        Self {
            id,
            unit_id,
            weight,
            stem_role,
            bbox,
            geometry,
        }
    }

    /// Return the snap target's unique identifier.
    pub fn id(&self) -> SnapId {
        self.id
    }

    /// Return the identifier of the drainage unit this target belongs to.
    pub fn unit_id(&self) -> UnitId {
        self.unit_id
    }

    /// Return the proportional weight used for allocation across competing targets.
    pub fn weight(&self) -> Weight {
        self.weight
    }

    /// Return the optional stem role for this target.
    pub fn stem_role(&self) -> Option<StemRole> {
        self.stem_role
    }

    /// Return a reference to the optional axis-aligned bounding box.
    pub fn bbox(&self) -> Option<&BoundingBox> {
        self.bbox.as_ref()
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

    fn test_unit_id(raw: i64) -> UnitId {
        UnitId::new(raw).unwrap()
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
    fn stem_role_variants_are_not_equal() {
        assert_ne!(StemRole::Mainstem, StemRole::Tributary);
        assert_ne!(StemRole::Mainstem, StemRole::Unknown);
    }

    #[test]
    fn stem_role_can_be_copied_and_compared() {
        let status = StemRole::Mainstem;
        let copy = status;
        assert_eq!(status, copy);

        let tributary = StemRole::Tributary;
        let copy2 = tributary;
        assert_eq!(tributary, copy2);
    }

    #[test]
    fn stem_role_parse_accepts_supported_values() {
        assert_eq!("mainstem".parse::<StemRole>().unwrap(), StemRole::Mainstem);
        assert_eq!(
            "tributary".parse::<StemRole>().unwrap(),
            StemRole::Tributary
        );
        assert_eq!("unknown".parse::<StemRole>().unwrap(), StemRole::Unknown);
    }

    #[test]
    fn stem_role_parse_rejects_unknown_value() {
        assert!(matches!(
            "primary".parse::<StemRole>(),
            Err(SnapError::UnsupportedStemRole { value }) if value == "primary"
        ));
    }

    #[test]
    fn snap_target_getters_return_expected_values() {
        let snap_id = test_snap_id(7);
        let unit_id = test_unit_id(3);
        let weight = test_weight(0.75);
        let stem_role = Some(StemRole::Mainstem);
        let bbox = test_bbox();
        let geometry = test_wkb();

        let target = SnapTarget::new(
            snap_id,
            unit_id,
            weight,
            stem_role,
            Some(bbox),
            geometry.clone(),
        );

        assert_eq!(target.id(), snap_id);
        assert_eq!(target.unit_id(), unit_id);
        assert_eq!(target.weight(), weight);
        assert_eq!(target.stem_role(), Some(StemRole::Mainstem));
        assert_eq!(target.bbox(), Some(&bbox));
        assert_eq!(target.geometry(), &geometry);
    }

    #[test]
    fn unit_id_returns_unit_id_passed_to_constructor() {
        let unit_id = test_unit_id(99);
        let target = SnapTarget::new(
            test_snap_id(1),
            unit_id,
            test_weight(1.0),
            Some(StemRole::Tributary),
            None,
            test_wkb(),
        );

        assert_eq!(target.unit_id(), unit_id);
        assert_eq!(target.bbox(), None);
    }
}
