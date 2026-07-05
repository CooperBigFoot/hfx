//! Drainage unit and snap target identity types.

/// Errors from constructing identity types.
#[derive(Debug, thiserror::Error)]
pub enum IdError {
    /// Returned when an ID is zero. ID 0 is reserved by the HFX format.
    #[error("id must be non-zero (0 is reserved by the HFX format)")]
    ZeroId,

    /// Returned when an ID is negative.
    #[error("id must be positive, got {value}")]
    NegativeId {
        /// The invalid raw value.
        value: i64,
    },
}

/// A unique identifier for a drainage unit.
///
/// Invariant: the wrapped value is always strictly positive. Zero is reserved
/// by the HFX format; negatives are invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnitId(i64);

impl UnitId {
    /// Construct a [`UnitId`] from a raw `i64`.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |-----------|---------------|
    /// | `raw == 0` | [`IdError::ZeroId`] |
    /// | `raw < 0` | [`IdError::NegativeId`] |
    pub fn new(raw: i64) -> Result<Self, IdError> {
        match raw {
            0 => Err(IdError::ZeroId),
            v if v < 0 => Err(IdError::NegativeId { value: raw }),
            _ => Ok(Self(raw)),
        }
    }

    /// Return the underlying `i64` value.
    pub fn get(self) -> i64 {
        self.0
    }
}

/// A unique identifier for a snap target.
///
/// Kept as a distinct type from [`UnitId`] so that the two cannot be
/// accidentally mixed at call sites. Invariant: the wrapped value is always
/// strictly positive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapId(i64);

impl SnapId {
    /// Construct a [`SnapId`] from a raw `i64`.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |-----------|---------------|
    /// | `raw == 0` | [`IdError::ZeroId`] |
    /// | `raw < 0` | [`IdError::NegativeId`] |
    pub fn new(raw: i64) -> Result<Self, IdError> {
        match raw {
            0 => Err(IdError::ZeroId),
            v if v < 0 => Err(IdError::NegativeId { value: raw }),
            _ => Ok(Self(raw)),
        }
    }

    /// Return the underlying `i64` value.
    pub fn get(self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_id_accepts_positive() {
        let id = UnitId::new(1).unwrap();
        assert_eq!(id.get(), 1);
    }

    #[test]
    fn unit_id_rejects_zero() {
        assert!(matches!(UnitId::new(0), Err(IdError::ZeroId)));
    }

    #[test]
    fn unit_id_rejects_negative() {
        assert!(matches!(
            UnitId::new(-5),
            Err(IdError::NegativeId { value: -5 })
        ));
    }

    #[test]
    fn snap_id_accepts_positive() {
        let id = SnapId::new(42).unwrap();
        assert_eq!(id.get(), 42);
    }

    #[test]
    fn snap_id_rejects_zero() {
        assert!(matches!(SnapId::new(0), Err(IdError::ZeroId)));
    }

    #[test]
    fn snap_id_rejects_negative() {
        assert!(matches!(
            SnapId::new(-1),
            Err(IdError::NegativeId { value: -1 })
        ));
    }

    #[test]
    fn unit_and_snap_are_distinct_types() {
        // Compile-time check: UnitId and SnapId are not interchangeable.
        // This test simply exercises both constructors to confirm they exist
        // as separate types.
        let _u: UnitId = UnitId::new(10).unwrap();
        let _s: SnapId = SnapId::new(10).unwrap();
    }

    #[test]
    fn unit_id_max_value_succeeds() {
        let id = UnitId::new(i64::MAX).unwrap();
        assert_eq!(id.get(), i64::MAX);
    }

    #[test]
    fn unit_id_min_value_fails_with_negative_id() {
        assert!(matches!(
            UnitId::new(i64::MIN),
            Err(IdError::NegativeId { value: i64::MIN })
        ));
    }

    #[test]
    fn unit_id_equality() {
        let a = UnitId::new(7).unwrap();
        let b = UnitId::new(7).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn unit_id_ordering() {
        let a = UnitId::new(1).unwrap();
        let b = UnitId::new(2).unwrap();
        assert!(a < b);
    }

    #[test]
    fn unit_id_usable_in_hash_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(UnitId::new(1).unwrap());
        set.insert(UnitId::new(2).unwrap());
        set.insert(UnitId::new(1).unwrap());
        assert_eq!(set.len(), 2);
        assert!(set.contains(&UnitId::new(1).unwrap()));
    }
}
