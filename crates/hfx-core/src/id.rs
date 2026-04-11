//! Catchment and snap target identity types.

/// Errors from constructing identity types.
#[derive(Debug, thiserror::Error)]
pub enum IdError {
    /// Returned when an ID is zero. ID 0 is reserved as the terminal sink sentinel.
    #[error("id must be non-zero (0 is reserved as the terminal sink sentinel)")]
    ZeroId,

    /// Returned when an ID is negative.
    #[error("id must be positive, got {value}")]
    NegativeId {
        /// The invalid raw value.
        value: i64,
    },
}

/// A unique identifier for a catchment atom.
///
/// Invariant: the wrapped value is always strictly positive. Zero is reserved as
/// the terminal sink sentinel; negatives are invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AtomId(i64);

impl AtomId {
    /// Construct an [`AtomId`] from a raw `i64`.
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
/// Kept as a distinct type from [`AtomId`] so that the two cannot be
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
    fn atom_id_accepts_positive() {
        let id = AtomId::new(1).unwrap();
        assert_eq!(id.get(), 1);
    }

    #[test]
    fn atom_id_rejects_zero() {
        assert!(matches!(AtomId::new(0), Err(IdError::ZeroId)));
    }

    #[test]
    fn atom_id_rejects_negative() {
        assert!(matches!(
            AtomId::new(-5),
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
    fn atom_and_snap_are_distinct_types() {
        // Compile-time check: AtomId and SnapId are not interchangeable.
        // This test simply exercises both constructors to confirm they exist
        // as separate types.
        let _a: AtomId = AtomId::new(10).unwrap();
        let _s: SnapId = SnapId::new(10).unwrap();
    }

    #[test]
    fn atom_id_max_value_succeeds() {
        let id = AtomId::new(i64::MAX).unwrap();
        assert_eq!(id.get(), i64::MAX);
    }

    #[test]
    fn atom_id_min_value_fails_with_negative_id() {
        assert!(matches!(
            AtomId::new(i64::MIN),
            Err(IdError::NegativeId { value: i64::MIN })
        ));
    }

    #[test]
    fn atom_id_equality() {
        let a = AtomId::new(7).unwrap();
        let b = AtomId::new(7).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn atom_id_ordering() {
        let a = AtomId::new(1).unwrap();
        let b = AtomId::new(2).unwrap();
        assert!(a < b);
    }

    #[test]
    fn atom_id_usable_in_hash_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(AtomId::new(1).unwrap());
        set.insert(AtomId::new(2).unwrap());
        set.insert(AtomId::new(1).unwrap());
        assert_eq!(set.len(), 2);
        assert!(set.contains(&AtomId::new(1).unwrap()));
    }
}
