//! Dataset-local drainage-unit level type.

/// Errors from constructing [`Level`].
#[derive(Debug, thiserror::Error)]
pub enum LevelError {
    /// Returned when a level is negative.
    #[error("level must be non-negative, got {value}")]
    Negative {
        /// The invalid raw value.
        value: i16,
    },
}

/// Dataset-local drainage-unit resolution tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Level(i16);

impl Level {
    /// Construct a [`Level`] from a raw `i16`.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |-----------|---------------|
    /// | `raw < 0` | [`LevelError::Negative`] |
    pub fn new(raw: i16) -> Result<Self, LevelError> {
        if raw < 0 {
            return Err(LevelError::Negative { value: raw });
        }
        Ok(Self(raw))
    }

    /// Return the underlying `i16` value.
    pub fn get(self) -> i16 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_zero() {
        let level = Level::new(0).unwrap();
        assert_eq!(level.get(), 0);
    }

    #[test]
    fn accepts_positive() {
        let level = Level::new(7).unwrap();
        assert_eq!(level.get(), 7);
    }

    #[test]
    fn rejects_negative() {
        assert!(matches!(
            Level::new(-1),
            Err(LevelError::Negative { value: -1 })
        ));
    }
}
