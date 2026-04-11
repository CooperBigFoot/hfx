//! Area and weight measurement types.

/// Errors from constructing measured quantities.
#[derive(Debug, thiserror::Error)]
pub enum MeasureError {
    /// Returned when a value is negative.
    #[error("value must be non-negative, got {value}")]
    NegativeValue {
        /// The invalid value.
        value: f32,
    },

    /// Returned when a value is NaN or infinite.
    #[error("value must be finite, got {value}")]
    NonFiniteValue {
        /// The invalid value.
        value: f32,
    },
}

/// A catchment area expressed in square kilometres.
///
/// Invariant: the wrapped value is always finite and non-negative.
///
/// `Eq` is intentionally not derived — deriving `Eq` on `f32` is a Rust
/// footgun because IEEE-754 NaN != NaN. The constructor ensures the value is
/// finite, but the derive would still be misleading. Use `PartialEq` directly
/// or compare via [`AreaKm2::get`].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AreaKm2(f32);

impl AreaKm2 {
    /// Construct an [`AreaKm2`] from a raw `f32`.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |-----------|---------------|
    /// | `raw` is NaN or infinite | [`MeasureError::NonFiniteValue`] |
    /// | `raw < 0.0` | [`MeasureError::NegativeValue`] |
    pub fn new(raw: f32) -> Result<Self, MeasureError> {
        if !raw.is_finite() {
            return Err(MeasureError::NonFiniteValue { value: raw });
        }
        if raw < 0.0 {
            return Err(MeasureError::NegativeValue { value: raw });
        }
        Ok(Self(raw))
    }

    /// Return the underlying `f32` value.
    pub fn get(self) -> f32 {
        self.0
    }
}

/// Snap ranking priority weight.
///
/// Higher values indicate preferred snap targets during outlet resolution.
/// Typically upstream drainage area in km² or upstream cell count.
/// Invariant: the wrapped value is always finite and non-negative.
///
/// `Eq` is intentionally not derived for the same reason as [`AreaKm2`].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Weight(f32);

impl Weight {
    /// Construct a [`Weight`] from a raw `f32`.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |-----------|---------------|
    /// | `raw` is NaN or infinite | [`MeasureError::NonFiniteValue`] |
    /// | `raw < 0.0` | [`MeasureError::NegativeValue`] |
    pub fn new(raw: f32) -> Result<Self, MeasureError> {
        if !raw.is_finite() {
            return Err(MeasureError::NonFiniteValue { value: raw });
        }
        if raw < 0.0 {
            return Err(MeasureError::NegativeValue { value: raw });
        }
        Ok(Self(raw))
    }

    /// Return the underlying `f32` value.
    pub fn get(self) -> f32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn area_km2_accepts_zero() {
        let a = AreaKm2::new(0.0).unwrap();
        assert_eq!(a.get(), 0.0);
    }

    #[test]
    fn area_km2_accepts_positive() {
        let a = AreaKm2::new(123.45).unwrap();
        assert_eq!(a.get(), 123.45);
    }

    #[test]
    fn area_km2_rejects_negative() {
        assert!(matches!(
            AreaKm2::new(-1.0),
            Err(MeasureError::NegativeValue { value: _ })
        ));
    }

    #[test]
    fn area_km2_rejects_nan() {
        assert!(matches!(
            AreaKm2::new(f32::NAN),
            Err(MeasureError::NonFiniteValue { value: _ })
        ));
    }

    #[test]
    fn area_km2_rejects_inf() {
        assert!(matches!(
            AreaKm2::new(f32::INFINITY),
            Err(MeasureError::NonFiniteValue { value: _ })
        ));
    }

    #[test]
    fn area_km2_rejects_neg_inf() {
        assert!(matches!(
            AreaKm2::new(f32::NEG_INFINITY),
            Err(MeasureError::NonFiniteValue { value: _ })
        ));
    }

    #[test]
    fn weight_accepts_zero() {
        let w = Weight::new(0.0).unwrap();
        assert_eq!(w.get(), 0.0);
    }

    #[test]
    fn weight_accepts_positive() {
        let w = Weight::new(0.75).unwrap();
        assert_eq!(w.get(), 0.75);
    }

    #[test]
    fn weight_rejects_negative() {
        assert!(matches!(
            Weight::new(-0.1),
            Err(MeasureError::NegativeValue { value: _ })
        ));
    }

    #[test]
    fn weight_rejects_nan() {
        assert!(matches!(
            Weight::new(f32::NAN),
            Err(MeasureError::NonFiniteValue { value: _ })
        ));
    }

    #[test]
    fn weight_rejects_inf() {
        assert!(matches!(
            Weight::new(f32::INFINITY),
            Err(MeasureError::NonFiniteValue { value: _ })
        ));
    }

    #[test]
    fn area_km2_min_positive_succeeds() {
        let a = AreaKm2::new(f32::MIN_POSITIVE).unwrap();
        assert_eq!(a.get(), f32::MIN_POSITIVE);
    }

    #[test]
    fn weight_neg_infinity_fails_with_non_finite_not_negative() {
        // Finiteness is checked before the sign check, so NEG_INFINITY must
        // produce NonFiniteValue, not NegativeValue.
        assert!(matches!(
            Weight::new(f32::NEG_INFINITY),
            Err(MeasureError::NonFiniteValue { value: _ })
        ));
    }
}
