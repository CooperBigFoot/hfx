//! Spatial primitives for WGS84 coordinates, bounding boxes, and WKB geometry.

/// Errors from constructing spatial primitives.
#[derive(Debug, thiserror::Error)]
pub enum GeoError {
    /// Returned when a longitude is outside [-180, 180].
    #[error("longitude out of range [-180, 180]: {value}")]
    LongitudeOutOfRange {
        /// The invalid longitude value.
        value: f32,
    },

    /// Returned when a latitude is outside [-90, 90].
    #[error("latitude out of range [-90, 90]: {value}")]
    LatitudeOutOfRange {
        /// The invalid latitude value.
        value: f32,
    },

    /// Returned when a bounding box has min >= max on an axis.
    #[error("degenerate bounding box: {axis} min ({min}) >= max ({max})")]
    DegenerateBbox {
        /// Which axis is degenerate ("x" or "y").
        axis: &'static str,
        /// The minimum value.
        min: f32,
        /// The maximum value.
        max: f32,
    },

    /// Returned when WKB geometry bytes are empty.
    #[error("geometry must not be empty")]
    EmptyGeometry,

    /// Returned when a coordinate is NaN or infinite.
    #[error("coordinate must be finite, got {value}")]
    NonFiniteCoordinate {
        /// The non-finite value.
        value: f32,
    },
}

/// A validated WGS84 longitude in the range [-180.0, 180.0].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Longitude(f32);

impl Longitude {
    /// Constructs a `Longitude` from a raw `f32`, rejecting non-finite values
    /// and values outside [-180.0, 180.0].
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`GeoError::NonFiniteCoordinate`] | `raw` is NaN or infinite |
    /// | [`GeoError::LongitudeOutOfRange`] | `raw` is outside [-180.0, 180.0] |
    pub fn new(raw: f32) -> Result<Self, GeoError> {
        if !raw.is_finite() {
            return Err(GeoError::NonFiniteCoordinate { value: raw });
        }
        if !(-180.0..=180.0).contains(&raw) {
            return Err(GeoError::LongitudeOutOfRange { value: raw });
        }
        Ok(Self(raw))
    }

    /// Returns the raw longitude value.
    pub fn get(self) -> f32 {
        self.0
    }
}

/// A validated WGS84 latitude in the range [-90.0, 90.0].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Latitude(f32);

impl Latitude {
    /// Constructs a `Latitude` from a raw `f32`, rejecting non-finite values
    /// and values outside [-90.0, 90.0].
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`GeoError::NonFiniteCoordinate`] | `raw` is NaN or infinite |
    /// | [`GeoError::LatitudeOutOfRange`] | `raw` is outside [-90.0, 90.0] |
    pub fn new(raw: f32) -> Result<Self, GeoError> {
        if !raw.is_finite() {
            return Err(GeoError::NonFiniteCoordinate { value: raw });
        }
        if !(-90.0..=90.0).contains(&raw) {
            return Err(GeoError::LatitudeOutOfRange { value: raw });
        }
        Ok(Self(raw))
    }

    /// Returns the raw latitude value.
    pub fn get(self) -> f32 {
        self.0
    }
}

/// An axis-aligned bounding box in WGS84 coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    min_x: Longitude,
    min_y: Latitude,
    max_x: Longitude,
    max_y: Latitude,
}

impl BoundingBox {
    /// Constructs a `BoundingBox` from raw coordinate values.
    ///
    /// Validates each coordinate individually, then checks that the box is
    /// non-degenerate (`minx < maxx` and `miny < maxy`).
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`GeoError::NonFiniteCoordinate`] | Any value is NaN or infinite |
    /// | [`GeoError::LongitudeOutOfRange`] | `minx` or `maxx` outside [-180, 180] |
    /// | [`GeoError::LatitudeOutOfRange`] | `miny` or `maxy` outside [-90, 90] |
    /// | [`GeoError::DegenerateBbox`] | `minx >= maxx` or `miny >= maxy` |
    pub fn new(minx: f32, miny: f32, maxx: f32, maxy: f32) -> Result<Self, GeoError> {
        let min_x = Longitude::new(minx)?;
        let max_x = Longitude::new(maxx)?;
        let min_y = Latitude::new(miny)?;
        let max_y = Latitude::new(maxy)?;

        if minx >= maxx {
            return Err(GeoError::DegenerateBbox {
                axis: "x",
                min: minx,
                max: maxx,
            });
        }
        if miny >= maxy {
            return Err(GeoError::DegenerateBbox {
                axis: "y",
                min: miny,
                max: maxy,
            });
        }

        Ok(Self { min_x, min_y, max_x, max_y })
    }

    /// Returns the western boundary longitude.
    pub fn min_x(&self) -> Longitude {
        self.min_x
    }

    /// Returns the southern boundary latitude.
    pub fn min_y(&self) -> Latitude {
        self.min_y
    }

    /// Returns the eastern boundary longitude.
    pub fn max_x(&self) -> Longitude {
        self.max_x
    }

    /// Returns the northern boundary latitude.
    pub fn max_y(&self) -> Latitude {
        self.max_y
    }

    /// Returns `true` if the given coordinate falls within or on the boundary
    /// of this bounding box.
    pub fn contains(&self, lon: Longitude, lat: Latitude) -> bool {
        lon.get() >= self.min_x.get()
            && lon.get() <= self.max_x.get()
            && lat.get() >= self.min_y.get()
            && lat.get() <= self.max_y.get()
    }

    /// Returns `true` if this bounding box overlaps with `other` (including
    /// edge-touching).
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        self.min_x.get() <= other.max_x.get()
            && self.max_x.get() >= other.min_x.get()
            && self.min_y.get() <= other.max_y.get()
            && self.max_y.get() >= other.min_y.get()
    }
}

/// Opaque WKB (Well-Known Binary) geometry bytes.
///
/// `hfx-core` treats WKB as a raw byte buffer and does not parse its internal
/// structure. Callers that need geometry operations should use a dedicated
/// geometry library (e.g. `geo`, `geos`).
#[derive(Debug, Clone, PartialEq)]
pub struct WkbGeometry(Vec<u8>);

impl WkbGeometry {
    /// Wraps a raw WKB byte vector, rejecting empty inputs.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`GeoError::EmptyGeometry`] | `raw` is empty |
    pub fn new(raw: Vec<u8>) -> Result<Self, GeoError> {
        if raw.is_empty() {
            return Err(GeoError::EmptyGeometry);
        }
        Ok(Self(raw))
    }

    /// Returns a byte slice of the raw WKB data.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the wrapper and returns the raw WKB byte vector.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Longitude ---

    #[test]
    fn longitude_valid_boundaries() {
        assert!(Longitude::new(-180.0).is_ok());
        assert!(Longitude::new(180.0).is_ok());
        assert!(Longitude::new(0.0).is_ok());
    }

    #[test]
    fn longitude_out_of_range() {
        assert!(matches!(
            Longitude::new(180.1),
            Err(GeoError::LongitudeOutOfRange { value }) if (value - 180.1).abs() < 0.001
        ));
        assert!(matches!(
            Longitude::new(-180.1),
            Err(GeoError::LongitudeOutOfRange { .. })
        ));
    }

    #[test]
    fn longitude_non_finite() {
        assert!(matches!(
            Longitude::new(f32::NAN),
            Err(GeoError::NonFiniteCoordinate { .. })
        ));
        assert!(matches!(
            Longitude::new(f32::INFINITY),
            Err(GeoError::NonFiniteCoordinate { .. })
        ));
    }

    #[test]
    fn longitude_get_roundtrips() {
        let lon = Longitude::new(42.5).unwrap();
        assert!((lon.get() - 42.5).abs() < f32::EPSILON);
    }

    // --- Latitude ---

    #[test]
    fn latitude_valid_boundaries() {
        assert!(Latitude::new(-90.0).is_ok());
        assert!(Latitude::new(90.0).is_ok());
        assert!(Latitude::new(0.0).is_ok());
    }

    #[test]
    fn latitude_out_of_range() {
        assert!(matches!(
            Latitude::new(90.1),
            Err(GeoError::LatitudeOutOfRange { .. })
        ));
        assert!(matches!(
            Latitude::new(-90.1),
            Err(GeoError::LatitudeOutOfRange { .. })
        ));
    }

    #[test]
    fn latitude_non_finite() {
        assert!(matches!(
            Latitude::new(f32::NEG_INFINITY),
            Err(GeoError::NonFiniteCoordinate { .. })
        ));
    }

    // --- BoundingBox ---

    #[test]
    fn bbox_valid() {
        let bb = BoundingBox::new(-10.0, -5.0, 10.0, 5.0);
        assert!(bb.is_ok());
    }

    #[test]
    fn bbox_degenerate_x() {
        assert!(matches!(
            BoundingBox::new(5.0, -5.0, 5.0, 5.0),
            Err(GeoError::DegenerateBbox { axis: "x", .. })
        ));
    }

    #[test]
    fn bbox_degenerate_y() {
        assert!(matches!(
            BoundingBox::new(-5.0, 5.0, 5.0, 5.0),
            Err(GeoError::DegenerateBbox { axis: "y", .. })
        ));
    }

    #[test]
    fn bbox_non_finite_propagates() {
        assert!(matches!(
            BoundingBox::new(f32::NAN, 0.0, 10.0, 5.0),
            Err(GeoError::NonFiniteCoordinate { .. })
        ));
    }

    #[test]
    fn bbox_contains() {
        let bb = BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap();
        let inside_lon = Longitude::new(0.0).unwrap();
        let inside_lat = Latitude::new(0.0).unwrap();
        assert!(bb.contains(inside_lon, inside_lat));

        let outside_lon = Longitude::new(15.0).unwrap();
        assert!(!bb.contains(outside_lon, inside_lat));
    }

    #[test]
    fn bbox_contains_on_boundary() {
        let bb = BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap();
        let edge_lon = Longitude::new(-10.0).unwrap();
        let edge_lat = Latitude::new(5.0).unwrap();
        assert!(bb.contains(edge_lon, edge_lat));
    }

    #[test]
    fn bbox_intersects() {
        let a = BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap();
        let b = BoundingBox::new(5.0, 0.0, 20.0, 10.0).unwrap();
        assert!(a.intersects(&b));
        assert!(b.intersects(&a));
    }

    #[test]
    fn bbox_no_intersect() {
        let a = BoundingBox::new(-10.0, -5.0, 0.0, 5.0).unwrap();
        let b = BoundingBox::new(5.0, -5.0, 10.0, 5.0).unwrap();
        assert!(!a.intersects(&b));
    }

    #[test]
    fn bbox_getters() {
        let bb = BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap();
        assert!((bb.min_x().get() - (-10.0)).abs() < f32::EPSILON);
        assert!((bb.min_y().get() - (-5.0)).abs() < f32::EPSILON);
        assert!((bb.max_x().get() - 10.0).abs() < f32::EPSILON);
        assert!((bb.max_y().get() - 5.0).abs() < f32::EPSILON);
    }

    // --- WkbGeometry ---

    #[test]
    fn wkb_valid() {
        let geom = WkbGeometry::new(vec![0x01, 0x02, 0x03]);
        assert!(geom.is_ok());
    }

    #[test]
    fn wkb_empty_rejected() {
        assert!(matches!(WkbGeometry::new(vec![]), Err(GeoError::EmptyGeometry)));
    }

    #[test]
    fn wkb_as_bytes() {
        let geom = WkbGeometry::new(vec![0xDE, 0xAD]).unwrap();
        assert_eq!(geom.as_bytes(), &[0xDE, 0xAD]);
    }

    #[test]
    fn wkb_into_bytes() {
        let raw = vec![0xBE, 0xEF];
        let geom = WkbGeometry::new(raw.clone()).unwrap();
        assert_eq!(geom.into_bytes(), raw);
    }

    #[test]
    fn bbox_reversed_x_fails_with_degenerate_bbox() {
        // maxx < minx: a clearly reversed x-axis should be rejected.
        assert!(matches!(
            BoundingBox::new(10.0, -5.0, -10.0, 5.0),
            Err(GeoError::DegenerateBbox { axis: "x", .. })
        ));
    }

    #[test]
    fn bbox_longitude_out_of_range_propagates() {
        assert!(matches!(
            BoundingBox::new(-200.0, -5.0, 10.0, 5.0),
            Err(GeoError::LongitudeOutOfRange { .. })
        ));
    }

    #[test]
    fn bbox_near_antimeridian_succeeds() {
        // f32 precision near 180.0: both 179.0 and 180.0 are representable
        // exactly as f32, so this box must construct without error.
        assert!(BoundingBox::new(179.0, -5.0, 180.0, 5.0).is_ok());
    }

    #[test]
    fn wkb_clone_produces_equal_value() {
        let geom = WkbGeometry::new(vec![0x01, 0x02, 0x03]).unwrap();
        let cloned = geom.clone();
        assert_eq!(geom, cloned);
    }

    #[test]
    fn bbox_edge_touching_intersects() {
        // The two boxes share only the vertical edge at x = 0.
        let a = BoundingBox::new(-10.0, -5.0, 0.0, 5.0).unwrap();
        let b = BoundingBox::new(0.0, -5.0, 10.0, 5.0).unwrap();
        assert!(a.intersects(&b));
        assert!(b.intersects(&a));
    }
}
