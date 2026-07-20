//! Raster-related domain types.

use std::str::FromStr;

/// An uppercase EPSG authority code with a positive integer identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EpsgCode(String);

impl EpsgCode {
    /// Return the canonical EPSG authority string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EpsgCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error returned when parsing a malformed EPSG authority code.
#[derive(Debug, thiserror::Error)]
#[error("malformed EPSG authority code: {value:?}, expected \"EPSG:<positive integer>\"")]
pub struct EpsgCodeError {
    /// The malformed authority string.
    pub value: String,
}

impl FromStr for EpsgCode {
    type Err = EpsgCodeError;

    /// Parse an uppercase positive EPSG authority code.
    ///
    /// # Errors
    ///
    /// | Condition | Error type |
    /// |---|---|
    /// | `s` does not match `^EPSG:[1-9][0-9]*$` | [`EpsgCodeError`] |
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let suffix = s.strip_prefix("EPSG:");
        if !suffix.is_some_and(|digits| {
            digits
                .as_bytes()
                .first()
                .is_some_and(|first| matches!(first, b'1'..=b'9'))
                && digits.as_bytes().iter().all(u8::is_ascii_digit)
        }) {
            return Err(EpsgCodeError {
                value: s.to_owned(),
            });
        }

        Ok(Self(s.to_owned()))
    }
}

/// D8 flow direction encoding convention.
///
/// Declares which encoding convention a `flow_dir.tif` raster uses.
/// The engine normalizes to its internal convention at read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlowDirEncoding {
    /// ESRI convention: powers of 2 (1, 2, 4, 8, 16, 32, 64, 128).
    Esri,
    /// TauDEM convention: 1-8, east origin, counter-clockwise.
    Taudem,
    /// GRASS convention: 1-8, northeast origin, counter-clockwise.
    Grass,
}

impl std::fmt::Display for FlowDirEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlowDirEncoding::Esri => write!(f, "esri"),
            FlowDirEncoding::Taudem => write!(f, "taudem"),
            FlowDirEncoding::Grass => write!(f, "grass"),
        }
    }
}

/// Error returned when parsing an unknown flow direction encoding string.
#[derive(Debug, thiserror::Error)]
#[error("unknown flow direction encoding: {value:?}, expected \"esri\", \"taudem\", or \"grass\"")]
pub struct FlowDirEncodingError {
    /// The unrecognized string.
    pub value: String,
}

impl std::str::FromStr for FlowDirEncoding {
    type Err = FlowDirEncodingError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "esri" => Ok(FlowDirEncoding::Esri),
            "taudem" => Ok(FlowDirEncoding::Taudem),
            "grass" => Ok(FlowDirEncoding::Grass),
            _ => Err(FlowDirEncodingError {
                value: s.to_owned(),
            }),
        }
    }
}

/// Units represented by a D8 flow-accumulation raster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlowAccumulationUnits {
    /// Upstream cell count.
    Cells,
    /// Upstream drainage area in square kilometers.
    Km2,
}

impl std::fmt::Display for FlowAccumulationUnits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlowAccumulationUnits::Cells => write!(f, "cells"),
            FlowAccumulationUnits::Km2 => write!(f, "km2"),
        }
    }
}

/// Error returned when parsing unknown flow-accumulation units.
#[derive(Debug, thiserror::Error)]
#[error("unknown flow accumulation units: {value:?}, expected \"cells\" or \"km2\"")]
pub struct FlowAccumulationUnitsError {
    /// The unrecognized string.
    pub value: String,
}

impl FromStr for FlowAccumulationUnits {
    type Err = FlowAccumulationUnitsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cells" => Ok(FlowAccumulationUnits::Cells),
            "km2" => Ok(FlowAccumulationUnits::Km2),
            _ => Err(FlowAccumulationUnitsError {
                value: s.to_owned(),
            }),
        }
    }
}

/// Typed metadata declared by a D8 raster v2 auxiliary entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct D8RasterMetadataV2 {
    crs: EpsgCode,
    flow_dir_encoding: FlowDirEncoding,
    flow_acc_units: FlowAccumulationUnits,
}

impl D8RasterMetadataV2 {
    /// Parse required raw v2 metadata fields into domain types.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | a required field is absent or non-string | [`D8RasterMetadataV2Error::MissingField`] |
    /// | `crs` is malformed | [`D8RasterMetadataV2Error::InvalidCrs`] |
    /// | `flow_dir_encoding` is unsupported | [`D8RasterMetadataV2Error::InvalidFlowDirEncoding`] |
    /// | `flow_acc_units` is unsupported | [`D8RasterMetadataV2Error::InvalidFlowAccumulationUnits`] |
    pub fn parse(
        crs: Option<&str>,
        flow_dir_encoding: Option<&str>,
        flow_acc_units: Option<&str>,
    ) -> Result<Self, D8RasterMetadataV2Error> {
        let crs = crs
            .ok_or(D8RasterMetadataV2Error::MissingField { field: "crs" })?
            .parse()
            .map_err(|source| D8RasterMetadataV2Error::InvalidCrs { source })?;
        let flow_dir_encoding = flow_dir_encoding
            .ok_or(D8RasterMetadataV2Error::MissingField {
                field: "flow_dir_encoding",
            })?
            .parse()
            .map_err(|source| D8RasterMetadataV2Error::InvalidFlowDirEncoding { source })?;
        let flow_acc_units = flow_acc_units
            .ok_or(D8RasterMetadataV2Error::MissingField {
                field: "flow_acc_units",
            })?
            .parse()
            .map_err(|source| D8RasterMetadataV2Error::InvalidFlowAccumulationUnits { source })?;

        Ok(Self {
            crs,
            flow_dir_encoding,
            flow_acc_units,
        })
    }

    /// Return the declared raster CRS.
    pub fn crs(&self) -> &EpsgCode {
        &self.crs
    }

    /// Return the declared flow-direction encoding.
    pub fn flow_dir_encoding(&self) -> FlowDirEncoding {
        self.flow_dir_encoding
    }

    /// Return the declared flow-accumulation units.
    pub fn flow_acc_units(&self) -> FlowAccumulationUnits {
        self.flow_acc_units
    }
}

/// Errors from parsing required D8 raster v2 metadata.
#[derive(Debug, thiserror::Error)]
pub enum D8RasterMetadataV2Error {
    /// Returned when required v2 metadata is absent or is not a string.
    #[error("missing or non-string D8 raster v2 metadata field {field:?}")]
    MissingField {
        /// The required metadata field.
        field: &'static str,
    },

    /// Returned when `crs` is not an uppercase positive EPSG authority code.
    #[error("invalid D8 raster v2 crs")]
    InvalidCrs {
        /// The EPSG parsing failure.
        #[source]
        source: EpsgCodeError,
    },

    /// Returned when `flow_dir_encoding` is outside the v2 encoding enum.
    #[error("invalid D8 raster v2 flow_dir_encoding")]
    InvalidFlowDirEncoding {
        /// The encoding parsing failure.
        #[source]
        source: FlowDirEncodingError,
    },

    /// Returned when `flow_acc_units` is outside the v2 units enum.
    #[error("invalid D8 raster v2 flow_acc_units")]
    InvalidFlowAccumulationUnits {
        /// The units parsing failure.
        #[source]
        source: FlowAccumulationUnitsError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_dir_encoding_variants_are_not_equal() {
        assert_ne!(FlowDirEncoding::Esri, FlowDirEncoding::Taudem);
        assert_ne!(FlowDirEncoding::Grass, FlowDirEncoding::Esri);
        assert_ne!(FlowDirEncoding::Grass, FlowDirEncoding::Taudem);
    }

    #[test]
    fn flow_dir_encoding_clone_and_copy() {
        let original = FlowDirEncoding::Esri;
        let cloned = original;
        // Copy: bind by value into a second variable.
        let copied = original;
        assert_eq!(original, cloned);
        assert_eq!(original, copied);
    }

    #[test]
    fn flow_dir_encoding_usable_as_hash_map_key() {
        use std::collections::HashMap;
        let mut map: HashMap<FlowDirEncoding, &str> = HashMap::new();
        map.insert(FlowDirEncoding::Esri, "esri");
        map.insert(FlowDirEncoding::Taudem, "taudem");
        map.insert(FlowDirEncoding::Grass, "grass");
        assert_eq!(map[&FlowDirEncoding::Esri], "esri");
        assert_eq!(map[&FlowDirEncoding::Taudem], "taudem");
        assert_eq!(map[&FlowDirEncoding::Grass], "grass");
    }

    #[test]
    fn flow_dir_encoding_display() {
        assert_eq!(FlowDirEncoding::Esri.to_string(), "esri");
        assert_eq!(FlowDirEncoding::Taudem.to_string(), "taudem");
        assert_eq!(FlowDirEncoding::Grass.to_string(), "grass");
    }

    #[test]
    fn flow_dir_encoding_fromstr_valid() {
        assert_eq!(
            "esri".parse::<FlowDirEncoding>().unwrap(),
            FlowDirEncoding::Esri
        );
        assert_eq!(
            "taudem".parse::<FlowDirEncoding>().unwrap(),
            FlowDirEncoding::Taudem
        );
        assert_eq!(
            "grass".parse::<FlowDirEncoding>().unwrap(),
            FlowDirEncoding::Grass
        );
    }

    #[test]
    fn flow_dir_encoding_fromstr_invalid() {
        assert!("invalid".parse::<FlowDirEncoding>().is_err());
        assert!("ESRI".parse::<FlowDirEncoding>().is_err());
    }

    #[test]
    fn epsg_code_parses_schema_grammar_without_integer_width_limit() {
        for value in [
            "EPSG:4326",
            "EPSG:8857",
            "EPSG:123456789012345678901234567890",
        ] {
            let code = value.parse::<EpsgCode>().unwrap();
            assert_eq!(code.as_str(), value);
            assert_eq!(code.to_string(), value);
        }
    }

    #[test]
    fn epsg_code_rejects_malformed_values() {
        for value in ["epsg:4326", "EPSG:0", "EPSG:04326", "EPSG:", "EPSG:abc"] {
            assert!(value.parse::<EpsgCode>().is_err());
        }
        assert!(matches!(
            "4326".parse::<EpsgCode>(),
            Err(EpsgCodeError { value }) if value == "4326"
        ));
    }

    #[test]
    fn flow_accumulation_units_parse_and_display() {
        assert_eq!(
            "cells".parse::<FlowAccumulationUnits>().unwrap(),
            FlowAccumulationUnits::Cells
        );
        assert_eq!(
            "km2".parse::<FlowAccumulationUnits>().unwrap(),
            FlowAccumulationUnits::Km2
        );
        assert_eq!(FlowAccumulationUnits::Cells.to_string(), "cells");
        assert_eq!(FlowAccumulationUnits::Km2.to_string(), "km2");
        assert!(
            "square_kilometers"
                .parse::<FlowAccumulationUnits>()
                .is_err()
        );
    }

    #[test]
    fn d8_raster_metadata_v2_parses_typed_fields() {
        let metadata =
            D8RasterMetadataV2::parse(Some("EPSG:8857"), Some("grass"), Some("km2")).unwrap();
        assert_eq!(metadata.crs().as_str(), "EPSG:8857");
        assert_eq!(metadata.flow_dir_encoding(), FlowDirEncoding::Grass);
        assert_eq!(metadata.flow_acc_units(), FlowAccumulationUnits::Km2);
    }

    #[test]
    fn d8_raster_metadata_v2_reports_each_missing_field() {
        for (result, expected) in [
            (
                D8RasterMetadataV2::parse(None, Some("esri"), Some("cells")),
                "crs",
            ),
            (
                D8RasterMetadataV2::parse(Some("EPSG:4326"), None, Some("cells")),
                "flow_dir_encoding",
            ),
            (
                D8RasterMetadataV2::parse(Some("EPSG:4326"), Some("esri"), None),
                "flow_acc_units",
            ),
        ] {
            assert!(matches!(
                result,
                Err(D8RasterMetadataV2Error::MissingField { field }) if field == expected
            ));
        }
    }

    #[test]
    fn d8_raster_metadata_v2_reports_malformed_fields() {
        assert!(matches!(
            D8RasterMetadataV2::parse(Some("epsg:4326"), Some("esri"), Some("cells")),
            Err(D8RasterMetadataV2Error::InvalidCrs { .. })
        ));
        assert!(matches!(
            D8RasterMetadataV2::parse(Some("EPSG:4326"), Some("invalid"), Some("cells")),
            Err(D8RasterMetadataV2Error::InvalidFlowDirEncoding { .. })
        ));
        assert!(matches!(
            D8RasterMetadataV2::parse(Some("EPSG:4326"), Some("esri"), Some("invalid")),
            Err(D8RasterMetadataV2Error::InvalidFlowAccumulationUnits { .. })
        ));
    }
}
