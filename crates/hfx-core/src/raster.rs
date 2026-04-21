//! Raster-related domain types.

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
}

impl std::fmt::Display for FlowDirEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlowDirEncoding::Esri => write!(f, "esri"),
            FlowDirEncoding::Taudem => write!(f, "taudem"),
        }
    }
}

/// Error returned when parsing an unknown flow direction encoding string.
#[derive(Debug, thiserror::Error)]
#[error("unknown flow direction encoding: {value:?}, expected \"esri\" or \"taudem\"")]
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
            _ => Err(FlowDirEncodingError {
                value: s.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_dir_encoding_variants_are_not_equal() {
        assert_ne!(FlowDirEncoding::Esri, FlowDirEncoding::Taudem);
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
        assert_eq!(map[&FlowDirEncoding::Esri], "esri");
        assert_eq!(map[&FlowDirEncoding::Taudem], "taudem");
    }

    #[test]
    fn flow_dir_encoding_display() {
        assert_eq!(FlowDirEncoding::Esri.to_string(), "esri");
        assert_eq!(FlowDirEncoding::Taudem.to_string(), "taudem");
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
    }

    #[test]
    fn flow_dir_encoding_fromstr_invalid() {
        assert!("invalid".parse::<FlowDirEncoding>().is_err());
        assert!("ESRI".parse::<FlowDirEncoding>().is_err());
    }
}
