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
        let cloned = original.clone();
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
}
