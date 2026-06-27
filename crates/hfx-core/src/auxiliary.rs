//! Manifest-declared auxiliary artifact types.

use std::collections::BTreeMap;
use std::str::FromStr;

/// Errors from parsing auxiliary declarations.
#[derive(Debug, thiserror::Error)]
pub enum AuxiliaryError {
    /// Returned when an auxiliary schema ID is empty.
    #[error("auxiliary schema id must not be empty")]
    EmptySchemaId,

    /// Returned when an HFX-owned schema ID is malformed or unsupported.
    #[error("malformed auxiliary schema id: {value:?}")]
    MalformedSchemaId {
        /// The invalid schema ID.
        value: String,
    },

    /// Returned when an artifact key is empty.
    #[error("auxiliary artifact key must not be empty")]
    EmptyArtifactKey,

    /// Returned when an artifact path is empty.
    #[error("auxiliary artifact path must not be empty for key {key:?}")]
    EmptyArtifactPath {
        /// The artifact key whose path is empty.
        key: String,
    },
}

/// Blessed auxiliary schemas with stable HFX validator support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlessedAuxSchema {
    /// Paired D8 flow-direction and flow-accumulation rasters.
    D8RasterV1,
    /// Optional snap features for outlet snapping.
    SnapV2,
}

impl std::fmt::Display for BlessedAuxSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlessedAuxSchema::D8RasterV1 => write!(f, "hfx.aux.d8_raster.v1"),
            BlessedAuxSchema::SnapV2 => write!(f, "hfx.aux.snap.v2"),
        }
    }
}

/// Parsed auxiliary schema ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AuxiliarySchemaId {
    /// Stable HFX-owned schema.
    Blessed(BlessedAuxSchema),
    /// Provisional HFX-owned schema.
    Provisional(String),
    /// Third-party schema, expected to use reverse-DNS naming.
    ThirdParty(String),
}

impl AuxiliarySchemaId {
    /// Parse and classify an auxiliary schema ID.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | `raw` is empty | [`AuxiliaryError::EmptySchemaId`] |
    /// | unsupported `hfx.aux.*` schema | [`AuxiliaryError::MalformedSchemaId`] |
    pub fn parse(raw: &str) -> Result<Self, AuxiliaryError> {
        if raw.is_empty() {
            return Err(AuxiliaryError::EmptySchemaId);
        }
        match raw {
            "hfx.aux.d8_raster.v1" => Ok(Self::Blessed(BlessedAuxSchema::D8RasterV1)),
            "hfx.aux.snap.v2" => Ok(Self::Blessed(BlessedAuxSchema::SnapV2)),
            value if value.starts_with("hfx.aux.") => Err(AuxiliaryError::MalformedSchemaId {
                value: value.to_owned(),
            }),
            value if value.starts_with("hfx.x.") => Ok(Self::Provisional(value.to_owned())),
            value => Ok(Self::ThirdParty(value.to_owned())),
        }
    }
}

impl std::fmt::Display for AuxiliarySchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuxiliarySchemaId::Blessed(schema) => write!(f, "{schema}"),
            AuxiliarySchemaId::Provisional(value) | AuxiliarySchemaId::ThirdParty(value) => {
                write!(f, "{value}")
            }
        }
    }
}

impl FromStr for AuxiliarySchemaId {
    type Err = AuxiliaryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// A manifest-declared auxiliary artifact block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryDecl {
    schema: AuxiliarySchemaId,
    artifacts: BTreeMap<String, String>,
}

impl AuxiliaryDecl {
    /// Construct an [`AuxiliaryDecl`] from a schema and artifact map.
    ///
    /// Metadata is intentionally not modeled in `hfx-core`; schema-specific
    /// metadata parsing belongs at the manifest deserialization boundary.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | any artifact key is empty | [`AuxiliaryError::EmptyArtifactKey`] |
    /// | any artifact path is empty | [`AuxiliaryError::EmptyArtifactPath`] |
    pub fn new(
        schema: AuxiliarySchemaId,
        artifacts: BTreeMap<String, String>,
    ) -> Result<Self, AuxiliaryError> {
        for (key, path) in &artifacts {
            if key.is_empty() {
                return Err(AuxiliaryError::EmptyArtifactKey);
            }
            if path.is_empty() {
                return Err(AuxiliaryError::EmptyArtifactPath { key: key.clone() });
            }
        }
        Ok(Self { schema, artifacts })
    }

    /// Return the parsed auxiliary schema ID.
    pub fn schema(&self) -> &AuxiliarySchemaId {
        &self.schema
    }

    /// Return the artifact key-to-path map.
    pub fn artifacts(&self) -> &BTreeMap<String, String> {
        &self.artifacts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_blessed_d8_raster() {
        assert_eq!(
            AuxiliarySchemaId::parse("hfx.aux.d8_raster.v1").unwrap(),
            AuxiliarySchemaId::Blessed(BlessedAuxSchema::D8RasterV1)
        );
    }

    #[test]
    fn parse_blessed_snap_v2() {
        assert_eq!(
            AuxiliarySchemaId::parse("hfx.aux.snap.v2").unwrap(),
            AuxiliarySchemaId::Blessed(BlessedAuxSchema::SnapV2)
        );
    }

    #[test]
    fn display_blessed_snap_v2() {
        assert_eq!(BlessedAuxSchema::SnapV2.to_string(), "hfx.aux.snap.v2");
    }

    #[test]
    fn parse_provisional_schema() {
        assert_eq!(
            AuxiliarySchemaId::parse("hfx.x.experimental.v1").unwrap(),
            AuxiliarySchemaId::Provisional("hfx.x.experimental.v1".to_string())
        );
    }

    #[test]
    fn parse_third_party_schema() {
        assert_eq!(
            AuxiliarySchemaId::parse("org.example.custom.v1").unwrap(),
            AuxiliarySchemaId::ThirdParty("org.example.custom.v1".to_string())
        );
    }

    #[test]
    fn parse_empty_schema_fails() {
        assert!(matches!(
            AuxiliarySchemaId::parse(""),
            Err(AuxiliaryError::EmptySchemaId)
        ));
    }

    #[test]
    fn parse_unknown_blessed_schema_fails() {
        assert!(matches!(
            AuxiliarySchemaId::parse("hfx.aux.other.v1"),
            Err(AuxiliaryError::MalformedSchemaId { .. })
        ));
    }

    #[test]
    fn auxiliary_decl_rejects_empty_path() {
        let mut artifacts = BTreeMap::new();
        artifacts.insert("flow_dir".to_string(), String::new());
        assert!(matches!(
            AuxiliaryDecl::new(
                AuxiliarySchemaId::Blessed(BlessedAuxSchema::D8RasterV1),
                artifacts
            ),
            Err(AuxiliaryError::EmptyArtifactPath { key }) if key == "flow_dir"
        ));
    }
}
