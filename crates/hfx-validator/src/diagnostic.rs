//! Validation diagnostic types.

use std::fmt;

/// Severity of a validation finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Severity {
    /// Dataset is non-conformant.
    Error,
    /// Dataset is technically conformant but has a quality issue.
    Warning,
    /// Informational note (e.g., "check skipped because file absent").
    Info,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => write!(f, "ERROR"),
            Severity::Warning => write!(f, "WARN"),
            Severity::Info => write!(f, "INFO"),
        }
    }
}

/// Logical category grouping related checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    FilePresence,
    Manifest,
    Schema,
    IdConstraint,
    ReferentialIntegrity,
    GraphInvariant,
    ValueConsistency,
    Raster,
    Geometry,
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Category::FilePresence => write!(f, "file-presence"),
            Category::Manifest => write!(f, "manifest"),
            Category::Schema => write!(f, "schema"),
            Category::IdConstraint => write!(f, "id-constraint"),
            Category::ReferentialIntegrity => write!(f, "referential-integrity"),
            Category::GraphInvariant => write!(f, "graph-invariant"),
            Category::ValueConsistency => write!(f, "value-consistency"),
            Category::Raster => write!(f, "raster"),
            Category::Geometry => write!(f, "geometry"),
        }
    }
}

/// Which artifact a diagnostic pertains to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Artifact {
    Manifest,
    Catchments,
    Graph,
    Snap,
    FlowDir,
    FlowAcc,
    /// Cross-file check spanning multiple artifacts.
    CrossFile,
}

impl fmt::Display for Artifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Artifact::Manifest => write!(f, "manifest.json"),
            Artifact::Catchments => write!(f, "catchments.parquet"),
            Artifact::Graph => write!(f, "graph.arrow"),
            Artifact::Snap => write!(f, "snap.parquet"),
            Artifact::FlowDir => write!(f, "flow_dir.tif"),
            Artifact::FlowAcc => write!(f, "flow_acc.tif"),
            Artifact::CrossFile => write!(f, "cross-file"),
        }
    }
}

/// Optional location context for a diagnostic.
#[derive(Debug, Clone, PartialEq)]
pub enum Location {
    /// A specific row index in a tabular artifact.
    Row { index: usize },
    /// A named field in the manifest.
    Field { name: String },
    /// A specific column in a tabular artifact.
    Column { name: String },
    /// No specific location.
    None,
}

/// A single validation finding.
#[derive(Debug, Clone, PartialEq)]
pub struct Diagnostic {
    /// Machine-readable check identifier (e.g. "manifest.fabric_name_format").
    pub check_id: &'static str,
    /// Error, Warning, or Info.
    pub severity: Severity,
    /// Logical grouping.
    pub category: Category,
    /// Which file this pertains to.
    pub artifact: Artifact,
    /// Human-readable description.
    pub message: String,
    /// Optional row/field/column location.
    pub location: Location,
}

impl Diagnostic {
    /// Create an error diagnostic.
    pub fn error(
        check_id: &'static str,
        category: Category,
        artifact: Artifact,
        message: impl Into<String>,
    ) -> Self {
        Self {
            check_id,
            severity: Severity::Error,
            category,
            artifact,
            message: message.into(),
            location: Location::None,
        }
    }

    /// Create a warning diagnostic.
    pub fn warning(
        check_id: &'static str,
        category: Category,
        artifact: Artifact,
        message: impl Into<String>,
    ) -> Self {
        Self {
            check_id,
            severity: Severity::Warning,
            category,
            artifact,
            message: message.into(),
            location: Location::None,
        }
    }

    /// Create an info diagnostic.
    pub fn info(
        check_id: &'static str,
        category: Category,
        artifact: Artifact,
        message: impl Into<String>,
    ) -> Self {
        Self {
            check_id,
            severity: Severity::Info,
            category,
            artifact,
            message: message.into(),
            location: Location::None,
        }
    }

    /// Attach a location to this diagnostic.
    pub fn at(mut self, location: Location) -> Self {
        self.location = location;
        self
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.location {
            Location::None => write!(
                f,
                "[{}] {} ({}): {}",
                self.severity, self.artifact, self.check_id, self.message
            ),
            Location::Row { index } => write!(
                f,
                "[{}] {} row {} ({}): {}",
                self.severity, self.artifact, index, self.check_id, self.message
            ),
            Location::Field { name } => write!(
                f,
                "[{}] {} field {:?} ({}): {}",
                self.severity, self.artifact, name, self.check_id, self.message
            ),
            Location::Column { name } => write!(
                f,
                "[{}] {} column {:?} ({}): {}",
                self.severity, self.artifact, name, self.check_id, self.message
            ),
        }
    }
}
