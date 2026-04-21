//! Validation report aggregation and display.

use serde::Serialize;

use crate::diagnostic::{Diagnostic, Location, Severity};

/// Aggregated validation result for a complete dataset.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    diagnostics: Vec<Diagnostic>,
}

impl ValidationReport {
    pub fn new(diagnostics: Vec<Diagnostic>) -> Self {
        Self { diagnostics }
    }

    /// True if no Error-severity diagnostics exist.
    pub fn is_valid(&self) -> bool {
        !self
            .diagnostics
            .iter()
            .any(|d| d.severity == Severity::Error)
    }

    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    pub fn error_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .count()
    }

    pub fn warning_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Warning)
            .count()
    }

    pub fn info_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Info)
            .count()
    }

    /// Promote all warnings to errors (for --strict mode).
    pub fn promote_warnings(&mut self) {
        for d in &mut self.diagnostics {
            if d.severity == Severity::Warning {
                d.severity = Severity::Error;
            }
        }
    }

    /// Render as human-readable text.
    pub fn display_text(&self) -> String {
        let mut out = String::new();
        for d in &self.diagnostics {
            out.push_str(&format!("{}\n", d));
        }
        out.push_str(&format!(
            "\n{} error(s), {} warning(s), {} info(s)\n",
            self.error_count(),
            self.warning_count(),
            self.info_count(),
        ));
        if self.is_valid() {
            out.push_str("Result: VALID\n");
        } else {
            out.push_str("Result: INVALID\n");
        }
        out
    }

    /// Render as JSON.
    pub fn display_json(&self) -> String {
        let summary = JsonReport {
            passed: self.is_valid(),
            error_count: self.error_count(),
            warning_count: self.warning_count(),
            info_count: self.info_count(),
            diagnostics: self.diagnostics.iter().map(JsonDiagnostic::from).collect(),
        };
        serde_json::to_string_pretty(&summary)
            .unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e))
    }
}

#[derive(Serialize)]
struct JsonReport {
    passed: bool,
    error_count: usize,
    warning_count: usize,
    info_count: usize,
    diagnostics: Vec<JsonDiagnostic>,
}

#[derive(Serialize)]
struct JsonDiagnostic {
    check_id: String,
    severity: String,
    category: String,
    artifact: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    row: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column: Option<String>,
}

impl From<&Diagnostic> for JsonDiagnostic {
    fn from(d: &Diagnostic) -> Self {
        let (row, field, column) = match &d.location {
            Location::None => (None, None, None),
            Location::Row { index } => (Some(*index), None, None),
            Location::Field { name } => (None, Some(name.clone()), None),
            Location::Column { name } => (None, None, Some(name.clone())),
        };
        Self {
            check_id: d.check_id.to_string(),
            severity: d.severity.to_string(),
            category: d.category.to_string(),
            artifact: d.artifact.to_string(),
            message: d.message.clone(),
            row,
            field,
            column,
        }
    }
}
