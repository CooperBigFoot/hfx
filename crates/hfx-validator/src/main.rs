use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing::info;

/// Output format for validation results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum OutputFormat {
    /// Human-readable text output.
    Text,
    /// Machine-readable JSON output for CI pipelines.
    Json,
}

/// HFX dataset validator — checks an HFX directory against the spec.
#[derive(Debug, Parser)]
#[command(name = "hfx", version, about)]
struct Cli {
    /// Path to the HFX dataset directory to validate.
    dataset_path: PathBuf,

    /// Output format.
    #[arg(long, default_value = "text", value_enum)]
    format: OutputFormat,

    /// Treat warnings as errors.
    #[arg(long)]
    strict: bool,

    /// Skip raster validation (flow_dir.tif / flow_acc.tif).
    #[arg(long)]
    skip_rasters: bool,

    /// Geometry spot-check sample percentage (0.0–100.0).
    #[arg(long, default_value = "1.0")]
    sample_pct: f64,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if cli.format != OutputFormat::Json {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .init();
    }

    info!(path = %cli.dataset_path.display(), "validating HFX dataset");

    let report = hfx_validator::validate(
        &cli.dataset_path,
        cli.strict,
        cli.skip_rasters,
        cli.sample_pct,
    );

    let output = match cli.format {
        OutputFormat::Text => report.display_text(),
        OutputFormat::Json => report.display_json(),
    };
    print!("{output}");

    if report.is_valid() {
        ExitCode::from(0)
    } else {
        ExitCode::from(1)
    }
}
