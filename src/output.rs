use clap::ValueEnum;
use std::fmt::Display;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::instrument;

use crate::error::{Error, Result};

#[derive(Debug, Clone, ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Plain,
    Json,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Plain => write!(f, "plain"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

/// Format and print a single result
#[instrument(level = "trace", skip(writer))]
pub async fn write_result(
    key: &str,
    result: &crate::js::Value,
    output_format: &OutputFormat,
    writer: &mut BufWriter<tokio::io::Stdout>,
) -> Result<()> {
    match output_format {
        OutputFormat::Plain => {
            writer
                .write_all(format!("{}: {}\n", key, result.to_string()).as_bytes())
                .await
                .map_err(Error::WriteOutput)?;
        }
        OutputFormat::Json => {
            let val = serde_json::Value::from(result);
            writer
                .write_all(format!("{}\n", serde_json::json!({ key: val })).as_bytes())
                .await
                .map_err(Error::WriteOutput)?;
        }
    }

    // Use tokio's async version of flush
    tokio::io::stdout()
        .flush()
        .await
        .map_err(Error::WriteOutput)
}
