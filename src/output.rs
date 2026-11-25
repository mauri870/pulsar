use clap::ValueEnum;
use std::fmt::Display;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::instrument;

use crate::error::{Error, Result};
use crate::value::Value;

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

impl OutputFormat {
    /// Renders one result as a line of output, newline included.
    pub fn render(&self, key: &str, value: &Value) -> String {
        match self {
            OutputFormat::Plain => format!("{}: {}\n", key, plain_text(value)),
            OutputFormat::Json => format!("{}\n", serde_json::json!({ key: json_value(value) })),
        }
    }
}

/// The plain text form of a value: strings as they are, arrays joined with
/// commas and objects as `key: value` pairs.
fn plain_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(arr) => arr
            .iter()
            .map(plain_text)
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(obj) => obj
            .iter()
            .map(|(k, v)| format!("{}: {}", k, plain_text(v)))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

fn json_value(value: &Value) -> serde_json::Value {
    match value {
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
        Value::Float(n) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(json_value).collect()),
        Value::Object(obj) => serde_json::Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_value(v)))
                .collect(),
        ),
    }
}

/// Format and print a single result
#[instrument(level = "trace", skip(writer))]
pub async fn write_result(
    key: &str,
    result: &Value,
    output_format: &OutputFormat,
    writer: &mut BufWriter<tokio::io::Stdout>,
) -> Result<()> {
    writer
        .write_all(output_format.render(key, result).as_bytes())
        .await
        .map_err(Error::WriteOutput)?;

    // Use tokio's async version of flush
    tokio::io::stdout()
        .flush()
        .await
        .map_err(Error::WriteOutput)
}
