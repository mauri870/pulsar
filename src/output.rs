use clap::ValueEnum;
use std::fmt::Display;
use std::future::Future;
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
            OutputFormat::Json => format!("{}\n", serde_json::json!({ key: value.to_json() })),
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

/// Where the engine writes its results to.
pub trait Sink: Send + 'static {
    /// Writes one result.
    fn write(&mut self, key: &str, value: &Value) -> impl Future<Output = Result<()>> + Send;

    /// Writes out whatever is still buffered. Called once, after the last result.
    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Writes formatted results to stdout.
pub struct StdoutSink {
    writer: BufWriter<tokio::io::Stdout>,
    format: OutputFormat,
}

impl StdoutSink {
    pub fn new(format: OutputFormat) -> Self {
        StdoutSink {
            writer: BufWriter::new(tokio::io::stdout()),
            format,
        }
    }
}

impl Sink for StdoutSink {
    #[instrument(level = "trace", skip(self, value))]
    async fn write(&mut self, key: &str, value: &Value) -> Result<()> {
        self.writer
            .write_all(self.format.render(key, value).as_bytes())
            .await
            .map_err(Error::WriteOutput)?;

        // Use tokio's async version of flush
        tokio::io::stdout()
            .flush()
            .await
            .map_err(Error::WriteOutput)
    }

    async fn finish(&mut self) -> Result<()> {
        self.writer.flush().await.map_err(Error::WriteOutput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn object(entries: Vec<(&str, Value)>) -> Value {
        Value::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn plain(value: Value) -> String {
        OutputFormat::Plain.render("k", &value)
    }

    fn json(key: &str, value: Value) -> String {
        OutputFormat::Json.render(key, &value)
    }

    #[test]
    fn plain_renders_scalars() {
        assert_eq!(plain(Value::String("text".into())), "k: text\n");
        assert_eq!(plain(Value::Int(7)), "k: 7\n");
        assert_eq!(plain(Value::Float(1.5)), "k: 1.5\n");
        assert_eq!(plain(Value::Bool(true)), "k: true\n");
        assert_eq!(plain(Value::Null), "k: null\n");
    }

    #[test]
    fn plain_joins_arrays_with_commas_and_flattens_nested_ones() {
        let array = Value::Array(vec![Value::Int(1), Value::String("two".into())]);
        assert_eq!(plain(array), "k: 1,two\n");
        let nested = Value::Array(vec![Value::Array(vec![Value::Int(1), Value::Int(2)]), Value::Int(3)]);
        assert_eq!(plain(nested), "k: 1,2,3\n");
    }

    #[test]
    fn plain_renders_objects_as_key_value_pairs() {
        assert_eq!(plain(object(vec![("a", Value::Int(1))])), "k: a: 1\n");
        let nested = object(vec![("a", object(vec![("b", Value::Int(2))]))]);
        assert_eq!(plain(nested), "k: a: b: 2\n");
    }

    #[test]
    fn json_renders_one_object_per_line() {
        assert_eq!(json("k", Value::Int(7)), "{\"k\":7}\n");
        assert_eq!(json("k", Value::String("v".into())), "{\"k\":\"v\"}\n");
        assert_eq!(json("k", Value::Null), "{\"k\":null}\n");
        assert_eq!(json("k", Value::Bool(false)), "{\"k\":false}\n");
        assert_eq!(json("k", Value::Float(1.5)), "{\"k\":1.5}\n");
    }

    #[test]
    fn json_renders_nested_values() {
        let value = object(vec![(
            "a",
            Value::Array(vec![Value::Int(1), object(vec![("b", Value::Null)])]),
        )]);
        assert_eq!(json("k", value), "{\"k\":{\"a\":[1,{\"b\":null}]}}\n");
    }

    #[test]
    fn json_escapes_keys_and_strings() {
        assert_eq!(
            json("a\"b", Value::String("line\nbreak".into())),
            "{\"a\\\"b\":\"line\\nbreak\"}\n"
        );
    }

    #[test]
    fn json_renders_a_non_finite_float_as_null() {
        assert_eq!(json("k", Value::Float(f64::NAN)), "{\"k\":null}\n");
        assert_eq!(json("k", Value::Float(f64::INFINITY)), "{\"k\":null}\n");
    }
}
