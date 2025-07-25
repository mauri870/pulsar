mod runtime;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing::{debug, instrument};
use runtime::Runtime;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::signal;
use tokio::sync::mpsc;

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");
const CHUNK_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum PulsarError {
    #[error("no files or directories to watch")]
    NoFilesToWatch,
}

#[derive(Debug, Parser)]
#[command(name = "pulsar")]
#[command(about = "A simple map-reduce engine for parallel processing")]
#[command(author, version)]
pub struct Cli {
    /// Input file to read input data from.
    #[arg(short = 'f', default_value = "-")]
    input_file: String,

    /// Output format for the results.
    #[arg(long = "output", default_value_t = OutputFormat::Plain)]
    output_format: OutputFormat,

    /// JavaScript file containing map and reduce functions. If not provided, defaults to a word count script.
    #[arg(short = 's', long = "script")]
    script_file: Option<String>,

    /// Runtime to use for processing.
    #[arg(long = "runtime", default_value_t = RuntimeType::WC)]
    runtime: RuntimeType,
}

#[derive(Debug, Clone, ValueEnum, Default)]
enum RuntimeType {
    #[default]
    WC,
    JS,
}

impl Display for RuntimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeType::WC => write!(f, "wc"),
            RuntimeType::JS => write!(f, "js"),
        }
    }
}

#[derive(Debug, Clone, ValueEnum, Default)]
enum OutputFormat {
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

pub struct Pulsar<R: AsyncBufReadExt + Unpin> {
    runtime: RuntimeType,
    reader: R,
    script: String,
    output_format: OutputFormat,
}

impl Pulsar<BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
    /// Create a new Pulsar instance from CLI arguments
    #[instrument(level = "trace")]
    pub async fn from_cli(cli: Cli) -> Result<Self> {
        let reader: BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>> =
            if cli.input_file == "-" {
                // Read from stdin
                let stdin = tokio::io::stdin();
                BufReader::new(Box::new(stdin))
            } else {
                // Read from file
                let file = tokio::fs::File::open(&cli.input_file).await.map_err(|e| {
                    anyhow::anyhow!("Failed to open file {}: {}", cli.input_file, e)
                })?;
                BufReader::new(Box::new(file))
            };

        let script = if let Some(script_file) = cli.script_file {
            // Read custom script from file
            tokio::fs::read_to_string(&script_file)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read script file {}: {}", script_file, e))?
        } else {
            // Use default word count script
            DEFAULT_SCRIPT.into()
        };
        Ok(Pulsar {
            runtime: cli.runtime,
            reader,
            script: script.clone(),
            output_format: cli.output_format,
        })
    }

    /// Run the application with streaming and optimized processing
    #[instrument(level = "trace")]
    pub async fn run(self) -> Result<()> {
        // Create a single shared runtime for all phases
        let runtime: Arc<dyn runtime::Runtime + Send + Sync> = match self.runtime {
            RuntimeType::WC => Arc::new(runtime::WordCountRuntime::new(self.script).await?),
            RuntimeType::JS => Arc::new(runtime::JavaScriptRuntime::new(self.script).await?),
        };
        let shared_runtime = runtime.clone();

        let has_sort_function = shared_runtime.has_sort().await;

        // Channel for streaming results from map phase to reduce phase
        let (map_tx, map_rx) = mpsc::channel::<runtime::KeyValue>(1000);
        let (reduce_tx, reduce_rx) = mpsc::channel::<(String, runtime::Value)>(1000);

        // Spawn map phase processor
        let map_runtime = shared_runtime.clone();
        let map_handle = tokio::spawn(async move {
            Self::process_map_phase(self.reader, map_runtime, map_tx).await
        });

        // Spawn reduce phase processor
        let reduce_runtime = shared_runtime.clone();
        let reduce_handle = tokio::spawn(async move {
            Self::process_reduce_phase(map_rx, reduce_runtime, reduce_tx, has_sort_function).await
        });

        // Spawn output handler
        let output_format = self.output_format.clone();
        let output_handle =
            tokio::spawn(async move { Self::handle_output(reduce_rx, output_format).await });

        // Wait for all phases to complete
        let (map_result, reduce_result, output_result) =
            tokio::try_join!(map_handle, reduce_handle, output_handle)?;

        map_result?;
        reduce_result?;
        output_result?;

        Ok(())
    }

    /// Process map phase with streaming input and limited concurrency
    #[instrument(level = "trace", skip(reader, runtime, map_tx))]
    async fn process_map_phase<R: AsyncBufReadExt + Unpin>(
        mut reader: R,
        runtime: Arc<dyn runtime::Runtime>,
        map_tx: mpsc::Sender<runtime::KeyValue>,
    ) -> Result<()> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(num_cpus::get()));
        let mut tasks = Vec::new();
        let mut lines = Vec::new();
        let mut line_buf = String::new();
        while let Ok(n) = reader.read_line(&mut line_buf).await {
            if n == 0 { break; }
            let line = line_buf.trim();
            if !line.is_empty() {
                lines.push(line.to_string());
            }
            line_buf.clear();
        }

        for (chunk_idx, chunk) in lines.chunks(CHUNK_SIZE).enumerate() {
            let chunk = chunk.to_vec();
            let runtime_clone = runtime.clone();
            let map_tx_clone = map_tx.clone();
            let semaphore_clone = semaphore.clone();
            let context = runtime::RuntimeContext::new(format!("chunk-{}", chunk_idx));
            let task = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await.unwrap();
                for line in chunk {
                    match runtime_clone.map(&line, &context).await {
                        Ok(mapped) => {
                            for kv in mapped {
                                map_tx_clone.send(kv).await.unwrap();
                            }
                        },
                        Err(e) => eprintln!("Map error for line '{}': {}", line, e),
                    }
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    /// Process reduce phase with incremental grouping
    #[instrument(level = "trace", skip(map_rx, runtime, reduce_tx))]
    async fn process_reduce_phase(
        mut map_rx: mpsc::Receiver<runtime::KeyValue>,
        runtime: Arc<dyn runtime::Runtime>,
        reduce_tx: mpsc::Sender<(String, runtime::Value)>,
        has_sort_function: bool,
    ) -> Result<()> {
        let mut groups_map: HashMap<String, Vec<runtime::Value>> = HashMap::new();

        // Collect all mapped results into groups
        while let Some(kv) = map_rx.recv().await {
            groups_map
                .entry(kv.key)
                .or_insert_with(Vec::new)
                .push(kv.value);
        }

        if has_sort_function {
            // Need to collect all results for sorting
            Self::process_with_sorting(groups_map, runtime, reduce_tx).await
        } else {
            // Can stream results immediately
            Self::process_without_sorting(groups_map, runtime, reduce_tx).await
        }
    }

    /// Process reduce phase with sorting (collect all results first)
    #[instrument(level = "trace", skip(groups_map, runtime, reduce_tx))]
    async fn process_with_sorting(
        groups_map: HashMap<String, Vec<runtime::Value>>,
        runtime: Arc<dyn runtime::Runtime>,
        reduce_tx: mpsc::Sender<(String, runtime::Value)>,
    ) -> Result<()> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(num_cpus::get()));
        let mut reduce_tasks = Vec::new();

        for (key, values) in groups_map {
            let runtime_clone = runtime.clone();
            let semaphore_clone = semaphore.clone();
            let key_clone = key.clone();
            let values_clone = values.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await.unwrap();
                let context = runtime::RuntimeContext::new(format!("reduce-{}", key_clone));
                match runtime_clone
                    .reduce(runtime::Value::String(key_clone.clone()), values_clone, &context)
                    .await
                {
                    Ok(reduced_value) => Some(runtime::KeyValue {
                        key: key_clone,
                        value: reduced_value,
                    }),
                    Err(e) => {
                        eprintln!("Reduce error for key '{}': {}", key_clone, e);
                        None
                    }
                }
            });
            reduce_tasks.push(task);
        }

        // Collect all reduce results
        let mut reduced_results = Vec::new();
        for task in reduce_tasks {
            match task.await {
                Ok(Some(kv)) => reduced_results.push(kv),
                Ok(None) => {} // Error case, already logged
                Err(e) => eprintln!("Reduce task error: {}", e),
            }
        }

        // Apply sorting
        let sort_context = runtime::RuntimeContext::new("sort".to_string());
        match runtime.sort(reduced_results, &sort_context).await {
            Ok(sorted_pairs) => {
                for kv in sorted_pairs {
                    let _ = reduce_tx.send((kv.key, kv.value)).await;
                }
            }
            Err(e) => eprintln!("Sort error: {}", e),
        }

        Ok(())
    }

    /// Process reduce phase without sorting (stream results immediately)
    #[instrument(level = "trace", skip(groups_map, runtime, reduce_tx))]
    async fn process_without_sorting(
        groups_map: HashMap<String, Vec<runtime::Value>>,
        runtime: Arc<dyn runtime::Runtime>,
        reduce_tx: mpsc::Sender<(String, runtime::Value)>,
    ) -> Result<()> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(num_cpus::get()));
        let mut reduce_tasks = Vec::new();

        for (key, values) in groups_map {
            let reduce_tx_clone = reduce_tx.clone();
            let runtime_clone = runtime.clone();
            let semaphore_clone = semaphore.clone();
            let key_clone = key.clone();
            let values_clone = values.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await.unwrap();
                let context = runtime::RuntimeContext::new(format!("reduce-{}", key_clone));
                match runtime_clone
                    .reduce(runtime::Value::String(key_clone.clone()), values_clone, &context)
                    .await
                {
                    Ok(reduced_value) => {
                        let _ = reduce_tx_clone.send((key_clone, reduced_value)).await;
                    }
                    Err(e) => eprintln!("Reduce error for key '{}': {}", key_clone, e),
                }
            });
            reduce_tasks.push(task);
        }

        // Wait for all reduce tasks
        for task in reduce_tasks {
            let _ = task.await;
        }

        Ok(())
    }

    /// Handle output formatting and display
    #[instrument(level = "trace", skip(reduce_rx))]
    async fn handle_output(
        mut reduce_rx: mpsc::Receiver<(String, runtime::Value)>,
        output_format: OutputFormat,
    ) -> Result<()> {
        loop {
            tokio::select! {
                result = reduce_rx.recv() => {
                    match result {
                        Some((key, result)) => {
                            Self::format_and_print_result(&key, &result, &output_format).await;
                        }
                        None => break, // Channel closed
                    }
                }
                _ = signal::ctrl_c() => {
                    eprintln!("Received Ctrl+C, shutting down gracefully...");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Format and print a single result
    async fn format_and_print_result(
        key: &str,
        result: &runtime::Value,
        output_format: &OutputFormat,
    ) {
        match output_format {
            OutputFormat::Plain => {
                let display_value = Self::value_to_string(result);
                println!("{}: {}", key, display_value);
            }
            OutputFormat::Json => {
                let json_value = Self::value_to_json(result);
                println!(
                    "{{\"{}\": {}}}",
                    key,
                    serde_json::to_string(&json_value).unwrap()
                );
            }
        }

        // Use tokio's async version of flush
        let _ = tokio::io::stdout().flush().await;
    }

    /// Convert runtime::Value to string representation
    fn value_to_string(value: &runtime::Value) -> String {
        match value {
            runtime::Value::String(s) => s.clone(),
            runtime::Value::Int(n) => n.to_string(),
            runtime::Value::Bool(b) => b.to_string(),
            runtime::Value::Null => "null".to_string(),
            runtime::Value::Array(arr) => arr
                .iter()
                .map(Self::value_to_string)
                .collect::<Vec<_>>()
                .join(","),
        }
    }

    /// Convert runtime::Value to serde_json::Value
    fn value_to_json(value: &runtime::Value) -> serde_json::Value {
        match value {
            runtime::Value::String(s) => serde_json::Value::String(s.clone()),
            runtime::Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            runtime::Value::Bool(b) => serde_json::Value::Bool(*b),
            runtime::Value::Null => serde_json::Value::Null,
            runtime::Value::Array(arr) => {
                let json_arr = arr.iter().map(Self::value_to_json).collect();
                serde_json::Value::Array(json_arr)
            }
        }
    }
}

impl<R: AsyncBufReadExt + Unpin> Debug for Pulsar<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar").finish()
    }
}
