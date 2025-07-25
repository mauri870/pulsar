mod runtime;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::debug;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::io::{self, Write};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
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
    reader: R,
    output_format: OutputFormat,
    runtime: Arc<dyn runtime::Runtime + Send + Sync>,
}

impl Pulsar<BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
    /// Create a new Pulsar instance from CLI arguments
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
            reader,
            output_format: cli.output_format,
            runtime: Arc::new(runtime::JavaScriptRuntime::new(script.clone())?),
        })
    }

    /// Run the application
    pub async fn run(mut self) -> Result<()> {
        debug!("Running pulsar");

        // Collect all non-empty lines
        let mut lines = Vec::new();
        let mut line_buf = String::new();
        while self.reader.read_line(&mut line_buf).await? > 0 {
            let line = line_buf.trim();
            if !line.is_empty() {
                lines.push(line.to_string());
            }
            line_buf.clear();
        }

        // Map phase - use tokio::spawn for concurrent processing
        let chunk_size = 5; // Smaller chunks to balance parallelism with VM overhead
        let mut map_tasks = Vec::new();
        let runtime = Arc::clone(&self.runtime);

        for (chunk_idx, chunk) in lines.chunks(chunk_size).enumerate() {
            let chunk_lines = chunk.to_vec();
            let runtime_clone = Arc::clone(&runtime);

            let task = tokio::spawn(async move {
                let mut chunk_results: Vec<Vec<runtime::KeyValue>> = Vec::new();

                for line in chunk_lines.iter() {
                    let context = runtime::RuntimeContext::new(format!("chunk-{}", chunk_idx));
                    match runtime_clone.map(line, &context).await {
                        Ok(mapped) => chunk_results.push(mapped),
                        Err(e) => {
                            eprintln!("Map error for line '{}': {}", line, e);
                            continue;
                        }
                    }
                }

                (chunk_idx, chunk_results)
            });

            map_tasks.push(task);
        }

        // Wait for all map tasks to complete
        let mut mapped_results = Vec::new();
        for task in map_tasks {
            match task.await {
                Ok(result) => mapped_results.push(result),
                Err(e) => eprintln!("Task error: {}", e),
            }
        }

        // Flatten the results into a single vector of KeyValue pairs
        let all_pairs: Vec<runtime::KeyValue> = mapped_results
            .into_iter()
            .flat_map(|(_, chunk_results)| chunk_results.into_iter())
            .flatten()
            .collect();

        // Group phase
        let mut groups_map: HashMap<String, Vec<runtime::Value>> = HashMap::new();
        for r in all_pairs.iter() {
            groups_map
                .entry(r.key.clone())
                .or_insert_with(Vec::new)
                .push(r.value.clone());
        }

        let groups: Vec<(String, Vec<runtime::Value>)> = groups_map.into_iter().collect();

        // Reduce phase - use tokio::spawn for concurrent processing
        let output_format = self.output_format.clone();
        let (async_tx, mut rx) = mpsc::channel::<(String, runtime::Value)>(1000);

        let control_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Some((key, result)) => {
                                match output_format {
                                    OutputFormat::Plain => {
                                        let display_value = match &result {
                                            runtime::Value::String(s) => s.clone(),
                                            runtime::Value::Int(n) => n.to_string(),
                                            runtime::Value::Bool(b) => b.to_string(),
                                            runtime::Value::Null => "null".to_string(),
                                            runtime::Value::Array(arr) => {
                                                arr.iter()
                                                    .map(|v| match v {
                                                        runtime::Value::String(s) => s.clone(),
                                                        runtime::Value::Int(n) => n.to_string(),
                                                        runtime::Value::Bool(b) => b.to_string(),
                                                        runtime::Value::Null => "null".to_string(),
                                                        _ => format!("{:?}", v),
                                                    })
                                                    .collect::<Vec<_>>()
                                                    .join(",")
                                            },
                                        };
                                        println!("{}: {}", key, display_value);
                                        io::stdout().flush().unwrap();
                                    }
                                    OutputFormat::Json => {
                                        let json_value = match &result {
                                            runtime::Value::String(s) => serde_json::Value::String(s.clone()),
                                            runtime::Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                                            runtime::Value::Bool(b) => serde_json::Value::Bool(*b),
                                            runtime::Value::Null => serde_json::Value::Null,
                                            runtime::Value::Array(arr) => {
                                                let json_arr = arr.iter().map(|v| match v {
                                                    runtime::Value::String(s) => serde_json::Value::String(s.clone()),
                                                    runtime::Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                                                    runtime::Value::Bool(b) => serde_json::Value::Bool(*b),
                                                    runtime::Value::Null => serde_json::Value::Null,
                                                    _ => serde_json::Value::String(format!("{:?}", v)),
                                                }).collect::<Vec<_>>();
                                                serde_json::Value::Array(json_arr)
                                            },
                                        };
                                        println!("{{\"{}\": {}}}", key, serde_json::to_string(&json_value).unwrap());
                                        io::stdout().flush().unwrap();
                                    }
                                }
                            }
                            None => {
                                break;
                            }
                        }
                    }
                    _ = signal::ctrl_c() => {
                        eprintln!("Received Ctrl+C, shutting down gracefully...");
                        break;
                    }
                }
            }
        });

        // Check if we have a sort function
        let runtime = Arc::clone(&self.runtime);
        let has_sort_function = runtime.has_sort();

        if has_sort_function {
            // Collect all reduce results for sorting
            let mut reduce_tasks = Vec::new();

            for (key, values) in groups {
                let context = runtime::RuntimeContext::new(format!("reduce-{}", key));
                let runtime_clone = Arc::clone(&runtime);
                let task = tokio::spawn(async move {
                    match runtime_clone
                        .reduce(runtime::Value::String(key.clone()), values, &context)
                        .await
                    {
                        Ok(reduced_value) => Some((key, reduced_value)),
                        Err(e) => {
                            eprintln!("Reduce error for key '{}': {}", key, e);
                            None
                        }
                    }
                });
                reduce_tasks.push(task);
            }

            // Wait for all reduce tasks and collect results
            let mut reduced_results = Vec::new();
            for task in reduce_tasks {
                match task.await {
                    Ok(Some((key, value))) => {
                        reduced_results.push(runtime::KeyValue { key, value })
                    }
                    Ok(None) => {} // Error case, already logged
                    Err(e) => eprintln!("Task error: {}", e),
                }
            }

            // Apply sorting
            let sort_context = runtime::RuntimeContext::new("sort".to_string());
            match runtime.sort(reduced_results, &sort_context).await {
                Ok(sorted_pairs) => {
                    // Send sorted results to output
                    for kv in sorted_pairs {
                        let _ = async_tx.send((kv.key, kv.value)).await;
                    }
                }
                Err(e) => {
                    eprintln!("Sort error: {}", e);
                }
            }
        } else {
            // Stream results immediately without sorting
            let mut reduce_tasks = Vec::new();

            for (key, values) in groups {
                let async_tx_clone = async_tx.clone();
                let context = runtime::RuntimeContext::new(format!("reduce-{}", key));
                let runtime_clone = Arc::clone(&runtime);

                let task = tokio::spawn(async move {
                    match runtime_clone
                        .reduce(runtime::Value::String(key.clone()), values, &context)
                        .await
                    {
                        Ok(reduced_value) => {
                            let _ = async_tx_clone.send((key, reduced_value)).await;
                        }
                        Err(e) => {
                            eprintln!("Reduce error for key '{}': {}", key, e);
                        }
                    }
                });
                reduce_tasks.push(task);
            }

            // Wait for all reduce tasks to complete
            for task in reduce_tasks {
                let _ = task.await;
            }
        }

        drop(async_tx);
        control_handle.await?;

        Ok(())
    }
}

impl<R: AsyncBufReadExt + Unpin> Debug for Pulsar<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar").finish()
    }
}
