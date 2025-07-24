mod vm;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::debug;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::io::{self, Write};
use std::sync::Mutex;
use std::sync::mpsc as sync_mpsc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use vm::VM;

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
    script: String,
    output_format: OutputFormat,
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
            script,
            output_format: cli.output_format,
        })
    }

    /// Run the application
    pub async fn run(mut self) -> Result<()> {
        debug!("Running pulsar");
        let code = self.script.clone();
        let code_for_reduce = code.clone();

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

        // Map phase
        let chunk_size = CHUNK_SIZE;
        let script_content = code.clone();
        let map_results: Vec<_> = spawn_blocking(move || {
            lines
                .par_chunks(chunk_size)
                .enumerate()
                .map(|(chunk_idx, chunk)| {
                    let vm = VM::new_sync().expect("Failed to create VM");
                    if let Err(e) = vm.eval_script_sync(&script_content) {
                        eprintln!("Failed to evaluate js script: {:#?}", e);
                        std::process::exit(1);
                    }

                    let chunk_results: Vec<String> = chunk
                        .iter()
                        .map(|line| {
                            vm.call_function_with_string_sync("map", line.clone())
                                .expect("Failed to call map function")
                        })
                        .collect();

                    // Parse and collect key-value pairs
                    let mut pairs = Vec::new();
                    for result in chunk_results {
                        // The map function returns an array of [key, value] pairs
                        match serde_json::from_str::<Vec<(String, serde_json::Value)>>(&result) {
                            Ok(result_pairs) => {
                                for (key, value) in result_pairs {
                                    pairs.push((key, value));
                                }
                            }
                            Err(e) => eprintln!("Failed to parse map result: {} - {}", result, e),
                        }
                    }
                    (chunk_idx, pairs)
                })
                .collect()
        })
        .await
        .expect("Map phase failed");

        // Collect all pairs from chunks
        let mut all_pairs = Vec::new();
        for (_, pairs) in map_results {
            all_pairs.extend(pairs);
        }

        // group phase
        let mut groups_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for (key, value) in all_pairs {
            groups_map.entry(key).or_insert_with(Vec::new).push(value);
        }

        let groups: Vec<(String, Vec<serde_json::Value>)> = groups_map.into_iter().collect();

        // reduce phase - unified approach with chunked VMs
        let output_format = self.output_format.clone();
        let (async_tx, mut rx) = mpsc::channel::<(String, serde_json::Value)>(1000);

        let control_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Some((key, result)) => {
                                match output_format {
                                    OutputFormat::Plain => {
                                        let display_value = match &result {
                                            serde_json::Value::String(s) => s.clone(),
                                            serde_json::Value::Number(n) => n.to_string(),
                                            serde_json::Value::Bool(b) => b.to_string(),
                                            serde_json::Value::Null => "null".to_string(),
                                            serde_json::Value::Array(arr) => {
                                                arr.iter()
                                                    .map(|v| match v {
                                                        serde_json::Value::String(s) => s.clone(),
                                                        serde_json::Value::Number(n) => n.to_string(),
                                                        serde_json::Value::Bool(b) => b.to_string(),
                                                        serde_json::Value::Null => "null".to_string(),
                                                        _ => serde_json::to_string(v).unwrap(),
                                                    })
                                                    .collect::<Vec<_>>()
                                                    .join(",")
                                            },
                                            _ => serde_json::to_string(&result).unwrap(),
                                        };
                                        println!("{}: {}", key, display_value);
                                        io::stdout().flush().unwrap();
                                    }
                                    OutputFormat::Json => {
                                        println!("{{\"{}\": {}}}", key, serde_json::to_string(&result).unwrap());
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

        // Unified reduce processing with chunked VMs
        let async_tx_clone = async_tx.clone();
        drop(async_tx); // Drop original to ensure proper channel closure

        tokio::task::spawn_blocking(move || {
            // Use sync channel within the blocking task
            let (sync_tx, sync_rx) = sync_mpsc::channel::<(String, serde_json::Value)>();

            // Spawn a task to forward from sync channel to async channel
            let async_tx_for_forward = async_tx_clone.clone();
            std::thread::spawn(move || {
                while let Ok((key, value)) = sync_rx.recv() {
                    if async_tx_for_forward.blocking_send((key, value)).is_err() {
                        break; // Channel closed
                    }
                }
            });

            // Check if we have a sort function
            let has_sort_function = {
                let vm = VM::new_sync().expect("Failed to create VM for sort check");
                let mut has_sort = false;
                vm.eval_sync(|ctx| {
                    let _: () = ctx.eval(code_for_reduce.as_str()).unwrap_or(());
                    let globals = ctx.globals();
                    has_sort = globals.get("sort").map(|_: rquickjs::Function| ()).is_ok()
                        || ctx.eval("sort").map(|_: rquickjs::Function| ()).is_ok();
                });
                has_sort
            };

            let sort_results = if has_sort_function {
                Some(Mutex::new(Vec::new()))
            } else {
                None
            };

            groups.par_chunks(CHUNK_SIZE).for_each(|chunk| {
                let vm = VM::new_sync().expect("Failed to create VM");
                let sync_tx_clone = sync_tx.clone();
                vm.eval_script_sync(&code_for_reduce)
                    .expect("Failed to evaluate script");

                // Process all groups in this chunk
                for (key, values) in chunk.iter() {
                    // Call reduce for this key using async-aware method
                    let values_json: Vec<String> = values
                        .iter()
                        .map(|v| {
                            // If the value is already a string, don't double-encode it
                            match v {
                                serde_json::Value::String(s) => format!("\"{}\"", s),
                                _ => serde_json::to_string(v).unwrap(),
                            }
                        })
                        .collect();
                    let args = vec![
                        serde_json::to_string(&key).unwrap(),
                        format!("[{}]", values_json.join(",")),
                    ];
                    let result_json = vm
                        .call_function_with_json_args_sync("reduce", args)
                        .unwrap();
                    let value: serde_json::Value = serde_json::from_str(&result_json).unwrap();

                    if let Some(ref results) = sort_results {
                        // Collect for sorting
                        results.lock().unwrap().push((key.clone(), value));
                    } else {
                        // Stream immediately
                        sync_tx_clone.send((key.clone(), value)).unwrap();
                    }
                }
            });

            // apply sorting if needed
            if let Some(sort_results) = sort_results {
                let results = sort_results.into_inner().unwrap();
                let vm = VM::new_sync().expect("Failed to create VM for sorting");
                vm.eval_sync(|ctx| {
                    let _: () = ctx.eval(code_for_reduce.as_str()).unwrap();
                    let globals = ctx.globals();
                    let sort_fn: rquickjs::Function = globals
                        .get("sort")
                        .or_else(|_| ctx.eval("sort"))
                        .expect("sort function not found");

                    // Convert results to JavaScript array for sorting
                    let js_array = rquickjs::Array::new(ctx.clone()).unwrap();
                    for (i, (key, value)) in results.iter().enumerate() {
                        let result_pair = rquickjs::Array::new(ctx.clone()).unwrap();
                        result_pair.set(0, key.clone()).unwrap();
                        let js_value: rquickjs::Value = ctx
                            .json_parse(serde_json::to_string(value).unwrap())
                            .unwrap();
                        result_pair.set(1, js_value).unwrap();
                        js_array.set(i, result_pair).unwrap();
                    }

                    let sorted_array: rquickjs::Value = sort_fn.call((js_array,)).unwrap();

                    // Send sorted results to output thread
                    if let Some(array) = sorted_array.as_array() {
                        for i in 0..array.len() {
                            if let Ok(pair) = array.get::<rquickjs::Value>(i) {
                                if let Some(pair_array) = pair.as_array() {
                                    if pair_array.len() >= 2 {
                                        let key: String = pair_array.get(0).unwrap();
                                        let value_js: rquickjs::Value = pair_array.get(1).unwrap();
                                        // Convert to serde_json::Value for output formatting
                                        let json_str = ctx
                                            .json_stringify(value_js)
                                            .unwrap()
                                            .unwrap()
                                            .to_string()
                                            .unwrap();
                                        let value: serde_json::Value =
                                            serde_json::from_str(&json_str).unwrap();
                                        sync_tx.send((key, value)).unwrap();
                                    }
                                }
                            }
                        }
                    }
                });
            }

            drop(sync_tx);
        })
        .await?;

        control_handle.await?;

        Ok(())
    }
}

impl<R: AsyncBufReadExt + Unpin> Debug for Pulsar<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar")
            .field("script", &self.script)
            .finish()
    }
}
