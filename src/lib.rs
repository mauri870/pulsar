mod vm;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::debug;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::io::{self, Write};
use std::sync::Mutex;
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

        // map phase
        let all_pairs: Vec<(String, serde_json::Value)> = tokio::task::spawn_blocking(move || {
            lines.par_chunks(CHUNK_SIZE)
                .flat_map(|chunk| {
                    let vm = vm::VM::new().expect("Failed to create VM");
                    let mut all_intermediate_pairs = Vec::new();

                    vm.eval(|ctx| {
                        // Evaluate script once per VM (for the entire chunk)
                        let _: () = ctx.eval(code.as_str()).expect("Failed to evaluate js script");
                        let globals = ctx.globals();

                        // get the map function, support both function declaration and const assignment
                        let map_fn: rquickjs::Function = globals.get("map")
                            .or_else(|_| ctx.eval("map"))
                            .expect("map function not found in script. Make sure to define either 'function map() {}' or 'const map = () => {}'");

                        // Process all lines in this chunk
                        for line in chunk.iter() {
                            let result = map_fn.call::<_, rquickjs::Value>((line.as_str(),)).unwrap();

                            // Convert JavaScript array result to Rust vector
                            if let Some(array) = result.as_array() {
                                for i in 0..array.len() {
                                    if let Ok(pair) = array.get::<rquickjs::Value>(i) {
                                        if let Some(pair_array) = pair.as_array() {
                                            if pair_array.len() >= 2 {
                                                let key: String = pair_array.get(0).unwrap();
                                                let value: rquickjs::Value = pair_array.get(1).unwrap();
                                                // Convert rquickjs::Value to serde_json::Value for storage
                                                let json_str = ctx.json_stringify(value).unwrap().unwrap().to_string().unwrap();
                                                let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
                                                all_intermediate_pairs.push((key, json_value));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });

                    all_intermediate_pairs
                })
                .collect()
        }).await?;

        // group phase
        let mut groups_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for (key, value) in all_pairs {
            groups_map.entry(key).or_insert_with(Vec::new).push(value);
        }

        let groups: Vec<(String, Vec<serde_json::Value>)> = groups_map.into_iter().collect();

        // reduce phase - unified approach with chunked VMs
        let output_format = self.output_format.clone();
        let (tx, mut rx) = mpsc::channel::<(String, serde_json::Value)>(1000);

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
        let tx_clone = tx.clone();
        drop(tx); // Drop original to ensure proper channel closure

        tokio::task::spawn_blocking(move || {
            // Check if we have a sort function
            let has_sort_function = {
                let vm = vm::VM::new().expect("Failed to create VM for sort check");
                let mut has_sort = false;
                vm.eval(|ctx| {
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
                let vm = vm::VM::new().expect("Failed to create VM");
                vm.eval(|ctx| {
                    let _: () = ctx.eval(code_for_reduce.as_str()).unwrap();
                    let globals = ctx.globals();
                    let reduce_fn: rquickjs::Function = globals
                        .get("reduce")
                        .or_else(|_| ctx.eval("reduce"))
                        .expect("reduce function not found");

                    // Process all groups in this chunk
                    for (key, values) in chunk.iter() {
                        // Convert values to JS array
                        let js_array = rquickjs::Array::new(ctx.clone()).unwrap();
                        for (i, v) in values.iter().enumerate() {
                            let js_val: rquickjs::Value =
                                ctx.json_parse(serde_json::to_string(v).unwrap()).unwrap();
                            js_array.set(i, js_val).unwrap();
                        }

                        // Call reduce for this key
                        let reduce_result: rquickjs::Value =
                            reduce_fn.call((key.clone(), js_array)).unwrap();
                        // Convert to serde_json::Value
                        let json_str = ctx
                            .json_stringify(reduce_result)
                            .unwrap()
                            .unwrap()
                            .to_string()
                            .unwrap();
                        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

                        if let Some(ref results) = sort_results {
                            // Collect for sorting
                            results.lock().unwrap().push((key.clone(), value));
                        } else {
                            // Stream immediately
                            tx_clone.blocking_send((key.clone(), value)).unwrap();
                        }
                    }
                });
            });

            // apply sorting if needed
            if let Some(sort_results) = sort_results {
                let results = sort_results.into_inner().unwrap();
                let vm = vm::VM::new().expect("Failed to create VM for sorting");
                vm.eval(|ctx| {
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
                                        tx_clone.blocking_send((key, value)).unwrap();
                                    }
                                }
                            }
                        }
                    }
                });
            }

            drop(tx_clone);
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
