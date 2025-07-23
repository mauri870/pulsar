mod vm;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::{debug};
use std::fmt::{Debug, Display};
use std::collections::HashMap;
use std::io::{self, Write};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::signal;
use rayon::prelude::*;

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");

#[derive(Debug, Error)]
pub enum MapReduceError {
    #[error("no files or directories to watch")]
    NoFilesToWatch,
}

#[derive(Debug, Parser)]
#[command(name = "mapreduce")]
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

pub struct MapReduce<R: AsyncBufReadExt + Unpin> {
    reader: R,
    script: String,
    output_format: OutputFormat,
}

impl MapReduce<BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
    /// Create a new MapReduce instance from CLI arguments
    pub async fn from_cli(cli: Cli) -> Result<Self> {
        let reader: BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>> = if cli.input_file == "-" {
            // Read from stdin
            let stdin = tokio::io::stdin();
            BufReader::new(Box::new(stdin))
        } else {
            // Read from file
            let file = tokio::fs::File::open(&cli.input_file).await
                .map_err(|e| anyhow::anyhow!("Failed to open file {}: {}", cli.input_file, e))?;
            BufReader::new(Box::new(file))
        };

        let script = if let Some(script_file) = cli.script_file {
            // Read custom script from file
            tokio::fs::read_to_string(&script_file).await
                .map_err(|e| anyhow::anyhow!("Failed to read script file {}: {}", script_file, e))?
        } else {
            // Use default word count script
            DEFAULT_SCRIPT.into()
        };
        Ok(MapReduce { reader, script, output_format: cli.output_format })
    }

    /// Run the application
    pub async fn run(mut self) -> Result<()> {
        debug!("Running mapreduce");
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
            lines.par_iter().flat_map(|line| {
                let vm = vm::VM::new().expect("Failed to create VM");
                let mut intermediate_pairs = Vec::new();

                vm.eval(|ctx| {
                    let _: () = ctx.eval(code.as_str()).expect("Failed to evaluate js script");
                    let globals = ctx.globals();

                    // get the map function, support both function declaration and const assignment
                    let map_fn: rquickjs::Function = globals.get("map")
                        .or_else(|_| {
                            // try with const map
                            ctx.eval("map")
                        })
                        .expect("map function not found in script. Make sure to define either 'function map() {}' or 'const map = () => {}'");

                    // Process this single line through the map function
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
                                        intermediate_pairs.push((key, json_value));
                                    }
                                }
                            }
                        }
                    }
                });

                intermediate_pairs
            }).collect()
        }).await?;

        // group phase
        let mut groups: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for (key, value) in all_pairs {
            groups.entry(key).or_insert_with(Vec::new).push(value);
        }

        // reduce phase, stream results as we compute them
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
                                        // Convert serde_json::Value to a nice display format for plain output
                                        let display_value = match &result {
                                            serde_json::Value::String(s) => s.clone(),
                                            serde_json::Value::Number(n) => n.to_string(),
                                            serde_json::Value::Bool(b) => b.to_string(),
                                            serde_json::Value::Null => "null".to_string(),
                                            serde_json::Value::Array(arr) => {
                                                // for plain mode print array elements as comma-separated values
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
                                        io::stdout().flush().unwrap(); // Force immediate output
                                    }
                                    OutputFormat::Json => {
                                        println!("{{\"{}\": {}}}", key, serde_json::to_string(&result).unwrap());
                                        io::stdout().flush().unwrap(); // Force immediate output
                                    }
                                }
                            }
                            None => {
                                // all results processed
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

        // Process reduce operations in parallel but send results to output thread
        let tx_clone = tx.clone();
        drop(tx); // Drop original to ensure proper channel closure
        tokio::task::spawn_blocking(move || {
            groups.into_par_iter().for_each(|(key, values)| {
                let vm = vm::VM::new().expect("Failed to create VM");
                let mut result = serde_json::Value::Null;

                vm.eval(|ctx| {
                    let _: () = ctx.eval(code_for_reduce.as_str()).unwrap();
                    let globals = ctx.globals();

                    // Try to get reduce function - support both function declarations and const assignments
                    let reduce_fn: rquickjs::Function = globals.get("reduce")
                        .or_else(|_| {
                            // If direct access fails, try evaluating "reduce" to get the const
                            ctx.eval("reduce")
                        })
                        .expect("reduce function not found in script. Make sure to define either 'function reduce() {}' or 'const reduce = () => {}'");

                    // Convert serde_json::Values back to rquickjs::Values
                    let js_array = rquickjs::Array::new(ctx.clone()).unwrap();
                    for (i, v) in values.iter().enumerate() {
                        let js_val: rquickjs::Value = ctx.json_parse(serde_json::to_string(v).unwrap()).unwrap();
                        js_array.set(i, js_val).unwrap();
                    }

                    let reduce_result: rquickjs::Value = reduce_fn.call((key.clone(), js_array)).unwrap();

                    // Convert result back to serde_json::Value
                    let js_string = ctx.json_stringify(reduce_result).unwrap().unwrap();
                    let json_str: String = js_string.to_string().unwrap();
                    result = serde_json::from_str(&json_str).unwrap();
                });

                // Send result to output thread for streaming
                tx_clone.blocking_send((key, result)).unwrap();
            });
            // Drop the sender to signal completion
            drop(tx_clone);
        }).await?;

        control_handle.await?;

        Ok(())
    }
}

impl<R: AsyncBufReadExt + Unpin> Debug for MapReduce<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapReduce")
            .field("script", &self.script)
            .finish()
    }
}