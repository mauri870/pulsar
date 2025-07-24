mod vm;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::debug;
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

        // map phase - async approach with chunked processing
        let mut all_pairs = Vec::new();
        
        for chunk in lines.chunks(CHUNK_SIZE) {
            let vm = vm::VM::new().await.expect("Failed to create VM");
            
            // Evaluate script once per VM
            vm.eval_script(&code).await.map_err(|e| {
                anyhow::anyhow!("Failed to evaluate js script: {:?}", e)
            })?;
            
            for line in chunk.iter() {
                let line_clone = line.clone();
                
                // Use the new async function calling method
                let result = vm.call_function_with_string("map", line_clone).await.map_err(|e| {
                    anyhow::anyhow!("Failed to call map function: {:?}", e)
                })?;
                
                // Parse the JSON result back into key-value pairs
                let pairs = vm.eval(move |ctx| {
                    let mut chunk_pairs = Vec::new();
                    
                    // Try to parse the result as JSON
                    if let Ok(js_result) = ctx.json_parse(result.clone()) {
                        if let Some(array) = js_result.as_array() {
                            for i in 0..array.len() {
                                if let Ok(pair) = array.get::<rquickjs::Value>(i) {
                                    if let Some(pair_array) = pair.as_array() {
                                        if pair_array.len() >= 2 {
                                            let key: String = pair_array.get(0).unwrap();
                                            let value: rquickjs::Value = pair_array.get(1).unwrap();
                                            let json_str = ctx.json_stringify(value).unwrap().unwrap().to_string().unwrap();
                                            let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
                                            chunk_pairs.push((key, json_value));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    chunk_pairs
                }).await;
                
                all_pairs.extend(pairs);
            }
        }

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

        // Check if we have a sort function
        let has_sort_function = {
            let vm = vm::VM::new().await.expect("Failed to create VM for sort check");
            let mut has_sort = false;
            vm.eval(|ctx| {
                let _: () = ctx.eval(code_for_reduce.as_str()).unwrap_or(());
                let globals = ctx.globals();
                has_sort = globals.get("sort").map(|_: rquickjs::Function| ()).is_ok()
                    || ctx.eval("sort").map(|_: rquickjs::Function| ()).is_ok();
            }).await;
            has_sort
        };

        let sort_results = if has_sort_function {
            Some(Mutex::new(Vec::new()))
        } else {
            None
        };

        // Process groups using async VMs
        for chunk in groups.chunks(CHUNK_SIZE) {
            let vm = vm::VM::new().await.expect("Failed to create VM");
            vm.eval_script(&code_for_reduce).await.map_err(|e| {
                anyhow::anyhow!("Failed to evaluate JavaScript script for reduce: {:?}", e)
            })?;
            
            for (key, values) in chunk.iter() {
                let key_clone = key.clone();
                let values_json: Vec<String> = values.iter()
                    .map(|v| serde_json::to_string(v).unwrap())
                    .collect();
                let values_json_str = format!("[{}]", values_json.join(","));
                
                // Call reduce function with key and array
                let result_json = vm.call_function_with_json_args("reduce", vec![
                    format!("\"{}\"", key_clone),
                    values_json_str
                ]).await.map_err(|e| {
                    anyhow::anyhow!("Failed to call reduce function: {:?}", e)
                })?;
                
                let result_value: serde_json::Value = serde_json::from_str(&result_json)
                    .unwrap_or_else(|_| serde_json::Value::String(result_json));
                let result = (key_clone, result_value);

                if let Some(ref results) = sort_results {
                    results.lock().unwrap().push(result);
                } else {
                    tx_clone.send(result).await.unwrap();
                }
            }
        }

        // apply sorting if needed
        if let Some(sort_results) = sort_results {
            let results = sort_results.into_inner().unwrap();
            let vm = vm::VM::new().await.expect("Failed to create VM for sorting");
            vm.eval_script(&code_for_reduce).await.map_err(|e| {
                anyhow::anyhow!("Failed to evaluate JavaScript script for sorting: {:?}", e)
            })?;

            // Convert results to JavaScript array for sorting
            let results_json: Vec<String> = results.iter()
                .map(|(key, value)| format!("[\"{}\", {}]", key, serde_json::to_string(value).unwrap()))
                .collect();
            let array_str = format!("[{}]", results_json.join(","));
            
            let sorted_json = vm.call_function_with_json_args("sort", vec![array_str]).await
                .map_err(|e| anyhow::anyhow!("Failed to call sort function: {:?}", e))?;
            
            // Parse and send sorted results
            let sorted_array: serde_json::Value = serde_json::from_str(&sorted_json)
                .map_err(|e| anyhow::anyhow!("Failed to parse sorted results: {:?}", e))?;
            
            if let serde_json::Value::Array(arr) = sorted_array {
                for item in arr {
                    if let serde_json::Value::Array(pair) = item {
                        if pair.len() >= 2 {
                            if let (serde_json::Value::String(key), value) = (&pair[0], &pair[1]) {
                                tx_clone.send((key.clone(), value.clone())).await.unwrap();
                            }
                        }
                    }
                }
            }
        }

        drop(tx_clone);

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
