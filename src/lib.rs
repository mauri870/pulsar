mod vm;

use anyhow::Result;
use clap::Parser;
use log::{debug};
use std::fmt::Debug;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::AsyncReadExt;
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
    
    /// JavaScript file containing map and reduce functions. If not provided, defaults to a word count script.
    #[arg(short = 's', long = "script")]
    script_file: Option<String>,
}

pub struct MapReduce {
    buf: String,
    script: String,
}

impl MapReduce {
    /// Create a new MapReduce instance from CLI arguments
    pub async fn from_cli(cli: Cli) -> Result<Self> {
        let buf = if cli.input_file == "-" {
            // Read from stdin
            let mut stdin = tokio::io::stdin();
            let mut buf = String::new();
            stdin.read_to_string(&mut buf).await
                .map_err(|e| anyhow::anyhow!("Failed to read from stdin: {}", e))?;
            buf
        } else {
            // Read from file
            tokio::fs::read_to_string(&cli.input_file).await
                .map_err(|e| anyhow::anyhow!("Failed to read file {}: {}", cli.input_file, e))?
        };

        let script = if let Some(script_file) = cli.script_file {
            // Read custom script from file
            tokio::fs::read_to_string(&script_file).await
                .map_err(|e| anyhow::anyhow!("Failed to read script file {}: {}", script_file, e))?
        } else {
            // Use default word count script
            DEFAULT_SCRIPT.into()
        };
        Ok(MapReduce { buf, script })
    }

    /// Run the application
    pub async fn run(self) -> Result<()> {
        debug!("Running mapreduce");
        let code = self.script.clone();
        let code_for_reduce = code.clone();

        // Collect all non-empty lines
        let lines: Vec<String> = self.buf.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|s| s.to_string())
            .collect();

        // map phase
        let all_pairs: Vec<(String, i32)> = tokio::task::spawn_blocking(move || {
            lines.par_iter().flat_map(|line| {
                let vm = vm::VM::new().expect("Failed to create VM");
                let mut intermediate_pairs = Vec::new();

                vm.eval(|ctx| {
                    let _: () = ctx.eval(code.as_str()).unwrap();
                    let globals = ctx.globals();
                    let map_fn: rquickjs::Function = globals.get("map").unwrap();

                    // Process this single line through the map function
                    let result = map_fn.call::<_, rquickjs::Value>((line.as_str(),)).unwrap();

                    // Convert JavaScript array result to Rust vector
                    if let Some(array) = result.as_array() {
                        for i in 0..array.len() {
                            if let Ok(pair) = array.get::<rquickjs::Value>(i) {
                                if let Some(pair_array) = pair.as_array() {
                                    if pair_array.len() >= 2 {
                                        let key: String = pair_array.get(0).unwrap();
                                        let value: i32 = pair_array.get(1).unwrap();
                                        intermediate_pairs.push((key, value));
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
        let mut groups: HashMap<String, Vec<i32>> = HashMap::new();
        for (key, value) in all_pairs {
            groups.entry(key).or_insert_with(Vec::new).push(value);
        }

        // reduce phase 
        let results: Vec<(String, i32)> = tokio::task::spawn_blocking(move || {
            groups.into_par_iter().map(|(key, values)| {
                let vm = vm::VM::new().expect("Failed to create VM");
                let mut result = 0;

                vm.eval(|ctx| {
                    let _: () = ctx.eval(code_for_reduce.as_str()).unwrap();
                    let globals = ctx.globals();
                    let reduce_fn: rquickjs::Function = globals.get("reduce").unwrap();

                    // Convert Rust vector to JavaScript array
                    let array_str = format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
                    let js_array: rquickjs::Value = ctx.eval(array_str.as_str()).unwrap();
                    result = reduce_fn.call::<_, i32>((key.clone(), js_array)).unwrap();
                });

                (key, result)
            }).collect()
        }).await?;

        // Print results
        for (key, count) in results {
            println!("{}: {}", key, count);
        }

        Ok(())
    }
}

impl Debug for MapReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapReduce")
            .field("buf", &self.buf)
            .finish()
    }
}