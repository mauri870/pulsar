mod vm;

use anyhow::Result;
use clap::Parser;
use log::{debug};
use std::fmt::Debug;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
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

pub struct MapReduce<R: AsyncBufReadExt + Unpin> {
    reader: R,
    script: String,
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
        Ok(MapReduce { reader, script })
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
        let all_pairs: Vec<(String, i32)> = tokio::task::spawn_blocking(move || {
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
                    
                    // Try to get reduce function - support both function declarations and const assignments
                    let reduce_fn: rquickjs::Function = globals.get("reduce")
                        .or_else(|_| {
                            // If direct access fails, try evaluating "reduce" to get the const
                            ctx.eval("reduce")
                        })
                        .expect("reduce function not found in script. Make sure to define either 'function reduce() {}' or 'const reduce = () => {}'");

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

impl<R: AsyncBufReadExt + Unpin> Debug for MapReduce<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapReduce")
            .field("script", &self.script)
            .finish()
    }
}