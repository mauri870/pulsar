mod vm;

use anyhow::Result;
use clap::Parser;
use log::{debug};
use std::fmt::Debug;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::AsyncReadExt;

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
    /// File or directory to watch. To specify multiple files or directories, use standard input instead.
    #[arg(short = 'f', default_value = "-")]
    input_file: String,
}

pub struct MapReduce {
    buf: String
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

        Ok(MapReduce { buf })
    }

    /// Run the application
    pub async fn run(self) -> Result<()> {
        debug!("Running mapreduce");
        let code = r#"
function map(line) {
  return line.split(/\s+/).map(word => [word.toLowerCase(), 1]);
}

function reduce(key, values) {
  return values.reduce((a, b) => a + b, 0);
}
"#;

        // Collect all non-empty lines
        let lines: Vec<String> = self.buf.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|s| s.to_string())
            .collect();

        // Process each line individually in parallel
        let mut tasks = Vec::new();

        for line in lines {
            let code = code.to_string();

            let task = tokio::task::spawn_blocking(move || {
                // Create a new VM instance for each line
                let vm = vm::VM::new().map_err(|e| anyhow::anyhow!("Failed to create VM: {:?}", e))?;

                vm.eval(|ctx| -> Result<Vec<(String, i32)>> {
                    let _: () = ctx.eval(code.as_str())?;
                    let globals = ctx.globals();
                    let map_fn: rquickjs::Function = globals.get("map")?;

                    let mut intermediate_pairs = Vec::new();

                    // Process this single line through the map function
                    let result = map_fn.call::<_, rquickjs::Value>((line.as_str(),))?;

                    // Convert JavaScript array result to Rust vector
                    if let Some(array) = result.as_array() {
                        for i in 0..array.len() {
                            if let Ok(pair) = array.get::<rquickjs::Value>(i) {
                                if let Some(pair_array) = pair.as_array() {
                                    if pair_array.len() >= 2 {
                                        let key: String = pair_array.get(0)?;
                                        let value: i32 = pair_array.get(1)?;
                                        intermediate_pairs.push((key, value));
                                    }
                                }
                            }
                        }
                    }

                    Ok(intermediate_pairs)
                })
            });

            tasks.push(task);
        }

        // Collect results from all parallel tasks
        let mut all_pairs = Vec::new();
        for task in tasks {
            let result = task.await
                .map_err(|e| anyhow::anyhow!("Task execution failed: {:?}", e))?;
            let pairs = result?;
            all_pairs.extend(pairs);
        }

        // Group phase: group values by key
        let mut groups: HashMap<String, Vec<i32>> = HashMap::new();
        for (key, value) in all_pairs {
            groups.entry(key).or_insert_with(Vec::new).push(value);
        }

        // Reduce phase: apply reduce function to each group in parallel
        let mut reduce_tasks = Vec::new();

        for (key, values) in groups {
            let code = code.to_string();

            let reduce_task = tokio::task::spawn_blocking(move || {
                let vm = vm::VM::new().map_err(|e| anyhow::anyhow!("Failed to create VM: {:?}", e))?;

                vm.eval(|ctx| -> Result<(String, i32)> {
                    let _: () = ctx.eval(code.as_str())?;
                    let globals = ctx.globals();
                    let reduce_fn: rquickjs::Function = globals.get("reduce")?;

                    // Convert Rust vector to JavaScript array
                    let array_str = format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
                    let js_array: rquickjs::Value = ctx.eval(array_str.as_str())?;
                    let result = reduce_fn.call::<_, i32>((key.clone(), js_array))?;

                    Ok((key, result))
                })
            });

            reduce_tasks.push(reduce_task);
        }

        // Collect and print results from all reduce tasks
        for reduce_task in reduce_tasks {
            let result = reduce_task.await
                .map_err(|e| anyhow::anyhow!("Reduce task execution failed: {:?}", e))?;
            let (key, count) = result?;
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