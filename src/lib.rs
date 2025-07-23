mod vm;

use anyhow::Result;
use clap::Parser;
use log::{debug};
use std::fmt::Debug;
use std::cell::RefCell;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::AsyncReadExt;

// VM is thread-local to avoid contention
thread_local! {
    static VM: RefCell<Option<vm::VM>> = RefCell::new(None);
}

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

        // Process the input in an async task to avoid blocking
        let result = tokio::task::spawn_blocking(move || {
            VM.with(|vm_cell| {
                let mut vm_opt = vm_cell.borrow_mut();
                if vm_opt.is_none() {
                    *vm_opt = Some(vm::VM::new().expect("failed to initialize QJS VM"));
                }
                
                vm_opt.as_ref().unwrap().eval(|ctx| -> Result<()> {
                    let _: () = ctx.eval(code)?;
                    
                    let globals = ctx.globals();
                    let map_fn: rquickjs::Function = globals.get("map")?;
                    let reduce_fn: rquickjs::Function = globals.get("reduce")?;
                    
                    let mut intermediate_pairs: Vec<(String, i32)> = Vec::new();
                    
                    for line in self.buf.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        
                        let result = map_fn.call::<_, rquickjs::Value>((line,))?;
                        
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
                    }
                    
                    // Group phase: group values by key
                    let mut groups: HashMap<String, Vec<i32>> = HashMap::new();
                    for (key, value) in intermediate_pairs {
                        groups.entry(key).or_insert_with(Vec::new).push(value);
                    }
                    
                    // Reduce phase: apply reduce function to each group
                    for (key, values) in groups {
                        // Convert Rust vector to JavaScript array
                        let array_str = format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
                        let js_array: rquickjs::Value = ctx.eval(array_str.as_str())?;
                        let result = reduce_fn.call::<_, i32>((key.clone(), js_array))?;
                        println!("{}: {}", key, result);
                    }
                    
                    Ok(())
                })
            })
        }).await;
        
        match result {
            Ok(vm_result) => match vm_result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!("VM execution failed: {:?}", e)),
            },
            Err(e) => Err(anyhow::anyhow!("Task execution failed: {:?}", e)),
        }
    }
}

impl Debug for MapReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapReduce")
            .field("buf", &self.buf)
            .finish()
    }
}