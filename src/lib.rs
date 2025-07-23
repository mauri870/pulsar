mod vm;

use anyhow::Result;
use clap::Parser;
use log::{debug};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::cell::RefCell;
use std::io::{BufRead, BufReader};
use std::fs::File;
use thiserror::Error;

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
    /// Run the application
    pub fn run(self) -> Result<()> {
        debug!("Running mapreduce");
        Ok(())
    }
}

impl TryFrom<Cli> for MapReduce {
    type Error = anyhow::Error;
    fn try_from(cli: Cli) -> Result<Self> {
        let mut reader: Box<dyn BufRead> = if cli.input_file == "-" {
            Box::new(BufReader::new(std::io::stdin()))
        } else {
            Box::new(BufReader::new(File::open(&cli.input_file)?))
        };

        let mut buf = String::new();
        reader
            .read_to_string(&mut buf)
            .map_err(|e| anyhow::anyhow!("Failed to read input: {}", e))?;
        Ok(MapReduce {
            buf,
        })
    }
}

impl Debug for MapReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapReduce")
            .field("buf", &self.buf)
            .finish()
    }
}