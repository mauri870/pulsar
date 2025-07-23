use anyhow::{Context, Result};
use clap::Parser;
use log::{debug};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::{self, Read};
use std::path::Path;
use thiserror::Error;

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
        let buf = if cli.input_file == "-" {
            let mut input = String::new();
            io::stdin().read_to_string(&mut input).context("Failed to read from stdin")?;
            input
        } else {
            // read input and return String
            let path = Path::new(&cli.input_file);
            if !path.exists() {
                return Err(anyhow::anyhow!("Input file does not exist: {}", cli.input_file));
            }
            let mut file = std::fs::File::open(path).context("Failed to open input file")?;
            let mut input = String::new();
            file.read_to_string(&mut input).context("Failed to read input file")?;
            input
        };

        Ok(MapReduce {
            buf: buf,
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