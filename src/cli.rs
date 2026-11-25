use clap::Parser;
use std::path::PathBuf;

use crate::output::OutputFormat;
use crate::runtime::Engine;

const DEFAULT_CHUNK_SIZE: usize = 64;
const DEFAULT_MEMORY_LIMIT_MB: usize = 1024;

#[derive(Debug, Parser)]
#[command(name = "pulsar")]
#[command(about = "A simple map-reduce engine for parallel processing")]
#[command(author, version)]
pub struct Cli {
    /// Input file to read input data from.
    #[arg(short = 'f', default_value = "-")]
    pub(crate) input_file: String,

    /// Output format for the results.
    #[arg(long = "output", default_value_t = OutputFormat::Plain)]
    pub(crate) output_format: OutputFormat,

    /// JavaScript file containing map and reduce functions. If not provided, defaults to a word count script.
    #[arg(short = 's', long = "script")]
    pub(crate) script_file: Option<String>,

    /// Whether to sort the output before printing. Assumes the script has a `sort` function.
    #[arg(long = "sort", action = clap::ArgAction::SetTrue)]
    pub(crate) sort: bool,

    /// Number of parallel JS VM workers. Defaults to the number of logical CPUs.
    #[arg(short = 'j', long = "workers")]
    pub(crate) workers: Option<usize>,

    /// Run in test mode, executing the script against test cases.
    #[arg(long = "test", action = clap::ArgAction::SetTrue)]
    pub(crate) test: bool,

    /// Number of lines per chunk sent to each worker.
    #[arg(short = 'c', long = "chunk-size", default_value_t = DEFAULT_CHUNK_SIZE)]
    pub(crate) chunk_size: usize,

    /// JavaScript engine. llrt supports Node-style APIs (require, timers, fetch); v8 is much
    /// faster on script-heavy work but runs plain ECMAScript only, and needs a build with
    /// `--features v8`.
    #[arg(long = "engine", default_value_t = Engine::Llrt)]
    pub(crate) engine: Engine,

    /// Memory in MB for grouped data before it spills to disk, divided between the group shards.
    #[arg(long = "memory-limit", default_value_t = DEFAULT_MEMORY_LIMIT_MB)]
    pub(crate) memory_limit_mb: usize,

    /// Directory for temporary files when grouped data is too large for memory and spills to disk.
    /// Defaults to the working directory.
    #[arg(long = "tmpdir", default_value = ".")]
    pub(crate) tmpdir: PathBuf,

    /// Enable CPU profiling; writes pprof.pb to the working directory on exit.
    #[arg(long = "pprof", action = clap::ArgAction::SetTrue)]
    pub(crate) pprof: bool,
}
