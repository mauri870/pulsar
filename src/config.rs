use crate::{cli::Cli, output::OutputFormat};

/// Where the input lines come from.
#[derive(Debug, Clone)]
pub enum InputSource {
    Stdin,
    File(String),
}

/// Where the user script comes from.
#[derive(Debug, Clone)]
pub enum ScriptSource {
    /// The built-in word count script.
    Default,
    File(String),
}

/// Validated run settings. Building one performs no I/O.
#[derive(Debug, Clone)]
pub struct Config {
    pub input: InputSource,
    pub script: ScriptSource,
    pub output_format: OutputFormat,
    pub sort: bool,
    pub test: bool,
    pub workers: usize,
    pub chunk_size: usize,
    pub pprof: bool,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        Config {
            input: if cli.input_file == "-" {
                InputSource::Stdin
            } else {
                InputSource::File(cli.input_file)
            },
            script: match cli.script_file {
                Some(file) => ScriptSource::File(file),
                None => ScriptSource::Default,
            },
            output_format: cli.output_format,
            sort: cli.sort,
            test: cli.test,
            workers: cli.workers.unwrap_or_else(num_cpus::get_physical).max(1),
            chunk_size: cli.chunk_size.max(1),
            pprof: cli.pprof,
        }
    }
}
