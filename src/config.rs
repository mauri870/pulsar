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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn config(arguments: &[&str]) -> Config {
        let cli = Cli::try_parse_from(std::iter::once("pulsar").chain(arguments.iter().copied()))
            .unwrap();
        Config::from(cli)
    }

    #[test]
    fn defaults() {
        let config = config(&[]);
        assert!(matches!(config.input, InputSource::Stdin));
        assert!(matches!(config.script, ScriptSource::Default));
        assert!(matches!(config.output_format, OutputFormat::Plain));
        assert!(!config.sort && !config.test && !config.pprof);
        assert!(config.workers >= 1);
        assert_eq!(config.chunk_size, 64);
    }

    #[test]
    fn explicit_settings() {
        let config = config(&[
            "-f", "in.txt", "-s", "script.js", "--sort", "--test", "-j", "3", "-c", "5",
            "--output=json", "--pprof",
        ]);
        assert!(matches!(&config.input, InputSource::File(path) if path == "in.txt"));
        assert!(matches!(&config.script, ScriptSource::File(path) if path == "script.js"));
        assert!(matches!(config.output_format, OutputFormat::Json));
        assert!(config.sort && config.test && config.pprof);
        assert_eq!(config.workers, 3);
        assert_eq!(config.chunk_size, 5);
    }

    #[test]
    fn a_dash_means_stdin() {
        assert!(matches!(config(&["-f", "-"]).input, InputSource::Stdin));
    }

    #[test]
    fn worker_count_and_chunk_size_are_at_least_one() {
        let config = config(&["-j", "0", "-c", "0"]);
        assert_eq!(config.workers, 1);
        assert_eq!(config.chunk_size, 1);
    }

    #[test]
    fn an_unknown_output_format_is_rejected() {
        assert!(Cli::try_parse_from(["pulsar", "--output=xml"]).is_err());
    }
}
