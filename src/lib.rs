mod js;

use futures::stream::StreamExt;
use js::{JobRequest, JobResult};
use std::sync::atomic::AtomicUsize;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::oneshot,
};
use tokio_stream::wrappers::LinesStream;
use tracing::{error, info};

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use tracing::instrument;

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");
const CHUNK_SIZE: usize = 64;

#[derive(Debug, Parser)]
#[command(name = "pulsar")]
#[command(about = "A simple map-reduce engine for parallel processing")]
#[command(author, version)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Input file to read input data from.
    #[arg(short = 'f', default_value = "-")]
    input_file: String,

    /// Output format for the results.
    #[arg(long = "output", default_value_t = OutputFormat::Plain)]
    output_format: OutputFormat,

    /// JavaScript file containing map and reduce functions. If not provided, defaults to a word count script.
    #[arg(short = 's', long = "script")]
    script_file: Option<String>,

    /// Whether to sort the output before printing. Assumes the script has a `sort` function.
    #[arg(long = "sort", action = clap::ArgAction::SetTrue)]
    sort: bool,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Test { files: Option<Vec<String>> },
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
    sort: bool,
    output_format: OutputFormat,
    command: Option<Commands>,
}

impl Pulsar<BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
    /// Create a new Pulsar instance from CLI arguments
    #[instrument(level = "trace")]
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
            script: script.clone(),
            output_format: cli.output_format,
            sort: cli.sort,
            command: cli.command,
        })
    }

    /// Run the application with streaming and optimized processing
    #[instrument(level = "trace")]
    pub async fn run(self) -> Result<()> {
        match &self.command {
            Some(Commands::Test { files }) => self.run_tests(files).await,
            None => self.run_engine().await,
        }
    }

    #[instrument(level = "trace")]
    pub async fn run_tests(&self, files: &Option<Vec<String>>) -> Result<()> {
        if files.is_none() {
            js::run_test_file(self.script.clone())?;
            println!("OK");
            return Ok(());
        }

        for file in files.as_ref().unwrap() {
            print!("{}: ", file);
            let code = tokio::fs::read_to_string(file).await?;
            js::run_test_file(code)?;
            println!("OK");
        }
        Ok(())
    }

    #[instrument(level = "trace")]
    pub async fn run_engine(self) -> Result<()> {
        let n_cpus = num_cpus::get().max(1);
        let mut workers = Vec::with_capacity(n_cpus);
        for _ in 0..n_cpus {
            let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(64);
            workers.push(worker_tx);

            // spawn each worker with its own receiver
            js::start_vm_worker(self.script.clone(), worker_rx);
        }

        // aggregate map results
        let (map_tx, mut map_rx) = tokio::sync::mpsc::channel::<Vec<js::KeyValue>>(64);
        let map_consumer = tokio::spawn(async move {
            let mut groups: HashMap<String, Vec<js::Value>> = HashMap::new();
            while let Some(kvs) = map_rx.recv().await {
                for kv in kvs {
                    groups.entry(kv.key).or_insert_with(Vec::new).push(kv.value);
                }
            }
            groups
        });

        // map phase
        let task_idx = AtomicUsize::new(0);
        LinesStream::new(self.reader.lines())
            .filter_map(
                |r| async move { r.map_err(|e| eprintln!("Error reading line: {}", e)).ok() },
            )
            .chunks(CHUNK_SIZE)
            .for_each_concurrent(n_cpus, |batch| {
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let worker = &workers[idx % workers.len()];
                let map_tx = map_tx.clone();

                async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = worker.send(JobRequest::Map(batch, resp_tx)).await;

                    match resp_rx.await {
                        Ok(JobResult::MapSuccess(output)) => {
                            let _ = map_tx.send(output).await;
                        }
                        Ok(JobResult::Error(e)) => {
                            error!("Error during map: {}", e);
                        }
                        Err(e) => {
                            error!("JS worker error: {}", e);
                        }
                        _ => unreachable!(),
                    };
                }
            })
            .await;

        // group phase
        drop(map_tx);
        let groups = map_consumer.await.unwrap();

        // aggregate reduce results
        let (reduce_tx, mut reduce_rx) = tokio::sync::mpsc::channel(64);
        let reduce_consumer = tokio::spawn({
            let output_format = self.output_format.clone();
            let sort = self.sort;
            let worker = workers[0].clone();
            async move {
                let stdout = tokio::io::stdout();
                let mut writer = BufWriter::new(stdout);

                if sort {
                    let mut results = Vec::new();
                    while let Some(kv) = reduce_rx.recv().await {
                        results.push(kv);
                    }

                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = worker.send(JobRequest::Sort(results, resp_tx)).await;
                    match resp_rx.await {
                        Ok(JobResult::SortSuccess(output)) => {
                            for kv in output {
                                Self::format_and_print_result(
                                    &kv.key,
                                    &kv.value,
                                    &output_format,
                                    &mut writer,
                                )
                                .await;
                            }
                        }
                        _ => error!("Sort error"),
                    }
                } else {
                    while let Some(kv) = reduce_rx.recv().await {
                        Self::format_and_print_result(
                            &kv.key,
                            &kv.value,
                            &output_format,
                            &mut writer,
                        )
                        .await;
                    }
                }

                let _ = writer.flush().await;
            }
        });

        // reduce phase
        tokio_stream::iter(groups)
            .chunks(CHUNK_SIZE)
            .for_each_concurrent(n_cpus, |batch| {
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let worker = &workers[idx % workers.len()];
                let reduce_tx = reduce_tx.clone();

                async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = worker.send(JobRequest::Reduce(batch, resp_tx)).await;

                    match resp_rx.await {
                        Ok(JobResult::ReduceSuccess(value)) => {
                            for kv in value {
                                if let Err(e) = reduce_tx.send(kv).await {
                                    error!("Failed to send reduce result: {}", e);
                                    break;
                                }
                            }
                        }
                        Ok(JobResult::Error(e)) => {
                            error!("Error during reduce: {}", e);
                        }
                        Err(e) => {
                            error!("JS worker error: {}", e);
                        }
                        _ => unreachable!(),
                    };
                }
            })
            .await;

        // write results
        drop(reduce_tx);
        let _ = reduce_consumer.await;

        Ok(())
    }

    /// Format and print a single result
    async fn format_and_print_result(
        key: &str,
        result: &js::Value,
        output_format: &OutputFormat,
        writer: &mut BufWriter<tokio::io::Stdout>,
    ) {
        match output_format {
            OutputFormat::Plain => {
                let _ = writer
                    .write_all(format!("{}: {}\n", key, result.to_string()).as_bytes())
                    .await;
            }
            OutputFormat::Json => {
                let val = serde_json::Value::from(result);
                let _ = writer
                    .write_all(format!("{}\n", serde_json::json!({ key: val })).as_bytes())
                    .await;
            }
        }

        // Use tokio's async version of flush
        let _ = tokio::io::stdout().flush().await;
    }
}

impl<R: AsyncBufReadExt + Unpin> Debug for Pulsar<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar").finish()
    }
}
