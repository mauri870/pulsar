mod js;

use futures::stream::StreamExt;
use js::{JobRequest, JobResult};
use std::sync::atomic::AtomicUsize;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::{mpsc::unbounded_channel, oneshot},
};
use tokio_stream::wrappers::LinesStream;
use tracing::warn;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use thiserror::Error;
use tracing::instrument;

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");
const MAX_CONCURRENCY: Option<usize> = None;

#[derive(Debug, Error)]
pub enum PulsarError {
    #[error("no files or directories to watch")]
    NoFilesToWatch,
}

#[derive(Debug, Parser)]
#[command(name = "pulsar")]
#[command(about = "A simple map-reduce engine for parallel processing")]
#[command(author, version)]
pub struct Cli {
    /// Input file to read input data from.
    #[arg(short = 'f', default_value = "-")]
    input_file: String,

    /// Output format for the results.
    #[arg(long = "output", default_value_t = OutputFormat::Plain)]
    output_format: OutputFormat,

    /// JavaScript file containing map and reduce functions. If not provided, defaults to a word count script.
    #[arg(short = 's', long = "script")]
    script_file: Option<String>,
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
    output_format: OutputFormat,
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
        })
    }

    /// Run the application with streaming and optimized processing
    #[instrument(level = "trace")]
    pub async fn run(self) -> Result<()> {
        let num_workers = num_cpus::get();
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (worker_tx, worker_rx) = unbounded_channel();
            workers.push(worker_tx);

            // spawn each worker with its own receiver
            js::start_vm_worker(self.script.clone(), worker_rx);
        }

        // for map results
        let (map_tx, mut map_rx) = unbounded_channel();

        // map phase
        let task_idx = AtomicUsize::new(0);
        LinesStream::new(self.reader.lines())
            .filter_map(
                |r| async move { r.map_err(|e| eprintln!("Error reading line: {}", e)).ok() },
            )
            .for_each_concurrent(MAX_CONCURRENCY, |line| {
                let input = line.to_string();
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let worker = &workers[idx % workers.len()];
                let map_tx = map_tx.clone();

                async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    if worker.send(JobRequest::Map(input, resp_tx)).is_err() {
                        warn!("Failed to send map job");
                        return;
                    }

                    match resp_rx.await {
                        Ok(JobResult::MapSuccess(output)) => {
                            let _ = map_tx.send(output);
                        }
                        Ok(_) => {
                            warn!("Unexpected response from worker");
                        }
                        Err(e) => {
                            warn!("Worker channel error: {}", e);
                        }
                    };
                }
            })
            .await;

        // group phase
        drop(map_tx);
        let mut groups: HashMap<String, Vec<js::Value>> = HashMap::new();
        while let Some(kvs) = map_rx.recv().await {
            for kv in kvs {
                groups.entry(kv.key).or_insert_with(Vec::new).push(kv.value);
            }
        }

        // reduce phase
        let (reduce_tx, mut reduce_rx) = unbounded_channel();
        tokio_stream::iter(groups)
            .for_each_concurrent(MAX_CONCURRENCY, |(key, values)| {
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let worker = &workers[idx % workers.len()];
                let reduce_tx = reduce_tx.clone();

                async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    if worker
                        .send(JobRequest::Reduce(key.clone(), values, resp_tx))
                        .is_err()
                    {
                        warn!("Failed to send reduce job");
                        return;
                    }

                    match resp_rx.await {
                        Ok(JobResult::ReduceSuccess(value)) => {
                            let _ = reduce_tx.send((key, value));
                        }
                        Ok(_) => {
                            warn!("Unexpected response from worker");
                        }
                        Err(e) => {
                            warn!("Worker channel error: {}", e);
                        }
                    };
                }
            })
            .await;

        // write results
        drop(reduce_tx);
        let stdout = tokio::io::stdout();
        let mut writer = BufWriter::new(stdout);
        while let Some((key, value)) = reduce_rx.recv().await {
            Self::format_and_print_result(&key, &value, &self.output_format, &mut writer).await;
        }
        let _ = writer.flush().await;

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
