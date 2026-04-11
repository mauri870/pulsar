mod js;

use bincode::{deserialize, serialize};
use futures::stream::StreamExt;
use js::{JobRequest, JobResult};
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, error, info};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use std::fmt::{Debug, Display};
use tracing::instrument;

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");
const CHUNK_SIZE: usize = 64;

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

    /// Whether to sort the output before printing. Assumes the script has a `sort` function.
    #[arg(long = "sort", action = clap::ArgAction::SetTrue)]
    sort: bool,

    /// Number of parallel JS VM workers. Defaults to the number of logical CPUs.
    #[arg(short = 'j', long = "workers")]
    workers: Option<usize>,

    /// Run in test mode, executing the script against test cases.
    #[arg(long = "test", action = clap::ArgAction::SetTrue)]
    test: bool,

    /// Enable CPU profiling; writes pprof.pb to the working directory on exit.
    #[arg(long = "pprof", action = clap::ArgAction::SetTrue)]
    pub pprof: bool,
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
    test: bool,
    workers: usize,
    pprof_guard: Option<pprof2::ProfilerGuard<'static>>,
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
        let workers = cli.workers.unwrap_or_else(num_cpus::get_physical).max(1);
        Ok(Pulsar {
            reader,
            script: script.clone(),
            output_format: cli.output_format,
            sort: cli.sort,
            test: cli.test,
            workers,
            pprof_guard: if cli.pprof {
                Some(pprof2::ProfilerGuard::new(999).unwrap())
            } else {
                None
            },
        })
    }

    /// Run the application with streaming and optimized processing
    #[instrument(level = "trace")]
    pub async fn run(self) -> Result<()> {
        if self.test {
            return self.run_tests().await;
        }

        self.run_engine().await?;
        Ok(())
    }

    #[instrument(level = "trace")]
    pub async fn run_tests(&self) -> Result<()> {
        js::run_test_file(self.script.clone())?;
        println!("OK");
        Ok(())
    }

    #[instrument(level = "trace")]
    pub async fn run_engine(self) -> Result<()> {
        let n_cpus = self.workers;
        info!("Starting pulsar engine with {} CPU workers", n_cpus);
        let (worker_tx, worker_rx) = flume::bounded(64);
        let mut init_rxs = Vec::with_capacity(n_cpus);

        for idx in 0..n_cpus {
            let (init_tx, init_rx) = oneshot::channel();
            init_rxs.push(init_rx);
            if let Err(e) = js::start_vm_worker(self.script.clone(), worker_rx.clone(), init_tx) {
                error!("Failed to start JS VM worker {}: {}", idx, e);
                return Err(e.into());
            }
        }
        // Drop the original so the channel closes when all workers exit
        drop(worker_rx);

        // Wait for all workers to finish JS initialisation before proceeding.
        // This adds a startup delay but ensures we don't get trapped in a deadlock.
        for init_rx in init_rxs {
            match init_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return Ok(()),
            }
        }
        info!("Successfully started {} JS VM workers", n_cpus);

        // aggregate map results
        let (map_tx, mut map_rx) = tokio::sync::mpsc::channel::<Vec<js::KeyValue>>(64);
        let map_consumer: JoinHandle<Result<sled::Db>> = tokio::spawn(async move {
            let groups_db = sled::Config::default()
                .path("pulsar_groups")
                .temporary(true)
                .cache_capacity(64 * 1024 * 1024) // 64MB
                .open()?;

            let mut hashmap: HashMap<String, Vec<js::Value>> = HashMap::new();
            const FLUSH_THRESHOLD: usize = 10_000;
            let mut total_processed = 0;

            while let Some(kvs) = map_rx.recv().await {
                let batch_size = kvs.len();
                total_processed += batch_size;
                for kv in kvs {
                    let _ = hashmap
                        .entry(kv.key.clone())
                        .or_insert_with(Vec::new)
                        .push(kv.value);
                }

                if hashmap.len() >= FLUSH_THRESHOLD {
                    info!(
                        "Flushing {} entries to DB, total processed: {}",
                        hashmap.len(),
                        total_processed
                    );
                    // Batch insert
                    let mut batch = sled::Batch::default();
                    for (key, mut values) in hashmap.drain() {
                        // Merge with existing values
                        let mut all_values = match groups_db.get(&key)? {
                            Some(raw_bytes) => {
                                deserialize::<Vec<js::Value>>(&raw_bytes).unwrap_or_else(|e| {
                                    error!(
                                        "Failed to deserialize existing DB value for key '{:?}': {}. Discarding corrupted data.",
                                        &key,
                                        e
                                    );
                                    Vec::new()
                                })
                            }
                            None => Vec::new(),
                        };
                        all_values.append(&mut values);
                        let updated_value_bytes = serialize(&all_values)?;
                        batch.insert(key.as_bytes(), updated_value_bytes);
                    }
                    groups_db.apply_batch(batch)?;
                }
            }

            // Flush any remaining entries
            if !hashmap.is_empty() {
                info!(
                    "Final flush of {} entries to DB, total processed: {}",
                    hashmap.len(),
                    total_processed
                );
                let mut batch = sled::Batch::default();
                for (key, mut values) in hashmap.drain() {
                    let mut all_values = match groups_db.get(&key)? {
                        Some(raw_bytes) => {
                            deserialize::<Vec<js::Value>>(&raw_bytes).unwrap_or_else(|e| {
                                error!(
                                    "Failed to deserialize existing DB value for key '{:?}': {}. Discarding corrupted data.",
                                    &key,
                                    e
                                );
                                Vec::new()
                            })
                        }
                        None => Vec::new(),
                    };
                    all_values.append(&mut values);
                    let updated_value_bytes = serialize(&all_values)?;
                    batch.insert(key.as_bytes(), updated_value_bytes);
                }
                groups_db.apply_batch(batch)?;
            }
            info!(
                "Group phase completed, total key-value pairs processed: {}",
                total_processed
            );
            Ok(groups_db)
        });

        // map phase
        info!("Starting map phase");
        let task_idx = AtomicUsize::new(0);
        LinesStream::new(self.reader.lines())
            .filter_map(
                |r| async move { r.map_err(|e| eprintln!("Error reading line: {}", e)).ok() },
            )
            .chunks(CHUNK_SIZE)
            .for_each_concurrent(n_cpus, |batch| {
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let worker_tx = worker_tx.clone();
                let map_tx = map_tx.clone();

                async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = worker_tx.send_async(JobRequest::Map(batch, resp_tx)).await;

                    match resp_rx.await {
                        Ok(JobResult::MapSuccess(output)) => {
                            tracing::debug!(
                                "Map task {} completed with {} results",
                                idx,
                                output.len()
                            );
                            let _ = map_tx.send(output).await;
                        }
                        Ok(JobResult::Error(e)) => {
                            error!("Error during map task {}: {}", idx, e);
                        }
                        Err(e) => {
                            error!("JS worker error in task {}: {}", idx, e);
                        }
                        _ => unreachable!(),
                    };
                }
            })
            .await;

        // group phase
        info!("Map phase completed, starting group phase");
        drop(map_tx);
        let groups = map_consumer.await?.unwrap();
        info!(
            "Group phase completed, {} total keys in database",
            groups.len()
        );

        // aggregate reduce results
        info!("Starting reduce result aggregation phase");
        let (reduce_tx, mut reduce_rx) = tokio::sync::mpsc::channel(64);
        let reduce_consumer = tokio::spawn({
            let output_format = self.output_format.clone();
            let sort = self.sort;
            let worker_tx = worker_tx.clone();
            async move {
                info!("Starting output writer task");
                let stdout = tokio::io::stdout();
                let mut writer = BufWriter::new(stdout);
                let mut result_count = 0;

                if sort {
                    info!("Collecting results for sorting");
                    let mut results = Vec::new();
                    while let Some(kv) = reduce_rx.recv().await {
                        results.push(kv);
                        result_count += 1;
                    }
                    info!(
                        "Collected {} results, starting sort operation",
                        result_count
                    );

                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = worker_tx.send_async(JobRequest::Sort(results, resp_tx)).await;
                    match resp_rx.await {
                        Ok(JobResult::SortSuccess(output)) => {
                            info!(
                                "Sort operation completed, writing {} sorted results",
                                output.len()
                            );
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
                    info!("Writing results without sorting");
                    while let Some(kv) = reduce_rx.recv().await {
                        result_count += 1;
                        Self::format_and_print_result(
                            &kv.key,
                            &kv.value,
                            &output_format,
                            &mut writer,
                        )
                        .await;
                    }
                    info!("Wrote {} results to output", result_count);
                }

                let _ = writer.flush().await;
            }
        });

        // reduce phase
        info!("Starting reduce phase");
        tokio_stream::iter(groups.iter()
            .filter_map(|res| {
                match res {
                    Ok((key, value)) => Some((key, value)),
                    Err(e) => {
                        eprintln!("Error iterating sled db: {}", e);
                        None // Skip entries that cause an error
                    }
                }
            })
            .map(|(k, value_bytes)| {
                let key: String = String::from_utf8_lossy(&k).to_string();
                let values: Vec<js::Value> = deserialize(&value_bytes).unwrap_or_else(|e| {
                    error!(
                        "Failed to deserialize DB value for key '{:?}': {}. Discarding corrupted data.",
                        &k,
                        e
                    );
                    Vec::new()
                });
                (key, values)
        }))
        .chunks(CHUNK_SIZE)
        .for_each_concurrent(n_cpus, |batch: Vec<(String, Vec<js::Value>)>| {
            let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let worker_tx = worker_tx.clone();
            let reduce_tx = reduce_tx.clone();

            async move {
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = worker_tx.send_async(JobRequest::Reduce(batch, resp_tx)).await;

                match resp_rx.await {
                    Ok(JobResult::ReduceSuccess(value)) => {
                        debug!("Reduce task {} completed with {} results", idx, value.len());
                        for kv in value {
                            if let Err(e) = reduce_tx.send(kv).await {
                                error!("Failed to send reduce result: {}", e);
                                break;
                            }
                        }
                    }
                    Ok(JobResult::Error(e)) => {
                        error!("Error during reduce task {}: {}", idx, e);
                    }
                    Err(e) => {
                        error!("JS worker error in reduce task {}: {}", idx, e);
                    }
                    _ => unreachable!(),
                };
            }
        })
        .await;

        // write results
        info!("Reduce phase completed, waiting for output");
        drop(reduce_tx);
        let _ = reduce_consumer.await;
        info!("Pulsar processing completed successfully");

        if let Some(guard) = self.pprof_guard {
            if let Ok(report) = guard.report().build() {
                use pprof2::protos::Message;
                use std::io::Write;
                let profile = report.pprof()?;
                let mut content = Vec::new();
                profile.encode(&mut content)?;
                let mut file = std::fs::File::create("pprof.pb")?;
                file.write_all(&content)?;
            }
        }

        Ok(())
    }

    /// Format and print a single result
    #[instrument(level = "trace", skip(writer))]
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
