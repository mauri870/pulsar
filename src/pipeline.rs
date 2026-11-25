//! The engine: map, group, reduce and output as separate stages connected by
//! channels. Each stage is a function with typed input and output, and the
//! whole pipeline is generic over the script runtime, the input source and the
//! output sink.

use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use std::path::PathBuf;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, info, instrument};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::input::{self, ReaderSource, Source};
use crate::output::{OutputFormat, Sink, StdoutSink};
use crate::runtime::{LlrtRuntime, ScriptRuntime, WorkerPool};
use crate::profile;
use crate::script::{self, Contract};
use crate::store::{DEFAULT_MEMORY_LIMIT_BYTES, GroupStore, SpillStore};
use crate::value::{KeyValue, Value};

#[cfg(test)]
mod tests;

/// The settings the stages need.
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub workers: usize,
    pub chunk_size: usize,
    pub sort: bool,
}

/// A spawned stage that is aborted if it is dropped before it is joined, so
/// that returning early with an error never leaves a stage running.
struct Task<T>(JoinHandle<Result<T>>);

impl<T: Send + 'static> Task<T> {
    fn spawn(future: impl Future<Output = Result<T>> + Send + 'static) -> Self {
        Task(tokio::spawn(future))
    }

    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }

    async fn join(mut self) -> Result<T> {
        (&mut self.0).await.map_err(task_failed)?
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn task_failed(error: JoinError) -> Error {
    Error::Worker(format!("a pipeline task failed: {}", error))
}

/// Runs the whole pipeline: read `source`, map, group, reduce, and write the
/// results to `sink`.
pub(crate) async fn run_pipeline<R: ScriptRuntime + 'static>(
    script: &str,
    settings: &Settings,
    source: impl Source,
    sink: impl Sink,
    store: impl GroupStore,
) -> Result<()> {
    info!("Starting pulsar engine with {} CPU workers", settings.workers);
    let contract = Contract {
        sort: settings.sort,
        test: false,
    };
    let pool = Arc::new(WorkerPool::start::<R>(script, contract, settings.workers).await?);
    info!("Successfully started {} JS VM workers", settings.workers);

    let (map_results, map_receiver) = mpsc::channel(settings.workers * 2);
    let (reduce_results, reduce_receiver) = mpsc::channel(settings.chunk_size);
    let group = Task::spawn(group_stage(map_receiver, store));
    let output = Task::spawn(output_stage(
        reduce_receiver,
        Arc::clone(&pool),
        settings.sort,
        sink,
    ));

    info!("Starting map phase");
    map_stage(source, &pool, settings, map_results).await?;

    info!("Map phase completed, starting group phase");
    let store = group.join().await?;
    info!("Group phase completed, {} total keys", store.key_count());

    info!("Starting reduce phase");
    if let Err(reduce_error) = reduce_stage(store, &pool, settings, reduce_results).await {
        // A failed writer is the root cause of a failed send, so report it first.
        if output.is_finished() {
            output.join().await?;
        }
        return Err(reduce_error);
    }

    info!("Reduce phase completed, waiting for output");
    output.join().await?;
    if let Some(pool) = Arc::into_inner(pool) {
        pool.shutdown()?;
    }
    info!("Pulsar processing completed successfully");
    Ok(())
}

/// Sends batches of lines to the workers, a few batches per worker at a time,
/// and forwards the pairs each batch produced. Returns when the input is
/// exhausted and every batch is done, or at the first failure.
async fn map_stage(
    source: impl Source,
    pool: &WorkerPool,
    settings: &Settings,
    results: mpsc::Sender<Vec<KeyValue>>,
) -> Result<()> {
    let mut lines = source.into_lines();
    let mut batch = Vec::with_capacity(settings.chunk_size);
    let mut in_flight = FuturesUnordered::new();
    let max_in_flight = settings.workers * 2;
    let mut input_done = false;

    while !input_done || !in_flight.is_empty() {
        tokio::select! {
            Some(result) = in_flight.next(), if !in_flight.is_empty() => {
                let pairs = result?;
                results
                    .send(pairs)
                    .await
                    .map_err(|_| Error::Worker("the group stage stopped".into()))?;
            }
            line = lines.next(), if !input_done && in_flight.len() < max_in_flight => match line {
                Some(Ok(line)) => {
                    batch.push(line);
                    if batch.len() == settings.chunk_size {
                        in_flight.push(pool.map_batch(std::mem::take(&mut batch)));
                    }
                }
                Some(Err(e)) => return Err(Error::ReadInput(e)),
                None => {
                    input_done = true;
                    if !batch.is_empty() {
                        in_flight.push(pool.map_batch(std::mem::take(&mut batch)));
                    }
                }
            },
        }
    }
    Ok(())
}

/// Groups the mapped pairs by key in the store. Returns the store when the
/// map stage drops its sender.
async fn group_stage<S: GroupStore>(
    mut pairs: mpsc::Receiver<Vec<KeyValue>>,
    mut store: S,
) -> Result<S> {
    let mut total_processed = 0;
    while let Some(kvs) = pairs.recv().await {
        total_processed += kvs.len();
        store.add(kvs)?;
    }
    store.finish()?;
    info!(
        "Group phase completed, total key-value pairs processed: {}",
        total_processed
    );
    Ok(store)
}

/// Reduces every group in batches, several batches at a time, and forwards
/// the reduced pairs. Returns at the first failure.
async fn reduce_stage(
    store: impl GroupStore,
    pool: &Arc<WorkerPool>,
    settings: &Settings,
    results: mpsc::Sender<KeyValue>,
) -> Result<()> {
    let entries = store.into_groups()?;

    let task_index = AtomicUsize::new(0);
    tokio_stream::iter(entries)
        .chunks(settings.chunk_size)
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(settings.workers, |batch: Vec<(String, Vec<Value>)>| {
            let index = task_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let pool = Arc::clone(pool);
            let results = results.clone();

            async move {
                let reduced = pool.reduce_batch(batch).await?;
                debug!("Reduce task {} completed with {} results", index, reduced.len());
                for kv in reduced {
                    results
                        .send(kv)
                        .await
                        .map_err(|_| Error::Worker("the output writer stopped".into()))?;
                }
                Ok(())
            }
        })
        .await
}

/// Writes the reduced results to the sink, sorting them first when asked to.
/// Returns when the reduce stage drops its sender.
async fn output_stage(
    mut results: mpsc::Receiver<KeyValue>,
    pool: Arc<WorkerPool>,
    sort: bool,
    mut sink: impl Sink,
) -> Result<()> {
    info!("Starting output writer task");
    let mut result_count = 0;

    if sort {
        info!("Collecting results for sorting");
        let mut collected = Vec::new();
        while let Some(kv) = results.recv().await {
            collected.push(kv);
            result_count += 1;
        }
        info!(
            "Collected {} results, starting sort operation",
            result_count
        );

        let sorted = pool.sort(collected).await?;
        info!(
            "Sort operation completed, writing {} sorted results",
            sorted.len()
        );
        for kv in sorted {
            sink.write(&kv.key, &kv.value).await?;
        }
    } else {
        info!("Writing results without sorting");
        while let Some(kv) = results.recv().await {
            result_count += 1;
            sink.write(&kv.key, &kv.value).await?;
        }
        info!("Wrote {} results to output", result_count);
    }

    sink.finish().await
}

pub struct Pulsar {
    source: ReaderSource,
    script: String,
    settings: Settings,
    output_format: OutputFormat,
    test: bool,
    profiler: Option<profile::Profiler>,
}

impl Pulsar {
    /// Create a new Pulsar instance from a configuration, opening the input
    /// and reading the script.
    #[instrument(level = "trace")]
    pub async fn from_config(config: Config) -> Result<Self> {
        let source = input::open(&config.input).await?;
        let script = script::load(&config.script).await?;
        Ok(Pulsar {
            source,
            script,
            settings: Settings {
                workers: config.workers,
                chunk_size: config.chunk_size,
                sort: config.sort,
            },
            output_format: config.output_format,
            test: config.test,
            profiler: if config.pprof {
                Some(profile::Profiler::start())
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

        run_pipeline::<LlrtRuntime>(
            &self.script,
            &self.settings,
            self.source,
            StdoutSink::new(self.output_format),
            SpillStore::new(PathBuf::from("pulsar_groups"), DEFAULT_MEMORY_LIMIT_BYTES),
        )
        .await?;

        if let Some(profiler) = self.profiler {
            profiler.write_report()?;
        }
        Ok(())
    }

    #[instrument(level = "trace")]
    async fn run_tests(&self) -> Result<()> {
        let contract = Contract {
            sort: false,
            test: true,
        };
        let pool = WorkerPool::start::<LlrtRuntime>(&self.script, contract, 1).await?;
        pool.test().await?;
        pool.shutdown()?;
        println!("OK");
        Ok(())
    }
}

impl Debug for Pulsar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar").finish()
    }
}
