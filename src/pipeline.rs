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
use crate::runtime::{Engine, LlrtRuntime, ScriptRuntime, WorkerPool};
use crate::profile;
use crate::script::{self, Contract};
use crate::store::{DEFAULT_MEMORY_LIMIT_BYTES, GroupStore, SpillStore};
use crate::value::{KeyValue, Value};

#[cfg(test)]
mod tests;

/// How many ways the grouping is split by key. Grouping one pair costs about
/// as much as a light map function, so a single group stage is the limit of a
/// run with many workers; a few shards are enough to stay ahead of them.
const MAX_GROUP_SHARDS: usize = 4;

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
/// results to `sink`. The grouping is split by key over `stores`, one group
/// stage each, so that it does not become the one serial part of a run.
pub(crate) async fn run_pipeline<R: ScriptRuntime + 'static>(
    script: &str,
    settings: &Settings,
    source: impl Source,
    sink: impl Sink,
    stores: Vec<impl GroupStore>,
) -> Result<()> {
    info!("Starting pulsar engine with {} CPU workers", settings.workers);
    let contract = Contract {
        sort: settings.sort,
        test: false,
    };
    let pool = Arc::new(WorkerPool::start::<R>(script, contract, settings.workers).await?);
    info!("Successfully started {} JS VM workers", settings.workers);

    let mut map_results = Vec::with_capacity(stores.len());
    let mut groups = Vec::with_capacity(stores.len());
    for store in stores {
        let (sender, receiver) = mpsc::channel(settings.workers * 2);
        map_results.push(sender);
        groups.push(Task::spawn(group_stage(receiver, store)));
    }
    let (reduce_results, reduce_receiver) = mpsc::channel(settings.chunk_size);
    let output = Task::spawn(output_stage(
        reduce_receiver,
        Arc::clone(&pool),
        settings.sort,
        sink,
    ));

    info!("Starting map phase");
    map_stage(source, &pool, settings, map_results).await?;

    info!("Map phase completed, starting group phase");
    let mut stores = Vec::with_capacity(groups.len());
    for group in groups {
        stores.push(group.join().await?);
    }
    info!(
        "Group phase completed, {} total keys",
        stores.iter().map(|store| store.key_count()).sum::<usize>()
    );

    info!("Starting reduce phase");
    if let Err(reduce_error) = reduce_stage(stores, &pool, settings, reduce_results).await {
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
/// and forwards the pairs each batch produced to the group stage of the shard
/// they belong to (`results` has one sender per shard). Returns when the
/// input is exhausted and every batch is done, or at the first failure.
async fn map_stage(
    source: impl Source,
    pool: &WorkerPool,
    settings: &Settings,
    results: Vec<mpsc::Sender<Vec<KeyValue>>>,
) -> Result<()> {
    let shards = results.len();
    let mut lines = source.into_lines();
    let mut batch = Vec::with_capacity(settings.chunk_size);
    let mut in_flight = FuturesUnordered::new();
    let max_in_flight = settings.workers * 2;
    let mut input_done = false;

    while !input_done || !in_flight.is_empty() {
        tokio::select! {
            Some(result) = in_flight.next(), if !in_flight.is_empty() => {
                let parts: Vec<Vec<KeyValue>> = result?;
                for (sender, pairs) in results.iter().zip(parts) {
                    if pairs.is_empty() {
                        continue;
                    }
                    sender
                        .send(pairs)
                        .await
                        .map_err(|_| Error::Worker("the group stage stopped".into()))?;
                }
            }
            line = lines.next(), if !input_done && in_flight.len() < max_in_flight => match line {
                Some(Ok(line)) => {
                    batch.push(line);
                    if batch.len() == settings.chunk_size {
                        in_flight.push(pool.map_batch(std::mem::take(&mut batch), shards));
                    }
                }
                Some(Err(e)) => return Err(Error::ReadInput(e)),
                None => {
                    input_done = true;
                    if !batch.is_empty() {
                        in_flight.push(pool.map_batch(std::mem::take(&mut batch), shards));
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
/// the reduced pairs. The store hands out its groups lazily, so only the
/// batches being reduced are in memory besides the store itself. Returns at
/// the first failure.
async fn reduce_stage(
    stores: Vec<impl GroupStore>,
    pool: &Arc<WorkerPool>,
    settings: &Settings,
    results: mpsc::Sender<KeyValue>,
) -> Result<()> {
    let task_index = AtomicUsize::new(0);
    let batch_size = settings.chunk_size;
    futures::stream::iter(
        stores
            .into_iter()
            .flat_map(move |store| store.into_batches(batch_size)),
    )
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
    engine: Engine,
    tmpdir: PathBuf,
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
            engine: config.engine,
            tmpdir: config.tmpdir,
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

        let shards = self.settings.workers.clamp(1, MAX_GROUP_SHARDS);
        let stores = (0..shards)
            .map(|shard| {
                SpillStore::new(
                    self.tmpdir
                        .join(format!("pulsar-groups-{}-{}", std::process::id(), shard)),
                    DEFAULT_MEMORY_LIMIT_BYTES / shards,
                )
            })
            .collect();
        let sink = StdoutSink::new(self.output_format);
        match self.engine {
            Engine::Llrt => {
                run_pipeline::<LlrtRuntime>(&self.script, &self.settings, self.source, sink, stores)
                    .await?
            }
            #[cfg(feature = "v8")]
            Engine::V8 => {
                run_pipeline::<crate::runtime::V8Runtime>(
                    &self.script,
                    &self.settings,
                    self.source,
                    sink,
                    stores,
                )
                .await?
            }
            #[cfg(not(feature = "v8"))]
            Engine::V8 => return Err(v8_unavailable()),
        }

        if let Some(profiler) = self.profiler {
            profiler.write_report()?;
        }
        Ok(())
    }

    #[instrument(level = "trace")]
    async fn run_tests(&self) -> Result<()> {
        match self.engine {
            Engine::Llrt => run_script_tests::<LlrtRuntime>(&self.script).await?,
            #[cfg(feature = "v8")]
            Engine::V8 => run_script_tests::<crate::runtime::V8Runtime>(&self.script).await?,
            #[cfg(not(feature = "v8"))]
            Engine::V8 => return Err(v8_unavailable()),
        }
        println!("OK");
        Ok(())
    }
}

/// Runs the script's `test` function in a runtime of type `R`.
async fn run_script_tests<R: ScriptRuntime + 'static>(script: &str) -> Result<()> {
    let contract = Contract {
        sort: false,
        test: true,
    };
    let pool = WorkerPool::start::<R>(script, contract, 1).await?;
    pool.test().await?;
    pool.shutdown()
}

#[cfg(not(feature = "v8"))]
fn v8_unavailable() -> Error {
    Error::Unsupported(
        "the v8 engine is not included in this build; rebuild with `cargo build --release --features v8`"
            .into(),
    )
}

impl Debug for Pulsar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar").finish()
    }
}
