use bincode::{deserialize, serialize};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use crate::js::JobRequest;
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufWriter},
    sync::oneshot,
    task::{JoinError, JoinHandle},
};
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, info};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::input::InputReader;
use crate::output::OutputFormat;
use crate::{input, js, output, profile, script};
use std::fmt::Debug;
use tracing::instrument;

fn worker_gone<T>(_: flume::SendError<T>) -> Error {
    Error::Worker("all JS VM workers have exited".into())
}

fn task_failed(error: JoinError) -> Error {
    Error::Worker(format!("a pipeline task failed: {}", error))
}

fn merge_values(_key: &[u8], old_value: Option<&[u8]>, new_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut values: Vec<js::Value> = old_value
        .and_then(|bytes| deserialize(bytes).ok())
        .unwrap_or_default();
    let new_values: Vec<js::Value> = deserialize(new_bytes).unwrap_or_default();
    values.extend(new_values);
    serialize(&values).ok()
}

const MAX_GROUPING_HEAP_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB

const HASHMAP_SLOT_SIZE: usize = {
    let size = std::mem::size_of::<(String, Vec<js::Value>)>();
    let align = {
        let a = std::mem::align_of::<(String, Vec<js::Value>)>();
        let b = std::mem::align_of::<usize>();
        if a > b { a } else { b }
    };
    (size / align) * align
};

enum GroupStorage {
    Memory(HashMap<String, Vec<js::Value>>),
    Sled(sled::Db),
}

pub struct Pulsar<R: AsyncBufReadExt + Unpin> {
    reader: R,
    script: String,
    sort: bool,
    output_format: OutputFormat,
    test: bool,
    workers: usize,
    chunk_size: usize,
    profiler: Option<profile::Profiler>,
}

impl Pulsar<InputReader> {
    /// Create a new Pulsar instance from a configuration, opening the input
    /// and reading the script.
    #[instrument(level = "trace")]
    pub async fn from_config(config: Config) -> Result<Self> {
        let reader = input::open(&config.input).await?;
        let script = script::load(&config.script).await?;
        Ok(Pulsar {
            reader,
            script,
            output_format: config.output_format,
            sort: config.sort,
            test: config.test,
            workers: config.workers,
            chunk_size: config.chunk_size,
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
        let (worker_tx, worker_rx) = flume::bounded(self.chunk_size);
        let mut init_rxs = Vec::with_capacity(n_cpus);

        for idx in 0..n_cpus {
            let (init_tx, init_rx) = oneshot::channel();
            init_rxs.push(init_rx);
            if let Err(e) = js::start_vm_worker(self.script.clone(), worker_rx.clone(), init_tx) {
                return Err(Error::Worker(format!(
                    "Failed to start JS VM worker {}: {}",
                    idx, e
                )));
            }
        }
        // Drop the original so the channel closes when all workers exit
        drop(worker_rx);

        // Wait for all workers to finish JS initialisation before proceeding.
        // This adds a startup delay but ensures we don't get trapped in a deadlock.
        for init_rx in init_rxs {
            match init_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(Error::Worker(
                        "a JS VM worker exited during initialization".into(),
                    ));
                }
            }
        }
        info!("Successfully started {} JS VM workers", n_cpus);

        // aggregate map results — workers send one Vec<KeyValue> per input line
        let (map_result_tx, map_result_rx) =
            tokio::sync::mpsc::channel::<Vec<js::KeyValue>>(n_cpus * self.chunk_size);
        let map_consumer: JoinHandle<Result<GroupStorage>> = tokio::spawn(async move {
            let mut map_result_rx = map_result_rx;
            let mut overflow_db: Option<sled::Db> = None;
            let mut hashmap: HashMap<String, Vec<js::Value>> = HashMap::new();
            let mut total_processed = 0;

            while let Some(kvs) = map_result_rx.recv().await {
                total_processed += kvs.len();
                for kv in kvs {
                    hashmap.entry(kv.key).or_insert_with(Vec::new).push(kv.value);
                }

                // check if we need to spill to disk
                if HASHMAP_SLOT_SIZE * hashmap.capacity() >= MAX_GROUPING_HEAP_BYTES {
                    info!(
                        "Flushing {} entries to DB, total processed: {}",
                        hashmap.len(),
                        total_processed
                    );
                    let db = match overflow_db {
                        Some(ref db) => db,
                        None => {
                            let db = sled::Config::default()
                                .path("pulsar_groups")
                                .temporary(true)
                                .open()?;
                            db.set_merge_operator(merge_values);
                            overflow_db = Some(db);
                            overflow_db.as_ref().unwrap()
                        }
                    };
                    for (key, values) in hashmap.drain() {
                        let values_bytes = serialize(&values)?;
                        db.merge(key.as_bytes(), values_bytes)?;
                    }
                }
            }

            info!(
                "Group phase completed, total key-value pairs processed: {}",
                total_processed
            );

            if let Some(db) = overflow_db {
                if !hashmap.is_empty() {
                    info!("Final flush of {} remaining entries to DB", hashmap.len());
                    for (key, values) in hashmap.drain() {
                        let values_bytes = serialize(&values)?;
                        db.merge(key.as_bytes(), values_bytes)?;
                    }
                }
                Ok(GroupStorage::Sled(db))
            } else {
                Ok(GroupStorage::Memory(hashmap))
            }
        });

        // map phase: dispatch RunMapPhase to each worker, then stream lines into the shared channel
        info!("Starting map phase");
        let (map_item_tx, map_item_rx) = flume::bounded::<String>(n_cpus * self.chunk_size);

        let mut map_done = FuturesUnordered::new();
        for _ in 0..n_cpus {
            let (done_tx, done_rx) = oneshot::channel::<Result<()>>();
            map_done.push(async move {
                match done_rx.await {
                    Ok(result) => result,
                    Err(_) => Err(Error::Worker(
                        "a JS VM worker exited during the map phase".into(),
                    )),
                }
            });
            worker_tx
                .send_async(JobRequest::RunMapPhase {
                    item_rx: map_item_rx.clone(),
                    result_tx: map_result_tx.clone(),
                    concurrency: self.chunk_size,
                    done_tx,
                })
                .await
                .map_err(worker_gone)?;
        }
        drop(map_item_rx);
        drop(map_result_tx); // workers hold the remaining Sender clones

        // Feed the lines to the workers, and stop as soon as a worker reports a failure.
        let mut lines = LinesStream::new(self.reader.lines());
        let mut map_failure: Option<Error> = None;
        loop {
            tokio::select! {
                Some(result) = map_done.next() => {
                    if let Err(e) = result {
                        map_failure = Some(e);
                        break;
                    }
                }
                line = lines.next() => match line {
                    Some(Ok(line)) => {
                        if map_item_tx.send_async(line).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        map_failure = Some(Error::ReadInput(e));
                        break;
                    }
                    None => break,
                },
            }
        }
        drop(map_item_tx); // closing the channel signals workers: no more items
        if let Some(e) = map_failure {
            return Err(e);
        }

        // Wait for all map workers to finish and drop their result_tx clones;
        // once they do, map_result_rx closes and map_consumer terminates.
        while let Some(result) = map_done.next().await {
            result?;
        }

        // group phase
        info!("Map phase completed, starting group phase");
        let groups = map_consumer.await.map_err(task_failed)??;
        info!(
            "Group phase completed, {} total keys",
            match &groups {
                GroupStorage::Memory(m) => m.len(),
                GroupStorage::Sled(db) => db.len(),
            }
        );

        // aggregate reduce results
        info!("Starting reduce result aggregation phase");
        let (reduce_tx, mut reduce_rx) = tokio::sync::mpsc::channel(self.chunk_size);
        let reduce_consumer: JoinHandle<Result<()>> = tokio::spawn({
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
                    worker_tx
                        .send_async(JobRequest::Sort(results, resp_tx))
                        .await
                        .map_err(worker_gone)?;
                    let output = resp_rx.await.map_err(|_| {
                        Error::Worker("a JS VM worker exited during sorting".into())
                    })??;
                    info!(
                        "Sort operation completed, writing {} sorted results",
                        output.len()
                    );
                    for kv in output {
                        output::write_result(&kv.key, &kv.value, &output_format, &mut writer)
                            .await?;
                    }
                } else {
                    info!("Writing results without sorting");
                    while let Some(kv) = reduce_rx.recv().await {
                        result_count += 1;
                        output::write_result(&kv.key, &kv.value, &output_format, &mut writer)
                            .await?;
                    }
                    info!("Wrote {} results to output", result_count);
                }

                writer.flush().await.map_err(Error::WriteOutput)?;
                Ok(())
            }
        });

        // reduce phase
        info!("Starting reduce phase");
        let task_idx = AtomicUsize::new(0);
        let reduce_entries: Vec<(String, Vec<js::Value>)> = match groups {
            GroupStorage::Memory(hashmap) => hashmap.into_iter().collect(),
            GroupStorage::Sled(db) => db
                .iter()
                .map(|entry| -> Result<(String, Vec<js::Value>)> {
                    let (key, value_bytes) = entry?;
                    let key = String::from_utf8_lossy(&key).to_string();
                    let values: Vec<js::Value> = deserialize(&value_bytes)?;
                    Ok((key, values))
                })
                .collect::<Result<_>>()?,
        };
        let reduce_result = tokio_stream::iter(reduce_entries)
            .chunks(self.chunk_size)
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(n_cpus, |batch: Vec<(String, Vec<js::Value>)>| {
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let worker_tx = worker_tx.clone();
                let reduce_tx = reduce_tx.clone();

                async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    worker_tx
                        .send_async(JobRequest::Reduce(batch, resp_tx))
                        .await
                        .map_err(worker_gone)?;
                    let value = resp_rx.await.map_err(|_| {
                        Error::Worker(format!(
                            "a JS VM worker exited during reduce task {}",
                            idx
                        ))
                    })??;
                    debug!("Reduce task {} completed with {} results", idx, value.len());
                    for kv in value {
                        reduce_tx
                            .send(kv)
                            .await
                            .map_err(|_| Error::Worker("the output writer stopped".into()))?;
                    }
                    Ok(())
                }
            })
            .await;

        // write results
        info!("Reduce phase completed, waiting for output");
        drop(reduce_tx);
        if let Err(reduce_error) = reduce_result {
            // A failed writer is the root cause of a failed send, so report it first.
            if reduce_consumer.is_finished() {
                reduce_consumer.await.map_err(task_failed)??;
            } else {
                reduce_consumer.abort();
            }
            return Err(reduce_error);
        }
        reduce_consumer.await.map_err(task_failed)??;
        info!("Pulsar processing completed successfully");

        if let Some(profiler) = self.profiler {
            profiler.write_report()?;
        }

        Ok(())
    }

}

impl<R: AsyncBufReadExt + Unpin> Debug for Pulsar<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pulsar").finish()
    }
}
