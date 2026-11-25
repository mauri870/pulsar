use bincode::{deserialize, serialize};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use std::{collections::HashMap, sync::Arc, sync::atomic::AtomicUsize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufWriter},
    task::{JoinError, JoinHandle},
};
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, info};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::input::InputReader;
use crate::output::OutputFormat;
use crate::runtime::{LlrtRuntime, WorkerPool};
use crate::script::Contract;
use crate::value::{KeyValue, Value};
use crate::{input, output, profile, script};
use std::fmt::Debug;
use tracing::instrument;

fn task_failed(error: JoinError) -> Error {
    Error::Worker(format!("a pipeline task failed: {}", error))
}

fn merge_values(_key: &[u8], old_value: Option<&[u8]>, new_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut values: Vec<Value> = old_value
        .and_then(|bytes| deserialize(bytes).ok())
        .unwrap_or_default();
    let new_values: Vec<Value> = deserialize(new_bytes).unwrap_or_default();
    values.extend(new_values);
    serialize(&values).ok()
}

const MAX_GROUPING_HEAP_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB

const HASHMAP_SLOT_SIZE: usize = {
    let size = std::mem::size_of::<(String, Vec<Value>)>();
    let align = {
        let a = std::mem::align_of::<(String, Vec<Value>)>();
        let b = std::mem::align_of::<usize>();
        if a > b { a } else { b }
    };
    (size / align) * align
};

enum GroupStorage {
    Memory(HashMap<String, Vec<Value>>),
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
        let contract = Contract { sort: false, test: true };
        let pool = WorkerPool::start::<LlrtRuntime>(&self.script, contract, 1).await?;
        pool.test().await?;
        pool.shutdown()?;
        println!("OK");
        Ok(())
    }

    #[instrument(level = "trace")]
    pub async fn run_engine(self) -> Result<()> {
        let n_cpus = self.workers;
        info!("Starting pulsar engine with {} CPU workers", n_cpus);
        let contract = Contract { sort: self.sort, test: false };
        let pool =
            Arc::new(WorkerPool::start::<LlrtRuntime>(&self.script, contract, n_cpus).await?);
        info!("Successfully started {} JS VM workers", n_cpus);

        // aggregate map results — one Vec<KeyValue> per mapped batch of lines
        let (map_result_tx, map_result_rx) = tokio::sync::mpsc::channel::<Vec<KeyValue>>(n_cpus * 2);
        let map_consumer: JoinHandle<Result<GroupStorage>> = tokio::spawn(async move {
            let mut map_result_rx = map_result_rx;
            let mut overflow_db: Option<sled::Db> = None;
            let mut hashmap: HashMap<String, Vec<Value>> = HashMap::new();
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

        // map phase: send batches of lines to the workers, a few batches per worker at a time
        info!("Starting map phase");
        let mut lines = LinesStream::new(self.reader.lines());
        let mut batch = Vec::with_capacity(self.chunk_size);
        let mut in_flight = FuturesUnordered::new();
        let max_in_flight = n_cpus * 2;
        let mut input_done = false;
        while !input_done || !in_flight.is_empty() {
            tokio::select! {
                Some(result) = in_flight.next(), if !in_flight.is_empty() => {
                    let pairs = result?;
                    map_result_tx
                        .send(pairs)
                        .await
                        .map_err(|_| Error::Worker("the group stage stopped".into()))?;
                }
                line = lines.next(), if !input_done && in_flight.len() < max_in_flight => match line {
                    Some(Ok(line)) => {
                        batch.push(line);
                        if batch.len() == self.chunk_size {
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
        // Closing the channel ends the group stage.
        drop(map_result_tx);
        drop(in_flight);

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
            let pool = Arc::clone(&pool);
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

                    let output = pool.sort(results).await?;
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
        let reduce_entries: Vec<(String, Vec<Value>)> = match groups {
            GroupStorage::Memory(hashmap) => hashmap.into_iter().collect(),
            GroupStorage::Sled(db) => db
                .iter()
                .map(|entry| -> Result<(String, Vec<Value>)> {
                    let (key, value_bytes) = entry?;
                    let key = String::from_utf8_lossy(&key).to_string();
                    let values: Vec<Value> = deserialize(&value_bytes)?;
                    Ok((key, values))
                })
                .collect::<Result<_>>()?,
        };
        let reduce_result = tokio_stream::iter(reduce_entries)
            .chunks(self.chunk_size)
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(n_cpus, |batch: Vec<(String, Vec<Value>)>| {
                let idx = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let pool = Arc::clone(&pool);
                let reduce_tx = reduce_tx.clone();

                async move {
                    let value = pool.reduce_batch(batch).await?;
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
        if let Some(pool) = Arc::into_inner(pool) {
            pool.shutdown()?;
        }
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
