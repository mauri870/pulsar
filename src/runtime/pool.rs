use std::thread::{self, JoinHandle};
use tokio::sync::oneshot;

use super::ScriptRuntime;
use crate::error::{Error, Result};
use crate::script::Contract;
use crate::value::{KeyValue, Value, shard_of};

type Reply<T> = oneshot::Sender<Result<T>>;

enum Job {
    Map(Vec<String>, usize, Reply<Vec<Vec<KeyValue>>>),
    Reduce(Vec<(String, Vec<Value>)>, Reply<Vec<KeyValue>>),
    Sort(Vec<KeyValue>, Reply<Vec<KeyValue>>),
    Test(Reply<()>),
}

/// A fixed set of worker threads, each owning one script runtime. Callers
/// submit typed jobs; whichever worker is free runs the next one.
pub struct WorkerPool {
    jobs: flume::Sender<Job>,
    threads: Vec<JoinHandle<Result<()>>>,
}

impl WorkerPool {
    /// Starts `workers` threads, each loading `script` into its own runtime
    /// of type `R` and checking it against `contract`, and waits until all
    /// of them are ready.
    pub async fn start<R: ScriptRuntime + 'static>(
        script: &str,
        contract: Contract,
        workers: usize,
    ) -> Result<Self> {
        let (jobs, job_receiver) = flume::bounded(workers);
        let mut threads = Vec::with_capacity(workers);
        let mut ready_receivers = Vec::with_capacity(workers);

        for _ in 0..workers {
            let (ready_sender, ready_receiver) = oneshot::channel();
            ready_receivers.push(ready_receiver);
            let script = script.to_owned();
            let job_receiver = job_receiver.clone();
            threads.push(thread::spawn(move || {
                run_worker::<R>(script, contract, job_receiver, ready_sender)
            }));
        }
        drop(job_receiver);

        for ready_receiver in ready_receivers {
            match ready_receiver.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(Error::Worker(
                        "a JS VM worker exited during initialization".into(),
                    ));
                }
            }
        }
        Ok(WorkerPool { jobs, threads })
    }

    /// Runs `map` on a batch of lines. The pairs it produced come back split
    /// into `shards` groups by key, so the same key is always in the same
    /// one; the splitting is done on the worker thread.
    pub async fn map_batch(&self, lines: Vec<String>, shards: usize) -> Result<Vec<Vec<KeyValue>>> {
        self.submit(|reply| Job::Map(lines, shards, reply)).await
    }

    pub async fn reduce_batch(&self, groups: Vec<(String, Vec<Value>)>) -> Result<Vec<KeyValue>> {
        self.submit(|reply| Job::Reduce(groups, reply)).await
    }

    pub async fn sort(&self, results: Vec<KeyValue>) -> Result<Vec<KeyValue>> {
        self.submit(|reply| Job::Sort(results, reply)).await
    }

    pub async fn test(&self) -> Result<()> {
        self.submit(Job::Test).await
    }

    /// Stops accepting jobs and waits for every worker thread to finish.
    pub fn shutdown(self) -> Result<()> {
        drop(self.jobs);
        for thread in self.threads {
            thread
                .join()
                .map_err(|panic| Error::Worker(format!("a JS VM worker panicked: {:?}", panic)))??;
        }
        Ok(())
    }

    async fn submit<T>(&self, job: impl FnOnce(Reply<T>) -> Job) -> Result<T> {
        let (reply, response) = oneshot::channel();
        self.jobs
            .send_async(job(reply))
            .await
            .map_err(|_| Error::Worker("all JS VM workers have exited".into()))?;
        response
            .await
            .map_err(|_| Error::Worker("a JS VM worker exited while running a job".into()))?
    }
}

/// Splits pairs into `shards` groups by the shard of their key.
fn partition(pairs: Vec<KeyValue>, shards: usize) -> Vec<Vec<KeyValue>> {
    if shards == 1 {
        return vec![pairs];
    }
    let mut parts: Vec<Vec<KeyValue>> = (0..shards).map(|_| Vec::new()).collect();
    for pair in pairs {
        parts[shard_of(&pair.key, shards)].push(pair);
    }
    parts
}

/// Reports a startup failure to the waiting caller and returns it.
fn fail_startup(ready: Reply<()>, error: Error) -> Result<()> {
    let message = error.to_string();
    let _ = ready.send(Err(error));
    Err(Error::Worker(message))
}

fn run_worker<R: ScriptRuntime>(
    script: String,
    contract: Contract,
    jobs: flume::Receiver<Job>,
    ready: Reply<()>,
) -> Result<()> {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            return fail_startup(
                ready,
                Error::Worker(format!("Failed to create Tokio runtime: {}", e)),
            );
        }
    };

    runtime.block_on(async move {
        let script_runtime = match R::load(&script, &contract).await {
            Ok(script_runtime) => script_runtime,
            Err(error) => return fail_startup(ready, error),
        };
        let _ = ready.send(Ok(()));

        while let Ok(job) = jobs.recv_async().await {
            match job {
                Job::Map(lines, shards, reply) => {
                    let pairs = script_runtime.map_batch(lines).await;
                    let _ = reply.send(pairs.map(|pairs| partition(pairs, shards)));
                }
                Job::Reduce(groups, reply) => {
                    let _ = reply.send(script_runtime.reduce_batch(groups).await);
                }
                Job::Sort(results, reply) => {
                    let _ = reply.send(script_runtime.sort(results).await);
                }
                Job::Test(reply) => {
                    let _ = reply.send(script_runtime.test().await);
                }
            }
        }
        script_runtime.idle().await;
        Ok(())
    })
}
