//! The boundary between the engine and the JavaScript runtime. The pipeline
//! only talks to a [`WorkerPool`]; everything specific to an engine lives in
//! the implementation of [`ScriptRuntime`].

#[cfg(test)]
mod conformance;
mod llrt;
mod pool;
#[cfg(feature = "v8")]
mod v8;

pub use llrt::LlrtRuntime;
#[cfg(feature = "v8")]
pub use v8::V8Runtime;
pub use pool::WorkerPool;

use crate::error::Result;
use crate::script::Contract;
use crate::value::{KeyValue, Value};

/// Name of the global object that holds the engine's own functions, so they
/// cannot collide with names defined by the user script.
pub(crate) const ENGINE_GLOBAL: &str = "__pulsar";

/// Installs the engine's functions. It runs after the user script, which can
/// define `map`, `combine`, `reduce` and `sort`; those are looked up in the
/// script's scope when the functions are called.
pub(crate) const WRAPPER: &str = r#"
globalThis.__pulsar = (() => {
    const hasCombine = typeof combine === 'function';

    const isThenable = (value) => typeof value?.then === 'function';

    const mapError = (line, error) =>
        new Error(`map failed on input line ${JSON.stringify(line.slice(0, 200))}: ${error}`);

    const reduceError = (key, error) =>
        new Error(`reduce failed for key ${JSON.stringify(key)}: ${error}`);

    // Calls map on every line in turn. Results that are promises run
    // concurrently and are awaited together; plain results need no await.
    const mapBatch = async (lines) => {
        const results = new Array(lines.length);
        let pending = false;
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            let result;
            try {
                result = map(line);
            } catch (error) {
                throw mapError(line, error);
            }
            if (isThenable(result)) {
                pending = true;
                results[i] = Promise.resolve(result).catch((error) => {
                    throw mapError(line, error);
                });
            } else {
                results[i] = result;
            }
        }
        const pairs = (pending ? await Promise.all(results) : results).flat(1);
        if (!hasCombine) {
            return pairs;
        }
        try {
            return await combine(pairs);
        } catch (error) {
            throw new Error(`combine failed on a batch of ${lines.length} lines: ${error}`);
        }
    };

    const reduceBatch = async (batch) => {
        const results = new Array(batch.length);
        let pending = false;
        for (let i = 0; i < batch.length; i++) {
            const [key, values] = batch[i];
            let reduced;
            try {
                reduced = reduce(key, values);
            } catch (error) {
                throw reduceError(key, error);
            }
            if (isThenable(reduced)) {
                pending = true;
                results[i] = Promise.resolve(reduced).then(
                    (value) => [key, value],
                    (error) => {
                        throw reduceError(key, error);
                    }
                );
            } else {
                results[i] = [key, reduced];
            }
        }
        return pending ? await Promise.all(results) : results;
    };

    const sortResults = async (results) => await sort(results);

    const runTest = async () => await test();

    return { mapBatch, reduceBatch, sortResults, runTest };
})();
"#;

/// Which JavaScript engine runs the script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum Engine {
    /// QuickJS through llrt: Node-style APIs (`require`, timers, `fetch`).
    #[default]
    Llrt,
    /// V8: much faster on script-heavy work, plain ECMAScript only. Needs a
    /// build with the `v8` feature.
    V8,
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Llrt => write!(f, "llrt"),
            Engine::V8 => write!(f, "v8"),
        }
    }
}

/// One script runtime instance. It is created on the thread that uses it and
/// never shared between threads.
pub(crate) trait ScriptRuntime: Sized {
    /// Creates the runtime, loads the user script into it and checks that it
    /// defines the functions the contract requires.
    async fn load(script: &str, contract: &Contract) -> Result<Self>;

    /// Runs `map` on every line of the batch, concurrently, and returns all
    /// the pairs it produced.
    async fn map_batch(&self, lines: Vec<String>) -> Result<Vec<KeyValue>>;

    /// Runs `reduce` for every group of the batch.
    async fn reduce_batch(&self, groups: Vec<(String, Vec<Value>)>) -> Result<Vec<KeyValue>>;

    /// Runs `sort` over the reduced results.
    async fn sort(&self, results: Vec<KeyValue>) -> Result<Vec<KeyValue>>;

    /// Runs the script's `test` function.
    async fn test(&self) -> Result<()>;

    /// Waits for work still pending inside the runtime, then drops it.
    async fn idle(self);
}
