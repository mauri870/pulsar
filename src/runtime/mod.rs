//! The boundary between the engine and the JavaScript runtime. The pipeline
//! only talks to a [`WorkerPool`]; everything specific to an engine lives in
//! the implementation of [`ScriptRuntime`].

mod llrt;
mod pool;

pub use llrt::LlrtRuntime;
pub use pool::WorkerPool;

use crate::error::Result;
use crate::value::{KeyValue, Value};

/// One script runtime instance. It is created on the thread that uses it and
/// never shared between threads.
pub(crate) trait ScriptRuntime: Sized {
    /// Creates the runtime and loads the user script into it.
    async fn load(script: &str) -> Result<Self>;

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
