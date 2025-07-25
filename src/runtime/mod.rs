pub mod javascript;
pub mod word_count;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

pub use javascript::*;
pub use word_count::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
    Array(Vec<Value>),
}

// Key-value pair for MapReduce operations
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Value,
}

// Context passed to runtime functions
#[derive(Debug)]
pub struct RuntimeContext {
    pub id: String,
}

impl RuntimeContext {
    pub fn new(id: String) -> Self {
        Self { id }
    }
}

// Error type for runtime operations
#[derive(Debug)]
pub enum RuntimeError {
    ExecutionError(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RuntimeError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
        }
    }
}

impl Error for RuntimeError {}

// Runtime defines the interface for a MapReduce implementation
#[async_trait]
pub trait Runtime: Send + Sync {
    /// Execute the map function on input data
    /// Takes a single line of input and returns zero or more key-value pairs
    async fn map(
        &self,
        line: &str,
        context: &RuntimeContext,
    ) -> Result<Vec<KeyValue>, RuntimeError>;

    /// Execute the reduce function on grouped data
    /// Takes a key and all values associated with that key
    /// Returns a single aggregated value for the key
    async fn reduce(
        &self,
        key: Value,
        values: Vec<Value>,
        context: &RuntimeContext,
    ) -> Result<Value, RuntimeError>;

    /// Execute custom sorting logic on all results
    /// Takes the full list of key-value pairs after reduction
    async fn sort(
        &self,
        results: Vec<KeyValue>,
        context: &RuntimeContext,
    ) -> Result<Vec<KeyValue>, RuntimeError>;

    /// Check if the runtime has a sort function available
    fn has_sort(&self) -> bool;
}
