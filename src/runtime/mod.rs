pub mod word_count;

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

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
    ScriptError(String),
    SerializationError(String),
    InvalidFunction(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RuntimeError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            RuntimeError::ScriptError(msg) => write!(f, "Script error: {}", msg),
            RuntimeError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            RuntimeError::InvalidFunction(msg) => write!(f, "Invalid function: {}", msg),
        }
    }
}

impl Error for RuntimeError {}

// Runtime defines the interface for a MapReduce implementation
pub trait Runtime: Send + Sync {
    /// Execute the map function on input data
    /// Takes a single line of input and returns zero or more key-value pairs
    fn map(&self, line: &str, context: &RuntimeContext) -> Result<Vec<KeyValue>, RuntimeError>;

    /// Execute the reduce function on grouped data
    /// Takes a key and all values associated with that key
    /// Returns a single aggregated value for the key
    fn reduce(
        &self,
        key: Value,
        values: Vec<Value>,
        context: &RuntimeContext,
    ) -> Result<Value, RuntimeError>;

    /// Execute custom sorting logic on all results
    /// Takes the full list of key-value pairs after reduction
    fn sort(
        &self,
        results: Vec<KeyValue>,
        context: &RuntimeContext,
    ) -> Result<Vec<KeyValue>, RuntimeError>;

    /// Check if the runtime has a sort function available
    fn has_sort(&self) -> bool;
}
