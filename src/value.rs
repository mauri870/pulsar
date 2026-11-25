use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Value {
    /// Approximate memory the value occupies, including its heap allocations.
    pub fn estimated_size(&self) -> usize {
        std::mem::size_of::<Value>()
            + match self {
                Value::String(text) => text.len(),
                Value::Array(items) => items.iter().map(Value::estimated_size).sum(),
                Value::Object(entries) => entries
                    .iter()
                    .map(|(key, value)| std::mem::size_of::<String>() + key.len() + value.estimated_size())
                    .sum(),
                Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 0,
            }
    }
}

/// The shard a key belongs to, out of `shards`. The same key always maps to
/// the same shard, in every thread and every run of the same binary.
pub fn shard_of(key: &str, shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % shards as u64) as usize
}

// Key-value pair for MapReduce operations
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalars_take_the_size_of_a_value() {
        let base = std::mem::size_of::<Value>();
        assert_eq!(Value::Null.estimated_size(), base);
        assert_eq!(Value::Int(7).estimated_size(), base);
        assert_eq!(Value::Float(1.5).estimated_size(), base);
    }

    #[test]
    fn strings_add_their_length() {
        let base = std::mem::size_of::<Value>();
        assert_eq!(Value::String("abcd".into()).estimated_size(), base + 4);
    }

    #[test]
    fn collections_add_their_elements() {
        let base = std::mem::size_of::<Value>();
        let array = Value::Array(vec![Value::Int(1), Value::String("ab".into())]);
        assert_eq!(array.estimated_size(), base + base + base + 2);
        let object = Value::Object(HashMap::from([("k".to_string(), Value::Int(1))]));
        assert_eq!(
            object.estimated_size(),
            base + std::mem::size_of::<String>() + 1 + base
        );
    }
}
