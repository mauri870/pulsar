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

/// The largest whole number a float represents exactly, along with every
/// whole number below it.
pub const MAX_EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

impl Value {
    /// The value as JSON. A float that JSON cannot represent (NaN, infinity)
    /// becomes null.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::Float(n) => serde_json::Number::from_f64(*n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Null => serde_json::Value::Null,
            Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(Value::to_json).collect())
            }
            Value::Object(entries) => serde_json::Value::Object(
                entries
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json()))
                    .collect(),
            ),
        }
    }

    /// The value read back from JSON. A whole number that is not an exact
    /// integer in the JSON text but fits in 2^53 becomes an integer, as for
    /// numbers coming from JavaScript.
    pub fn from_json(value: serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => Value::Int(i),
                None => {
                    let f = n.as_f64().unwrap_or(0.0);
                    if f.fract() == 0.0 && f.abs() <= MAX_EXACT_INTEGER {
                        Value::Int(f as i64)
                    } else {
                        Value::Float(f)
                    }
                }
            },
            serde_json::Value::Array(items) => {
                Value::Array(items.into_iter().map(Value::from_json).collect())
            }
            serde_json::Value::Object(entries) => Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, Value::from_json(value)))
                    .collect(),
            ),
        }
    }

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
