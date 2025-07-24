use super::{KeyValue, Runtime, RuntimeContext, RuntimeError, Value};

/// A built-in runtime that implements word count
pub struct WordCountRuntime {}

impl WordCountRuntime {
    pub fn new() -> Self {
        Self {}
    }
}

impl Runtime for WordCountRuntime {
    fn map(&self, line: &str, _context: &RuntimeContext) -> Result<Vec<KeyValue>, RuntimeError> {
        let mut results = Vec::new();

        let normalized = line
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c.is_whitespace() {
                    c
                } else {
                    ' '
                }
            })
            .collect::<String>();

        let words: Vec<&str> = normalized.split_whitespace().collect();

        // Emit each word with count of 1
        for word in words {
            if !word.is_empty() {
                results.push(KeyValue {
                    key: word.to_string(),
                    value: Value::Int(1),
                });
            }
        }

        Ok(results)
    }

    fn reduce(
        &self,
        _key: Value,
        values: Vec<Value>,
        _context: &RuntimeContext,
    ) -> Result<Value, RuntimeError> {
        // Sum all the counts for this key
        let mut total = 0i64;
        for value in values {
            match value {
                Value::Int(count) => total += count,
                _ => {
                    return Err(RuntimeError::ExecutionError(
                        "Expected integer values in reduce phase".to_string(),
                    ));
                }
            }
        }

        Ok(Value::Int(total))
    }

    fn sort(
        &self,
        mut results: Vec<KeyValue>,
        _context: &RuntimeContext,
    ) -> Result<Vec<KeyValue>, RuntimeError> {
        // Sort alphabetically by key (word)
        results.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(results)
    }

    fn has_sort(&self) -> bool {
        true
    }
}

impl Default for WordCountRuntime {
    fn default() -> Self {
        Self::new()
    }
}
