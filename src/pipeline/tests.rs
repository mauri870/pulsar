//! Tests that drive the whole pipeline without a JavaScript engine: a fake
//! runtime that counts words, an in-memory source and an in-memory sink.

use super::*;
use futures::Stream;
use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;

/// Counts words: map emits `(word, 1)`, reduce sums the values and sort
/// orders the keys in reverse. A few magic words make a stage fail.
struct FakeRuntime;

impl ScriptRuntime for FakeRuntime {
    async fn load(_script: &str, _contract: &Contract) -> Result<Self> {
        Ok(FakeRuntime)
    }

    async fn map_batch(&self, lines: Vec<String>) -> Result<Vec<KeyValue>> {
        let mut pairs = Vec::new();
        for line in lines {
            if line == "map-error" {
                return Err(Error::Map("fake map failure".into()));
            }
            for word in line.split_whitespace() {
                pairs.push(KeyValue {
                    key: word.to_string(),
                    value: Value::Int(1),
                });
            }
        }
        Ok(pairs)
    }

    async fn reduce_batch(&self, groups: Vec<(String, Vec<Value>)>) -> Result<Vec<KeyValue>> {
        groups
            .into_iter()
            .map(|(key, values)| {
                if key == "reduce-error" {
                    return Err(Error::Reduce("fake reduce failure".into()));
                }
                let total = values
                    .iter()
                    .map(|value| if let Value::Int(n) = value { *n } else { 0 })
                    .sum();
                Ok(KeyValue {
                    key,
                    value: Value::Int(total),
                })
            })
            .collect()
    }

    async fn sort(&self, mut results: Vec<KeyValue>) -> Result<Vec<KeyValue>> {
        results.sort_by(|a, b| b.key.cmp(&a.key));
        Ok(results)
    }

    async fn test(&self) -> Result<()> {
        Ok(())
    }

    async fn idle(self) {}
}

struct VecSource(Vec<io::Result<String>>);

impl VecSource {
    fn lines(lines: &[&str]) -> Self {
        VecSource(lines.iter().map(|line| Ok(line.to_string())).collect())
    }
}

impl Source for VecSource {
    fn into_lines(self) -> impl Stream<Item = io::Result<String>> + Send + Unpin {
        futures::stream::iter(self.0)
    }
}

type Written = Arc<Mutex<Vec<(String, Value)>>>;

/// Collects what is written; fails when asked to write `fail_on`.
struct VecSink {
    written: Written,
    fail_on: Option<&'static str>,
}

impl VecSink {
    fn new() -> (Self, Written) {
        let written = Written::default();
        let sink = VecSink {
            written: Arc::clone(&written),
            fail_on: None,
        };
        (sink, written)
    }
}

impl Sink for VecSink {
    async fn write(&mut self, key: &str, value: &Value) -> Result<()> {
        if self.fail_on == Some(key) {
            return Err(Error::WriteOutput(io::Error::other("sink closed")));
        }
        self.written
            .lock()
            .unwrap()
            .push((key.to_string(), value.clone()));
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn settings(workers: usize, chunk_size: usize, sort: bool) -> Settings {
    Settings {
        workers,
        chunk_size,
        sort,
    }
}

fn counts(written: &Written) -> BTreeMap<String, i64> {
    written
        .lock()
        .unwrap()
        .iter()
        .map(|(key, value)| match value {
            Value::Int(n) => (key.clone(), *n),
            other => panic!("unexpected value {:?}", other),
        })
        .collect()
}

#[tokio::test]
async fn counts_words_for_every_worker_and_batch_size() {
    let expected = BTreeMap::from([
        ("a".to_string(), 3),
        ("b".to_string(), 2),
        ("c".to_string(), 1),
    ]);
    for (workers, chunk_size) in [(1, 1), (2, 1), (4, 3), (3, 100)] {
        let (sink, written) = VecSink::new();
        let source = VecSource::lines(&["a b a", "b a", "c", ""]);
        run_pipeline::<FakeRuntime>("", &settings(workers, chunk_size, false), source, sink)
            .await
            .unwrap();
        assert_eq!(counts(&written), expected, "{} workers, chunk {}", workers, chunk_size);
    }
}

#[tokio::test]
async fn empty_input_writes_nothing() {
    let (sink, written) = VecSink::new();
    run_pipeline::<FakeRuntime>("", &settings(2, 4, false), VecSource::lines(&[]), sink)
        .await
        .unwrap();
    assert!(written.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sort_orders_the_results_through_the_runtime() {
    let (sink, written) = VecSink::new();
    let source = VecSource::lines(&["b a c", "a"]);
    run_pipeline::<FakeRuntime>("", &settings(2, 1, true), source, sink)
        .await
        .unwrap();
    let keys: Vec<String> = written.lock().unwrap().iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(keys, ["c", "b", "a"]);
}

#[tokio::test]
async fn a_map_failure_is_returned() {
    let (sink, _) = VecSink::new();
    let mut lines = vec!["x"; 500];
    lines.insert(250, "map-error");
    let result =
        run_pipeline::<FakeRuntime>("", &settings(4, 2, false), VecSource::lines(&lines), sink).await;
    assert!(matches!(result, Err(Error::Map(_))), "{:?}", result);
}

#[tokio::test]
async fn a_reduce_failure_is_returned() {
    let (sink, _) = VecSink::new();
    let source = VecSource::lines(&["a", "reduce-error", "b"]);
    let result = run_pipeline::<FakeRuntime>("", &settings(2, 1, false), source, sink).await;
    assert!(matches!(result, Err(Error::Reduce(_))), "{:?}", result);
}

#[tokio::test]
async fn a_read_error_is_returned() {
    let (sink, _) = VecSink::new();
    let source = VecSource(vec![
        Ok("a".to_string()),
        Err(io::Error::new(io::ErrorKind::InvalidData, "bad line")),
    ]);
    let result = run_pipeline::<FakeRuntime>("", &settings(2, 1, false), source, sink).await;
    assert!(matches!(result, Err(Error::ReadInput(_))), "{:?}", result);
}

#[tokio::test]
async fn a_sink_failure_is_returned_as_the_root_cause() {
    let (mut sink, _) = VecSink::new();
    sink.fail_on = Some("b");
    let words: Vec<String> = (0..2000).map(|n| format!("word{}", n)).collect();
    let mut lines: Vec<&str> = words.iter().map(String::as_str).collect();
    lines.push("b");
    let result =
        run_pipeline::<FakeRuntime>("", &settings(3, 4, false), VecSource::lines(&lines), sink).await;
    assert!(matches!(result, Err(Error::WriteOutput(_))), "{:?}", result);
}
