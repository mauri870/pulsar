//! Tests of the llrt runtime itself: loading, the script contract, the
//! engine's functions and the conversion of values between Rust and JS.

use super::*;
use std::collections::HashMap;

async fn load(script: &str) -> Result<LlrtRuntime> {
    LlrtRuntime::load(script, &Contract::default()).await
}

fn pair(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value,
    }
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<HashMap<_, _>>(),
    )
}

const WORD_COUNT: &str = "\
const map = line => line.split(' ').map(word => [word, 1]);
const reduce = (key, values) => values.reduce((sum, value) => sum + value, 0);
";

#[tokio::test]
async fn a_syntax_error_is_a_load_error() {
    let result = load("const map =\nvar reduce = 1;").await;
    assert!(matches!(result, Err(Error::ScriptLoad(_))));
}

#[tokio::test]
async fn a_missing_required_function_names_it() {
    for (script, missing) in [
        ("const reduce = () => 1;", "map"),
        ("const map = () => [];", "reduce"),
    ] {
        match load(script).await {
            Err(Error::ScriptContract(message)) => assert!(message.contains(missing), "{}", message),
            other => panic!("expected a contract error, got {:?}", other.err()),
        }
    }
}

#[tokio::test]
async fn sort_and_test_are_required_only_when_the_contract_says_so() {
    let sort_contract = Contract { sort: true, test: false };
    let test_contract = Contract { sort: false, test: true };
    assert!(LlrtRuntime::load(WORD_COUNT, &Contract::default()).await.is_ok());
    assert!(matches!(
        LlrtRuntime::load(WORD_COUNT, &sort_contract).await,
        Err(Error::ScriptContract(message)) if message.contains("sort")
    ));
    assert!(matches!(
        LlrtRuntime::load(WORD_COUNT, &test_contract).await,
        Err(Error::ScriptContract(message)) if message.contains("test")
    ));
}

#[tokio::test]
async fn a_name_that_is_not_a_function_does_not_satisfy_the_contract() {
    let result = load("const map = 1; const reduce = () => 1;").await;
    assert!(matches!(result, Err(Error::ScriptContract(_))));
}

#[tokio::test]
async fn map_batch_returns_every_pair_of_every_line() {
    let runtime = load(WORD_COUNT).await.unwrap();
    let pairs = runtime
        .map_batch(vec!["a b".to_string(), "c".to_string(), "".to_string()])
        .await
        .unwrap();
    let mut keys: Vec<String> = pairs.into_iter().map(|pair| pair.key).collect();
    keys.sort();
    // The empty line splits into one empty word.
    assert_eq!(keys, ["", "a", "b", "c"]);
}

#[tokio::test]
async fn map_may_return_a_promise_or_a_plain_array() {
    let script = "\
const map = async line => [[line, 1]];
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs.len(), 1);
}

#[tokio::test]
async fn combine_runs_once_per_line_when_it_is_defined() {
    let script = "\
const map = line => line.split(' ').map(word => [word, 1]);
const combine = pairs => {
  const sums = {};
  for (const [key, value] of pairs) sums[key] = (sums[key] || 0) + value;
  return Object.entries(sums);
};
const reduce = (key, values) => values.length;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["a a b".to_string()]).await.unwrap();
    assert_eq!(pairs.len(), 2, "combine should merge the two `a` pairs");
}

#[tokio::test]
async fn a_failing_map_names_the_line_and_truncates_it() {
    let script = "\
const map = line => { throw new Error('nope'); };
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let long_line = "x".repeat(500);
    let error = runtime.map_batch(vec![long_line]).await.unwrap_err();
    let message = error.to_string();
    assert!(matches!(error, Error::Map(_)));
    assert!(message.contains("map failed on input line"), "{}", message);
    assert!(message.contains("nope"), "{}", message);
    assert!(!message.contains(&"x".repeat(201)), "line should be truncated");
}

#[tokio::test]
async fn a_map_result_that_is_not_a_pair_array_is_an_error() {
    let script = "\
const map = line => 'not an array';
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let result = runtime.map_batch(vec!["x".to_string()]).await;
    assert!(matches!(result, Err(Error::Map(_))));
}

#[tokio::test]
async fn reduce_batch_reduces_each_group() {
    let runtime = load(WORD_COUNT).await.unwrap();
    let reduced = runtime
        .reduce_batch(vec![
            ("a".to_string(), vec![Value::Int(1), Value::Int(2)]),
            ("b".to_string(), vec![Value::Int(5)]),
        ])
        .await
        .unwrap();
    let totals: HashMap<String, Value> = reduced.into_iter().map(|pair| (pair.key, pair.value)).collect();
    assert_eq!(totals["a"], Value::Int(3));
    assert_eq!(totals["b"], Value::Int(5));
}

#[tokio::test]
async fn a_failing_reduce_names_the_key() {
    let script = "\
const map = line => [[line, 1]];
const reduce = (key, values) => { throw new Error('bad total'); };
";
    let runtime = load(script).await.unwrap();
    let error = runtime
        .reduce_batch(vec![("somekey".to_string(), vec![Value::Int(1)])])
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(matches!(error, Error::Reduce(_)));
    assert!(message.contains("reduce failed for key \"somekey\""), "{}", message);
    assert!(message.contains("bad total"), "{}", message);
}

#[tokio::test]
async fn sort_orders_the_results() {
    let script = "\
const map = line => [[line, 1]];
const reduce = () => 1;
const sort = results => results.sort((a, b) => a[0].localeCompare(b[0]));
";
    let runtime = LlrtRuntime::load(script, &Contract { sort: true, test: false })
        .await
        .unwrap();
    let sorted = runtime
        .sort(vec![pair("b", Value::Int(1)), pair("a", Value::Int(2))])
        .await
        .unwrap();
    let keys: Vec<&str> = sorted.iter().map(|pair| pair.key.as_str()).collect();
    assert_eq!(keys, ["a", "b"]);
}

#[tokio::test]
async fn test_runs_the_test_function_and_reports_its_failure() {
    let passing = "\
const map = () => []; const reduce = () => 1;
const test = () => {};
";
    let failing = "\
const map = () => []; const reduce = () => 1;
const test = async () => { throw new Error('assertion failed'); };
";
    let contract = Contract { sort: false, test: true };
    let runtime = LlrtRuntime::load(passing, &contract).await.unwrap();
    runtime.test().await.unwrap();

    let runtime = LlrtRuntime::load(failing, &contract).await.unwrap();
    let error = runtime.test().await.unwrap_err();
    assert!(matches!(error, Error::Test(_)));
    assert!(error.to_string().contains("assertion failed"));
}

#[tokio::test]
async fn values_survive_the_trip_through_javascript() {
    let script = "\
const map = line => [['k', JSON.parse(line)]];
const reduce = (key, values) => values[0];
";
    let runtime = load(script).await.unwrap();
    let json = r#"{"a":[1,"x",null,true,1.5],"b":{"c":2},"d":"text"}"#;
    let pairs = runtime.map_batch(vec![json.to_string()]).await.unwrap();
    let expected = object(vec![
        (
            "a",
            Value::Array(vec![
                Value::Int(1),
                Value::String("x".to_string()),
                Value::Null,
                Value::Bool(true),
                Value::Float(1.5),
            ]),
        ),
        ("b", object(vec![("c", Value::Int(2))])),
        ("d", Value::String("text".to_string())),
    ]);
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].value, expected);

    // And back: a value passed to reduce arrives in JS as the same structure.
    let echoed = runtime
        .reduce_batch(vec![("k".to_string(), vec![expected.clone()])])
        .await
        .unwrap();
    assert_eq!(echoed[0].value, expected);
}
