//! Runs the shared conformance tests against the V8 runtime, and tests what
//! is particular to it.

use crate::error::{Error, Result};
use crate::runtime::ScriptRuntime;
use crate::runtime::conformance::conformance_tests;
use crate::script::Contract;
use crate::value::{KeyValue, Value};
use std::collections::HashMap;

type Runtime = super::V8Runtime;

conformance_tests!();

#[tokio::test]
async fn a_promise_that_never_settles_is_an_error() {
    let script = "\
const map = line => new Promise(() => {});
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let error = runtime.map_batch(vec!["x".to_string()]).await.unwrap_err();
    assert!(matches!(error, Error::Map(_)));
    assert!(error.to_string().contains("timer or I/O"), "{}", error);
}

#[tokio::test]
async fn timers_and_node_apis_are_not_provided() {
    let script = "\
const map = line => [[typeof setTimeout + '/' + typeof require + '/' + typeof performance, 1]];
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs[0].key, "undefined/undefined/undefined");
}
