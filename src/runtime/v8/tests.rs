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
    assert!(error.to_string().contains("waiting for I/O"), "{}", error);
}

#[tokio::test]
async fn node_io_apis_are_not_provided_but_timers_and_performance_are() {
    let script = "\
const map = line => [[[typeof setTimeout, typeof setImmediate, typeof clearTimeout, typeof performance.now,
                       typeof console.log, typeof require, typeof fetch].join('/'), 1]];
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs[0].key, "function/function/function/function/function/undefined/undefined");
}

#[tokio::test]
async fn an_async_map_can_wait_for_timers() {
    let script = "\
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const map = async line => { await delay(5); return [[line, 1]]; };
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let lines: Vec<String> = (0..20).map(|n| n.to_string()).collect();
    let started = std::time::Instant::now();
    let pairs = runtime.map_batch(lines).await.unwrap();
    assert_eq!(pairs.len(), 20);
    // The twenty waits overlap instead of adding up to 100 ms.
    assert!(started.elapsed() < std::time::Duration::from_millis(90), "{:?}", started.elapsed());
}

#[tokio::test]
async fn timers_run_earliest_first_and_in_creation_order() {
    let script = "\
const map = line => new Promise(resolve => {
  const order = [];
  setTimeout(() => order.push('slow'), 20);
  setTimeout(() => order.push('a'), 5);
  setTimeout(() => order.push('b'), 5);
  setImmediate(() => order.push('immediate'));
  setTimeout(() => resolve([[order.join(','), 1]]), 40);
});
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs[0].key, "immediate,a,b,slow");
}

#[tokio::test]
async fn a_cleared_timer_does_not_run_and_arguments_are_passed() {
    let script = "\
const map = line => new Promise(resolve => {
  const results = [];
  const cancelled = setTimeout(() => results.push('cancelled'), 5);
  clearTimeout(cancelled);
  setTimeout((a, b) => results.push(a + b), 5, 'x', 'y');
  setTimeout(() => resolve([[results.join(','), 1]]), 20);
});
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs[0].key, "xy");
}

#[tokio::test]
async fn performance_now_advances_with_time() {
    let script = "\
const map = async line => {
  const start = performance.now();
  await new Promise(resolve => setTimeout(resolve, 10));
  return [[performance.now() - start >= 9 ? 'waited' : 'too soon', 1]];
};
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs[0].key, "waited");
}

#[tokio::test]
async fn an_exception_in_a_timer_callback_is_an_error() {
    let script = "\
const map = line => new Promise(() => setTimeout(() => { throw new Error('in a timer'); }, 1));
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let error = runtime.map_batch(vec!["x".to_string()]).await.unwrap_err();
    assert!(error.to_string().contains("in a timer"), "{}", error);
}

#[tokio::test]
async fn console_output_does_not_fail_the_run() {
    let script = "\
const map = line => { console.log('seen', line, 1); console.error({}); return [[line, 1]]; };
const reduce = () => 1;
";
    let runtime = load(script).await.unwrap();
    let pairs = runtime.map_batch(vec!["x".to_string()]).await.unwrap();
    assert_eq!(pairs.len(), 1);
}
