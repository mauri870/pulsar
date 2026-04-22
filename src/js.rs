use anyhow::{Context, Result};
use llrt_core::vm::Vm;
use rquickjs::{CatchResultExt, Coerced};
use rquickjs::{Function, async_with, prelude::Promise};
use rquickjs::function::Async;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::thread::{self, JoinHandle};
use flume::Receiver;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, instrument};

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

// Key-value pair for MapReduce operations
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Value,
}

impl<'js> llrt_core::IntoJs<'js> for Value {
    fn into_js(self, ctx: &llrt_core::Ctx<'js>) -> rquickjs::Result<llrt_core::Value<'js>> {
        match self {
            Value::String(s) => s.into_js(ctx),
            Value::Int(i) => i.into_js(ctx),
            Value::Float(f) => f.into_js(ctx),
            Value::Bool(b) => b.into_js(ctx),
            Value::Null => Ok(rquickjs::Value::new_null(ctx.clone())),
            Value::Array(values) => {
                let js_array = rquickjs::Array::new(ctx.clone()).unwrap();
                for (i, v) in values.into_iter().enumerate() {
                    let js_val: rquickjs::Value = v.into_js(ctx)?;
                    js_array.set(i, js_val)?;
                }
                Ok(js_array.into())
            }
            Value::Object(obj) => {
                let js_object = rquickjs::Object::new(ctx.clone()).unwrap();
                for (key, value) in obj {
                    let js_val: rquickjs::Value = value.into_js(ctx)?;
                    js_object.set(key, js_val)?;
                }
                Ok(js_object.into())
            }
        }
    }
}

impl<'js> llrt_core::FromJs<'js> for Value {
    fn from_js(ctx: &llrt_core::Ctx<'js>, value: llrt_core::Value<'js>) -> rquickjs::Result<Self> {
        if value.is_string() {
            Ok(Value::String(value.as_string().unwrap().to_string()?))
        } else if value.is_int() {
            Ok(Value::Int(value.as_int().unwrap_or(0) as i64))
        } else if value.is_float() {
            Ok(Value::Float(value.as_float().unwrap_or(0.0)))
        } else if value.is_bool() {
            Ok(Value::Bool(value.as_bool().unwrap_or(false)))
        } else if value.is_null() {
            Ok(Value::Null)
        } else if value.is_array() {
            let js_array = value.as_array().unwrap();
            let mut vec = Vec::new();
            for i in 0..js_array.len() {
                let item = js_array.get(i)?;
                vec.push(Value::from_js(ctx, item)?);
            }
            Ok(Value::Array(vec))
        } else if value.is_object() {
            let object = value.as_object().unwrap();
            let map: HashMap<String, Value> = object
                .keys::<llrt_core::Value<'js>>()
                .map(|key| {
                    let key = key?;
                    let key_string: String = Coerced::from_js(ctx, key.clone())?.0;
                    let value_js = object.get::<_, llrt_core::Value<'js>>(key)?;
                    let value = Value::from_js(ctx, value_js)?;
                    Ok((key_string, value))
                })
                .collect::<Result<HashMap<String, Value>, rquickjs::Error>>()?;
            Ok(Value::Object(map))
        } else if value.is_undefined() {
            Ok(Value::Null) // Treat undefined as null
        } else {
            Err(rquickjs::Exception::throw_message(
                ctx,
                &format!("Unsupported JS value type: {:?}", value),
            ))
        }
    }
}

impl<'js> llrt_core::FromJs<'js> for KeyValue {
    fn from_js(ctx: &llrt_core::Ctx<'js>, value: llrt_core::Value<'js>) -> rquickjs::Result<Self> {
        if value.is_array() {
            let js_array = value.as_array().unwrap();
            let key: String = js_array.get(0)?;
            let value = Value::from_js(ctx, js_array.get(1)?)?;
            return Ok(KeyValue { key, value });
        }

        Err(rquickjs::Exception::throw_message(
            ctx,
            "KeyValue must be an array",
        ))
    }
}

impl<'js> llrt_core::IntoJs<'js> for KeyValue {
    fn into_js(self, ctx: &llrt_core::Ctx<'js>) -> rquickjs::Result<llrt_core::Value<'js>> {
        match self {
            KeyValue { key, value } => {
                let js_array = rquickjs::Array::new(ctx.clone()).unwrap();
                js_array.set(0, key.into_js(ctx)?)?;
                js_array.set(1, value.into_js(ctx)?)?;
                Ok(js_array.into())
            }
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            Value::Array(arr) => arr
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            Value::Object(obj) => obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v.to_string()))
                .collect::<Vec<_>>()
                .join(", "),
        }
    }
}

impl From<&Value> for serde_json::Value {
    fn from(value: &Value) -> Self {
        match value {
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::Float(n) => serde_json::Number::from_f64(*n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Null => serde_json::Value::Null,
            Value::Array(arr) => {
                let json_arr = arr.into_iter().map(Into::into).collect();
                serde_json::Value::Array(json_arr)
            }
            Value::Object(obj) => {
                let json_obj = obj
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::from(v)))
                    .collect();
                serde_json::Value::Object(json_obj)
            }
        }
    }
}

pub enum JobRequest {
    RunMapPhase {
        item_rx: Receiver<String>,
        result_tx: mpsc::Sender<Vec<KeyValue>>,
        concurrency: usize,
        done_tx: oneshot::Sender<Result<()>>,
    },
    Reduce(Vec<(String, Vec<Value>)>, oneshot::Sender<JobResult>),
    Sort(Vec<KeyValue>, oneshot::Sender<JobResult>),
}

impl fmt::Debug for JobRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobRequest::RunMapPhase { concurrency, .. } => f
                .debug_struct("JobRequest::RunMapPhase")
                .field("concurrency", concurrency)
                .finish(),
            JobRequest::Reduce(pairs, _) => f
                .debug_struct("JobRequest::Reduce")
                .field("pairs", pairs)
                .finish(),
            JobRequest::Sort(results, _) => f
                .debug_struct("JobRequest::Sort")
                .field("results", results)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JobResult {
    ReduceSuccess(Vec<KeyValue>),
    SortSuccess(Vec<KeyValue>),
    Error(String),
}

#[instrument(level = "trace")]
pub fn start_vm_worker(
    js_code: String,
    rx: Receiver<JobRequest>,
    init_tx: oneshot::Sender<Result<()>>,
) -> Result<JoinHandle<Result<()>>> {
    let handle = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create Tokio runtime")?;

        runtime.block_on(async move {
            let vm = Vm::new()
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))
                .context("Failed to create VM")?;

            let eval_result = vm
                .ctx
                .with(|ctx| {
                    let wrapper = r#"
                        const runMapWorker = async (concurrency) => {
                            if (typeof map !== 'function') {
                                throw new Error('map function is not defined');
                            }
                            const tick = async () => {
                                while (true) {
                                    const item = await nextMapItem();
                                    if (item == null) return;
                                    let pairs = await map(item);
                                    if (typeof combine === 'function') {
                                        pairs = await combine(pairs);
                                    }
                                    await sendMapResults(pairs);
                                }
                            };
                            await Promise.all(Array.from({length: concurrency}, tick));
                        };

                        const flatReduce = async (batch) => {
                            if (typeof reduce !== 'function') {
                                throw new Error('Reduce function is not defined');
                            }

                            const results = await Promise.all(
                                batch.map(async ([key, values]) => {
                                    const reduced = await reduce(key, values);
                                    return [key, reduced];
                                })
                            );

                            return results;
                        };
                    "#;

                    ctx.eval::<(), _>(format!("{}\n{}", wrapper, js_code))
                        .catch(&ctx)
                        .map_err(|e| e.to_string())
                })
                .await;
            if let Err(e) = eval_result {
                error!("Error loading JS code: {}", e);
                let _ = init_tx.send(Err(anyhow::anyhow!("Error loading JS code: {}", e)));
                return Err(anyhow::anyhow!("Error loading JS code: {}", e));
            }

            let _ = init_tx.send(Ok(()));

            while let Ok(job) = rx.recv_async().await {
                handle_job(&vm, job).await;
            }
            let _ = vm.idle().await;

            Ok(())
        })
    });

    Ok(handle)
}

#[instrument(level = "trace", skip(vm))]
async fn handle_job(vm: &Vm, job: JobRequest) {
    match job {
        JobRequest::RunMapPhase { item_rx, result_tx, concurrency, done_tx } => {
            let result = async_with!(vm.ctx => |ctx| {
                let rx = item_rx.clone();
                ctx.globals().set(
                    "nextMapItem",
                    Function::new(ctx.clone(), Async(move || {
                        let rx = rx.clone();
                        async move { rx.recv_async().await.ok() }
                    })),
                ).map_err(|e| e.to_string())?;
                let tx = result_tx.clone();
                ctx.globals().set(
                    "sendMapResults",
                    Function::new(ctx.clone(), Async(move |kvs: Vec<KeyValue>| {
                        let tx = tx.clone();
                        async move { let _ = tx.send(kvs).await; }
                    })),
                ).map_err(|e| e.to_string())?;
                let run_fn = ctx.globals()
                    .get::<_, Function>("runMapWorker")
                    .or_else(|_| ctx.eval("runMapWorker"))
                    .map_err(|e| format!("runMapWorker not found: {}", e))?;
                let promise: Promise = run_fn
                    .call((concurrency as u32,))
                    .catch(&ctx)
                    .map_err(|e| format!("Failed to call runMapWorker: {}", e))?;
                let () = promise
                    .into_future()
                    .await
                    .catch(&ctx)
                    .map_err(|e| format!("JavaScript error in runMapWorker: {}", e))?;
                Ok(())
            })
            .await;

            // Overwrite globals to drop the Sender clones they hold.
            // QuickJS uses reference counting so the closures are freed immediately.
            let _ = async_with!(vm.ctx => |ctx| {
                ctx.globals().set("nextMapItem", rquickjs::Value::new_null(ctx.clone()))?;
                ctx.globals().set("sendMapResults", rquickjs::Value::new_null(ctx.clone()))?;
                Ok::<_, rquickjs::Error>(())
            })
            .await;

            let _ = done_tx.send(result.map_err(|e: String| anyhow::anyhow!(e)));
        }
        JobRequest::Reduce(batch, respond_to) => {
            let result = async_with!(vm.ctx => |ctx| {
                let reduce_fn = ctx.globals()
                    .get::<_, Function>("flatReduce")
                    .or_else(|_| ctx.eval("flatReduce"))
                    .map_err(|e| format!("reduce function not found: {}", e))?;
                let batch_keyvalue: Vec<KeyValue> = batch
                    .into_iter()
                    .map(|(key, value)| KeyValue { key, value: Value::Array(value) })
                    .collect();
                let promise: Promise = reduce_fn
                    .call((batch_keyvalue,))
                    .catch(&ctx)
                    .map_err(|e| format!("Failed to call reduce function: {}", e))?;
                let output: Vec<KeyValue> = promise
                    .into_future()
                    .await
                    .catch(&ctx)
                    .map_err(|e| format!("JavaScript error: {}", e))?;
                Ok(output)
            })
            .await;

            let _ = match result {
                Ok(output) => respond_to.send(JobResult::ReduceSuccess(output)),
                Err(e) => respond_to.send(JobResult::Error(e)),
            };
        }
        JobRequest::Sort(results, respond_to) => {
            let result = async_with!(vm.ctx => |ctx| {
                let sort_fn = ctx.globals()
                    .get::<_, Function>("sort")
                    .or_else(|_| ctx.eval("sort"))
                    .map_err(|e| format!("sort function not found: {}", e))?;
                let promise: Promise = sort_fn
                    .call((results,))
                    .catch(&ctx)
                    .map_err(|e| format!("Failed to call sort function: {}", e))?;
                let output: Vec<KeyValue> = promise
                    .into_future()
                    .await
                    .catch(&ctx)
                    .map_err(|e| format!("JavaScript error: {}", e))?;
                Ok(output)
            })
            .await;

            let _ = match result {
                Ok(output) => respond_to.send(JobResult::SortSuccess(output)),
                Err(e) => respond_to.send(JobResult::Error(e)),
            };
        }
    }
}

#[instrument(level = "trace")]
pub fn run_test_file(code: String) -> Result<()> {
    let handle = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create Tokio runtime")?;

        runtime.block_on(async move {
            let vm = Vm::new()
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))
                .context("Failed to create VM")?;

            vm.ctx
                .with(|ctx| {
                    ctx.eval::<(), _>(code)
                        .catch(&ctx)
                        .map_err(|e| anyhow::anyhow!("JS eval error: {}", e))
                })
                .await?;

            let result = async_with!(vm.ctx => |ctx| {
                let test_fn = ctx.globals()
                    .get::<_, Function>("test")
                    .or_else(|_| ctx.eval("test"))
                    .context("test function not found")?;

                let promise: Promise = test_fn
                    .call(())
                    .catch(&ctx)
                    .map_err(|e| anyhow::anyhow!("Failed to call test function: {}", e))?;

                let () = promise
                    .into_future()
                    .await
                    .catch(&ctx)
                    .map_err(|e| anyhow::anyhow!("JavaScript error: {}", e))?;

                Ok(())
            })
            .await;

            let _ = vm.idle().await;

            result
        })
    });

    // Wait for the thread and propagate errors
    handle
        .join()
        .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?
}
