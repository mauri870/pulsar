use llrt_core::vm::Vm;
use rquickjs::{Function, async_with, prelude::Promise};
use serde::{Deserialize, Serialize};
use std::thread;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tracing::{error, instrument};

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

impl<'js> llrt_core::IntoJs<'js> for Value {
    fn into_js(self, ctx: &llrt_core::Ctx<'js>) -> rquickjs::Result<llrt_core::Value<'js>> {
        match self {
            Value::String(s) => s.into_js(ctx),
            Value::Int(i) => i.into_js(ctx),
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
        }
    }
}

impl<'js> llrt_core::FromJs<'js> for Value {
    fn from_js(ctx: &llrt_core::Ctx<'js>, value: llrt_core::Value<'js>) -> rquickjs::Result<Self> {
        if value.is_string() {
            Ok(Value::String(value.as_string().unwrap().to_string()?))
        } else if value.is_int() {
            Ok(Value::Int(value.as_int().unwrap_or(0) as i64))
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
        } else {
            Err(rquickjs::Exception::throw_message(
                ctx,
                "Unsupported runtime Value type",
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
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            Value::Array(arr) => arr
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
        }
    }
}

impl From<&Value> for serde_json::Value {
    fn from(value: &Value) -> Self {
        match value {
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Null => serde_json::Value::Null,
            Value::Array(arr) => {
                let json_arr = arr.into_iter().map(Into::into).collect();
                serde_json::Value::Array(json_arr)
            }
        }
    }
}

pub enum JobRequest {
    Map(String, oneshot::Sender<JobResult>),
    Reduce(String, Vec<Value>, oneshot::Sender<JobResult>),
    Sort(Vec<KeyValue>, oneshot::Sender<JobResult>),
}

pub enum JobResult {
    MapSuccess(Vec<KeyValue>),
    ReduceSuccess(Value),
    SortSuccess(Vec<KeyValue>),
    Error(String),
}

#[instrument(level = "trace")]
pub fn start_vm_worker(js_code: String, mut rx: UnboundedReceiver<JobRequest>) {
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let vm = Vm::new().await.unwrap();

            let eval_result = vm
                .ctx
                .with(|ctx| ctx.eval::<(), _>(js_code).map_err(|e| e.to_string()))
                .await;
            if let Err(e) = eval_result {
                error!("Error loading JS code: {}", e);
                return;
            }

            while let Some(job) = rx.recv().await {
                match job {
                    JobRequest::Map(input, respond_to) => {
                        let result = async_with!(vm.ctx => |ctx| {
                            let map_fn = ctx.globals()
                                .get::<_, Function>("map")
                                .or_else(|_| ctx.eval("map"))
                                .map_err(|e| format!("map function not found: {:?}", e))?;
                            let promise: Promise = map_fn
                                .call((input,))
                                .map_err(|e| format!("Failed to call map function: {:?}", e))?;
                            let output: Vec<KeyValue> = promise
                                .into_future()
                                .await
                                .map_err(|e| format!("JavaScript error: {:?}", e))?;
                            Ok(output)
                        })
                        .await;

                        let _ = match result {
                            Ok(output) => respond_to.send(JobResult::MapSuccess(output)),
                            Err(e) => respond_to.send(JobResult::Error(e)),
                        };
                    }
                    JobRequest::Reduce(input, values, respond_to) => {
                        let result = async_with!(vm.ctx => |ctx| {
                            let reduce_fn = ctx.globals()
                                .get::<_, Function>("reduce")
                                .or_else(|_| ctx.eval("reduce"))
                                .map_err(|e| format!("reduce function not found: {:?}", e))?;
                            let promise: Promise = reduce_fn
                                .call((input, values))
                                .map_err(|e| format!("Failed to call reduce function: {:?}", e))?;
                            let output: Value = promise
                                .into_future()
                                .await
                                .map_err(|e| format!("JavaScript error: {:?}", e))?;
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
                                .map_err(|e| format!("sort function not found: {:?}", e))?;
                            let promise: Promise = sort_fn
                                .call((results,))
                                .map_err(|e| format!("Failed to call sort function: {:?}", e))?;
                            let output: Vec<KeyValue> = promise
                                .into_future()
                                .await
                                .map_err(|e| format!("JavaScript error: {:?}", e))?;
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
        });
    });
}
