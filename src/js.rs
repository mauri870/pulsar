use llrt_core::vm::Vm;
use rquickjs::{Function, async_with, prelude::Promise};
use serde::{Deserialize, Serialize};
use std::thread;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tracing::instrument;

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

pub enum JobRequest {
    Map(String, oneshot::Sender<JobResult>),
    Reduce(String, Vec<Value>, oneshot::Sender<JobResult>),
}

pub enum JobResult {
    MapSuccess(Vec<KeyValue>),
    ReduceSuccess(Value),
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

            vm.ctx
                .with(|ctx| {
                    ctx.eval::<(), _>(js_code).unwrap();
                })
                .await;

            while let Some(job) = rx.recv().await {
                match job {
                    JobRequest::Map(input, respond_to) => {
                        let result = async_with!(vm.ctx => |ctx| {
                            let map_fn = ctx.globals().get::<_, Function>("map").or_else(|_| ctx.eval("map")).unwrap();
                            let promise: Promise = map_fn.call((input,)).unwrap();
                            let output: Vec<KeyValue> = promise.into_future().await.unwrap();
                            output
                        })
                        .await;

                        let _ = respond_to.send(JobResult::MapSuccess(result));
                    }
                    JobRequest::Reduce(input, values, respond_to) => {
                        let result = async_with!(vm.ctx => |ctx| {
                            let reduce_fn = ctx.globals().get::<_, Function>("reduce").or_else(|_| ctx.eval("reduce")).unwrap();
                            let promise: Promise = reduce_fn.call((input, values)).unwrap();
                            let output: Value = promise.into_future().await.unwrap();
                            output
                        })
                        .await;

                        let _ = respond_to.send(JobResult::ReduceSuccess(result));
                    }
                }
            }
        });
    });
}
