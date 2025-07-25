use crate::runtime::{KeyValue, Runtime, RuntimeContext, RuntimeError, Value};
use async_trait::async_trait;
use llrt_core::vm::Vm;
use rquickjs::{Function, async_with, prelude::Promise};

/// A JavaScript runtime implementation
/// This allows executing custom map-reduce scripts written in JavaScript
pub struct JavaScriptRuntime {
    script: String,
}

impl JavaScriptRuntime {
    pub fn new(script: String) -> Result<Self, RuntimeError> {
        Ok(Self { script })
    }
}

#[async_trait]
impl Runtime for JavaScriptRuntime {
    async fn map(
        &self,
        line: &str,
        _context: &RuntimeContext,
    ) -> Result<Vec<KeyValue>, RuntimeError> {
        // Create a new VM for each task
        let vm = Vm::new().await.unwrap();

        // Evaluate the code
        vm.ctx
            .with(|ctx| {
                let _ = ctx.eval::<(), _>(self.script.as_str()).unwrap();
            })
            .await;

        async_with!(vm.ctx => |ctx| {
            let map_fn = ctx.globals().get::<_, Function>("map").or_else(|_| ctx.eval("map")).map_err(|e| RuntimeError::ExecutionError(format!("map function not found: {:?}", e)))?;
            let promise: Promise = map_fn.call((line.to_string(),)).map_err(|e| RuntimeError::ExecutionError(format!("Failed to call map function: {:?}", e)))?;
            promise
                .into_future()
                .await
                .map_err(|e| RuntimeError::ExecutionError(format!("JavaScript error: {:?}", e)))
        })
        .await
    }

    async fn reduce(
        &self,
        key: Value,
        values: Vec<Value>,
        _context: &RuntimeContext,
    ) -> Result<Value, RuntimeError> {
        let vm = Vm::new().await.unwrap();

        vm.ctx
            .with(|ctx| {
                let _ = ctx.eval::<(), _>(self.script.as_str()).unwrap();
            })
            .await;

        async_with!(vm.ctx => |ctx| {
            let reduce_fn = ctx.globals().get::<_, Function>("reduce").or_else(|_| ctx.eval("reduce")).map_err(|e| RuntimeError::ExecutionError(format!("reduce function not found: {:?}", e)))?;
            let promise: Promise = reduce_fn.call((key, values)).map_err(|e| RuntimeError::ExecutionError(format!("Failed to call reduce function: {:?}", e)))?;
            promise
                .into_future()
                .await
                .map_err(|e| RuntimeError::ExecutionError(format!("JavaScript error: {:?}", e)))
        })
        .await
    }

    async fn sort(
        &self,
        data: Vec<KeyValue>,
        _context: &RuntimeContext,
    ) -> Result<Vec<KeyValue>, RuntimeError> {
        let vm = Vm::new().await.unwrap();

        vm.ctx
            .with(|ctx| {
                let _ = ctx.eval::<(), _>(self.script.as_str()).unwrap();
            })
            .await;

        async_with!(vm.ctx => |ctx| {
            let reduce_fn = ctx.globals().get::<_, Function>("sort").or_else(|_| ctx.eval("sort")).map_err(|e| RuntimeError::ExecutionError(format!("sort function not found: {:?}", e)))?;
            let promise: Promise = reduce_fn.call((data,)).map_err(|e| RuntimeError::ExecutionError(format!("Failed to call sort function: {:?}", e)))?;
            promise
                .into_future()
                .await
                .map_err(|e| RuntimeError::ExecutionError(format!("JavaScript error: {:?}", e)))
        })
        .await
    }

    async fn has_sort(&self) -> bool {
        let vm = Vm::new().await.unwrap();

        vm.ctx
            .with(|ctx| {
                // Evaluate the script
                if let Err(e) = ctx.eval::<(), _>(self.script.as_str()) {
                    eprintln!("Script eval failed: {}", e);
                    return false;
                }

                // Check if global 'sort' function exists
                ctx.globals()
                    .get::<_, Function>("sort")
                    .or_else(|_| ctx.eval("sort"))
                    .is_ok()
            })
            .await
    }
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
