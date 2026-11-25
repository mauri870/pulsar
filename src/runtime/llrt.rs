use llrt_core::vm::Vm;
use rquickjs::{CatchResultExt, Coerced};
use rquickjs::{Function, async_with, prelude::Promise};
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::runtime::ScriptRuntime;
use crate::value::{KeyValue, Value};

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

/// Defines the functions the engine calls. They run in the same scope as the
/// user script, which can define `map`, `combine`, `reduce` and `sort`.
const WRAPPER: &str = r#"
const mapBatch = async (lines) => {
    if (typeof map !== 'function') {
        throw new Error('map function is not defined');
    }
    const results = await Promise.all(lines.map(async (line) => {
        try {
            let pairs = await map(line);
            if (typeof combine === 'function') {
                pairs = await combine(pairs);
            }
            return pairs;
        } catch (error) {
            throw new Error(`map failed on input line ${JSON.stringify(line.slice(0, 200))}: ${error}`);
        }
    }));
    return results.flat(1);
};

const flatReduce = async (batch) => {
    if (typeof reduce !== 'function') {
        throw new Error('Reduce function is not defined');
    }

    const results = await Promise.all(
        batch.map(async ([key, values]) => {
            try {
                const reduced = await reduce(key, values);
                return [key, reduced];
            } catch (error) {
                throw new Error(`reduce failed for key ${JSON.stringify(key)}: ${error}`);
            }
        })
    );

    return results;
};
"#;

/// The llrt (QuickJS) implementation of [`ScriptRuntime`].
pub struct LlrtRuntime {
    vm: Vm,
}

impl ScriptRuntime for LlrtRuntime {
    async fn load(script: &str) -> Result<Self> {
        let vm = Vm::new()
            .await
            .map_err(|e| Error::Worker(format!("Failed to create VM: {}", e)))?;

        let source = format!("{}\n{}", WRAPPER, script);
        vm.ctx
            .with(|ctx| {
                ctx.eval::<(), _>(source)
                    .catch(&ctx)
                    .map_err(|e| Error::ScriptLoad(e.to_string()))
            })
            .await?;

        Ok(LlrtRuntime { vm })
    }

    async fn map_batch(&self, lines: Vec<String>) -> Result<Vec<KeyValue>> {
        async_with!(self.vm.ctx => |ctx| {
            let map_fn = ctx.globals()
                .get::<_, Function>("mapBatch")
                .or_else(|_| ctx.eval("mapBatch"))
                .map_err(|e| Error::Map(format!("map function not found: {}", e)))?;
            let promise: Promise = map_fn
                .call((lines,))
                .catch(&ctx)
                .map_err(|e| Error::Map(format!("Failed to call map function: {}", e)))?;
            let output: Vec<KeyValue> = promise
                .into_future()
                .await
                .catch(&ctx)
                .map_err(|e| Error::Map(format!("JavaScript error: {}", e)))?;
            Ok::<_, Error>(output)
        })
        .await
    }

    async fn reduce_batch(&self, groups: Vec<(String, Vec<Value>)>) -> Result<Vec<KeyValue>> {
        async_with!(self.vm.ctx => |ctx| {
            let reduce_fn = ctx.globals()
                .get::<_, Function>("flatReduce")
                .or_else(|_| ctx.eval("flatReduce"))
                .map_err(|e| Error::Reduce(format!("reduce function not found: {}", e)))?;
            let batch: Vec<KeyValue> = groups
                .into_iter()
                .map(|(key, values)| KeyValue { key, value: Value::Array(values) })
                .collect();
            let promise: Promise = reduce_fn
                .call((batch,))
                .catch(&ctx)
                .map_err(|e| Error::Reduce(format!("Failed to call reduce function: {}", e)))?;
            let output: Vec<KeyValue> = promise
                .into_future()
                .await
                .catch(&ctx)
                .map_err(|e| Error::Reduce(format!("JavaScript error: {}", e)))?;
            Ok::<_, Error>(output)
        })
        .await
    }

    async fn sort(&self, results: Vec<KeyValue>) -> Result<Vec<KeyValue>> {
        async_with!(self.vm.ctx => |ctx| {
            let sort_fn = ctx.globals()
                .get::<_, Function>("sort")
                .or_else(|_| ctx.eval("sort"))
                .map_err(|e| Error::Sort(format!("sort function not found: {}", e)))?;
            let promise: Promise = sort_fn
                .call((results,))
                .catch(&ctx)
                .map_err(|e| Error::Sort(format!("Failed to call sort function: {}", e)))?;
            let output: Vec<KeyValue> = promise
                .into_future()
                .await
                .catch(&ctx)
                .map_err(|e| Error::Sort(format!("JavaScript error: {}", e)))?;
            Ok::<_, Error>(output)
        })
        .await
    }

    async fn test(&self) -> Result<()> {
        async_with!(self.vm.ctx => |ctx| {
            let test_fn = ctx.globals()
                .get::<_, Function>("test")
                .or_else(|_| ctx.eval("test"))
                .map_err(|e| Error::Test(format!("test function not found: {}", e)))?;
            let promise: Promise = test_fn
                .call(())
                .catch(&ctx)
                .map_err(|e| Error::Test(format!("Failed to call test function: {}", e)))?;
            let () = promise
                .into_future()
                .await
                .catch(&ctx)
                .map_err(|e| Error::Test(format!("JavaScript error: {}", e)))?;
            Ok::<_, Error>(())
        })
        .await
    }

    async fn idle(self) {
        let _ = self.vm.idle().await;
    }
}
