use llrt_core::vm::Vm;
use rquickjs::{CatchResultExt, Coerced};
use rquickjs::{Ctx, Function, Object, async_with, prelude::Promise};
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::runtime::ScriptRuntime;
use crate::script::Contract;
use crate::value::{KeyValue, Value};

/// The largest whole number a float represents exactly, along with every
/// whole number below it.
const MAX_EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

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
            let number = value.as_float().unwrap_or(0.0);
            // JS has a single number type, and whole numbers beyond 32 bits
            // arrive as floats. Keep them integers up to 2^53, where every
            // whole number is still exact.
            if number.fract() == 0.0 && number.abs() <= MAX_EXACT_INTEGER {
                Ok(Value::Int(number as i64))
            } else {
                Ok(Value::Float(number))
            }
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

/// Name of the global object that holds the engine's own functions, so they
/// cannot collide with names defined by the user script.
const ENGINE_GLOBAL: &str = "__pulsar";

/// Installs the engine's functions. It runs after the user script, which can
/// define `map`, `combine`, `reduce` and `sort`; those are looked up in the
/// script's scope when the functions are called.
const WRAPPER: &str = r#"
globalThis.__pulsar = (() => {
    const hasCombine = typeof combine === 'function';

    const mapBatch = async (lines) => {
        const results = await Promise.all(lines.map(async (line) => {
            try {
                return await map(line);
            } catch (error) {
                throw new Error(`map failed on input line ${JSON.stringify(line.slice(0, 200))}: ${error}`);
            }
        }));
        const pairs = results.flat(1);
        if (!hasCombine) {
            return pairs;
        }
        try {
            return await combine(pairs);
        } catch (error) {
            throw new Error(`combine failed on a batch of ${lines.length} lines: ${error}`);
        }
    };

    const reduceBatch = async (batch) => {
        return await Promise.all(
            batch.map(async ([key, values]) => {
                try {
                    const reduced = await reduce(key, values);
                    return [key, reduced];
                } catch (error) {
                    throw new Error(`reduce failed for key ${JSON.stringify(key)}: ${error}`);
                }
            })
        );
    };

    const sortResults = async (results) => await sort(results);

    const runTest = async () => await test();

    return { mapBatch, reduceBatch, sortResults, runTest };
})();
"#;

/// Looks up one of the engine's functions.
fn engine_function<'js>(ctx: &Ctx<'js>, name: &str) -> rquickjs::Result<Function<'js>> {
    ctx.globals().get::<_, Object>(ENGINE_GLOBAL)?.get(name)
}

/// The llrt (QuickJS) implementation of [`ScriptRuntime`].
pub struct LlrtRuntime {
    vm: Vm,
}

impl ScriptRuntime for LlrtRuntime {
    async fn load(script: &str, contract: &Contract) -> Result<Self> {
        let vm = Vm::new()
            .await
            .map_err(|e| Error::Worker(format!("Failed to create VM: {}", e)))?;

        let required = contract.required_functions();
        vm.ctx
            .with(|ctx| {
                ctx.eval::<(), _>(script)
                    .catch(&ctx)
                    .map_err(|e| Error::ScriptLoad(e.to_string()))?;

                for name in required {
                    let defined: bool = ctx
                        .eval(format!("typeof {} === 'function'", name))
                        .catch(&ctx)
                        .map_err(|e| Error::ScriptLoad(e.to_string()))?;
                    if !defined {
                        return Err(Error::ScriptContract(format!(
                            "the script must define a `{}` function",
                            name
                        )));
                    }
                }

                ctx.eval::<(), _>(WRAPPER)
                    .catch(&ctx)
                    .map_err(|e| Error::ScriptLoad(e.to_string()))
            })
            .await?;

        Ok(LlrtRuntime { vm })
    }

    async fn map_batch(&self, lines: Vec<String>) -> Result<Vec<KeyValue>> {
        async_with!(self.vm.ctx => |ctx| {
            let map_fn = engine_function(&ctx, "mapBatch")
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
            let reduce_fn = engine_function(&ctx, "reduceBatch")
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
            let sort_fn = engine_function(&ctx, "sortResults")
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
            let test_fn = engine_function(&ctx, "runTest")
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

#[cfg(test)]
mod tests;
