//! A V8 implementation of [`ScriptRuntime`]. V8 has no timers, console or
//! Node-style APIs, so scripts are limited to plain ECMAScript; promises are
//! settled by running the microtask queue, and a promise that is still
//! pending after that (it waits for a timer or I/O) is an error.
//!
//! Lines and results cross the boundary as JSON strings, one V8 call per batch.

use std::cell::RefCell;
use std::sync::Once;

use super::{ScriptRuntime, WRAPPER};
use crate::error::{Error, Result};
use crate::script::Contract;
use crate::value::{KeyValue, Value};

/// JSON adapters around the functions the shared wrapper installs.
const JSON_ADAPTERS: &str = r#"
Object.assign(globalThis.__pulsar, {
    mapBatchJson: async (json) => JSON.stringify(await globalThis.__pulsar.mapBatch(JSON.parse(json))),
    reduceBatchJson: async (json) => JSON.stringify(await globalThis.__pulsar.reduceBatch(JSON.parse(json))),
    sortJson: async (json) => JSON.stringify(await globalThis.__pulsar.sortResults(JSON.parse(json))),
    testJson: async () => { await globalThis.__pulsar.runTest(); return ''; },
});
"#;

fn initialize_v8() {
    static INITIALIZED: Once = Once::new();
    INITIALIZED.call_once(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });
}

pub struct V8Runtime {
    // The context handle must be dropped before the isolate it belongs to,
    // and fields are dropped in declaration order.
    context: v8::Global<v8::Context>,
    isolate: RefCell<v8::OwnedIsolate>,
}

fn pairs_to_json(pairs: &[KeyValue]) -> String {
    let array: Vec<serde_json::Value> = pairs
        .iter()
        .map(|pair| serde_json::Value::Array(vec![serde_json::Value::String(pair.key.clone()), pair.value.to_json()]))
        .collect();
    serde_json::Value::Array(array).to_string()
}

fn pairs_from_json(text: &str) -> std::result::Result<Vec<KeyValue>, String> {
    let pairs: Vec<(String, serde_json::Value)> =
        serde_json::from_str(text).map_err(|e| format!("result is not a list of pairs: {}", e))?;
    Ok(pairs
        .into_iter()
        .map(|(key, value)| KeyValue { key, value: Value::from_json(value) })
        .collect())
}

impl V8Runtime {
    /// Runs `f` with a scope entered in the runtime's context.
    fn with_scope<R>(&self, f: impl FnOnce(&mut v8::PinScope) -> R) -> R {
        let mut isolate = self.isolate.borrow_mut();
        v8::scope!(let handle_scope, &mut *isolate);
        let context = v8::Local::new(handle_scope, &self.context);
        let scope = &mut v8::ContextScope::new(handle_scope, context);
        f(scope)
    }

    /// Calls `__pulsar[function](argument)`, settles the promise it returns
    /// and gives back the string it resolved to.
    fn call(&self, function: &str, argument: &str) -> std::result::Result<String, String> {
        self.with_scope(|scope| {
            v8::tc_scope!(let tc, scope);

            let global = tc.get_current_context().global(tc);
            let engine_key = v8::String::new(tc, "__pulsar").unwrap();
            let engine = global
                .get(tc, engine_key.into())
                .and_then(|value| value.to_object(tc))
                .ok_or("the engine functions are not installed")?;
            let name = v8::String::new(tc, function).unwrap();
            let function: v8::Local<v8::Function> = engine
                .get(tc, name.into())
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| format!("{} is not defined", function))?;
            let argument = v8::String::new(tc, argument).ok_or("argument is too large")?;

            let result = match function.call(tc, engine.into(), &[argument.into()]) {
                Some(result) => result,
                None => return Err(exception_text(tc)),
            };
            tc.perform_microtask_checkpoint();

            let promise: v8::Local<v8::Promise> =
                result.try_into().map_err(|_| "the engine function did not return a promise".to_string())?;
            match promise.state() {
                v8::PromiseState::Fulfilled => {
                    let value = promise.result(tc);
                    Ok(value.to_rust_string_lossy(tc))
                }
                v8::PromiseState::Rejected => {
                    let value = promise.result(tc);
                    Err(value.to_rust_string_lossy(tc))
                }
                v8::PromiseState::Pending => Err(
                    "the script is waiting for a timer or I/O, which this engine does not provide".to_string(),
                ),
            }
        })
    }
}

fn exception_text(tc: &mut v8::PinnedRef<'_, v8::TryCatch<v8::HandleScope>>) -> String {
    match tc.exception() {
        Some(exception) => exception.to_rust_string_lossy(tc),
        None => "unknown JavaScript error".to_string(),
    }
}

impl ScriptRuntime for V8Runtime {
    async fn load(script: &str, contract: &Contract) -> Result<Self> {
        initialize_v8();
        let mut isolate = v8::Isolate::new(v8::CreateParams::default());
        let context = {
            v8::scope!(let handle_scope, &mut isolate);
            let context = v8::Context::new(handle_scope, Default::default());
            v8::Global::new(handle_scope, context)
        };
        let runtime = V8Runtime {
            context,
            isolate: RefCell::new(isolate),
        };

        let run = |source: &str| -> std::result::Result<(), String> {
            runtime.with_scope(|scope| {
                v8::tc_scope!(let tc, scope);
                let code = v8::String::new(tc, source).ok_or("script is too large")?;
                let compiled = v8::Script::compile(tc, code, None).ok_or_else(|| exception_text(tc))?;
                compiled.run(tc).ok_or_else(|| exception_text(tc))?;
                Ok(())
            })
        };

        run(script).map_err(Error::ScriptLoad)?;
        for name in contract.required_functions() {
            let defined = runtime
                .with_scope(|scope| {
                    let code = v8::String::new(scope, &format!("typeof {} === 'function'", name)).unwrap();
                    let compiled = v8::Script::compile(scope, code, None)?;
                    Some(compiled.run(scope)?.is_true())
                })
                .unwrap_or(false);
            if !defined {
                return Err(Error::ScriptContract(format!("the script must define a `{}` function", name)));
            }
        }
        run(WRAPPER).map_err(Error::ScriptLoad)?;
        run(JSON_ADAPTERS).map_err(Error::ScriptLoad)?;
        Ok(runtime)
    }

    async fn map_batch(&self, lines: Vec<String>) -> Result<Vec<KeyValue>> {
        let input = serde_json::to_string(&lines).map_err(|e| Error::Map(e.to_string()))?;
        let output = self.call("mapBatchJson", &input).map_err(Error::Map)?;
        pairs_from_json(&output).map_err(Error::Map)
    }

    async fn reduce_batch(&self, groups: Vec<(String, Vec<Value>)>) -> Result<Vec<KeyValue>> {
        let batch: Vec<KeyValue> = groups
            .into_iter()
            .map(|(key, values)| KeyValue { key, value: Value::Array(values) })
            .collect();
        let output = self.call("reduceBatchJson", &pairs_to_json(&batch)).map_err(Error::Reduce)?;
        pairs_from_json(&output).map_err(Error::Reduce)
    }

    async fn sort(&self, results: Vec<KeyValue>) -> Result<Vec<KeyValue>> {
        let output = self.call("sortJson", &pairs_to_json(&results)).map_err(Error::Sort)?;
        pairs_from_json(&output).map_err(Error::Sort)
    }

    async fn test(&self) -> Result<()> {
        self.call("testJson", "").map(|_| ()).map_err(Error::Test)
    }

    async fn idle(self) {}
}

#[cfg(test)]
mod tests;
