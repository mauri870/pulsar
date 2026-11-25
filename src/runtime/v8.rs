//! A V8 implementation of [`ScriptRuntime`]. V8 itself has no host APIs, so
//! this provides the few a map or reduce function commonly needs: `console`
//! (written to stderr), `setTimeout`, `setImmediate`, their `clear`
//! counterparts and `performance.now`. There is no `require`, `fetch` or
//! other I/O. Promises are settled by running the microtask queue and, when
//! a promise is still pending, the next due timer, until it settles; one
//! that is pending with no timer left is an error.
//!
//! Lines and results cross the boundary as JSON strings, one V8 call per batch.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Once;
use std::time::{Duration, Instant};

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

/// A callback waiting for its time to come.
struct Timer {
    id: u32,
    due: Instant,
    callback: v8::Global<v8::Function>,
    arguments: Vec<v8::Global<v8::Value>>,
}

/// The timers of one runtime, kept in the isolate's slot so that the host
/// functions can reach them.
struct EventLoop {
    next_id: u32,
    origin: Instant,
    timers: Vec<Timer>,
}

type SharedEventLoop = Rc<RefCell<EventLoop>>;

fn event_loop(scope: &v8::PinScope) -> SharedEventLoop {
    scope
        .get_slot::<SharedEventLoop>()
        .expect("the event loop is installed when the runtime is created")
        .clone()
}

/// `setTimeout(callback, delay, ...arguments)` and, when `immediate`,
/// `setImmediate(callback, ...arguments)`.
fn add_timer(
    scope: &mut v8::PinScope,
    arguments: &v8::FunctionCallbackArguments,
    mut return_value: v8::ReturnValue,
    immediate: bool,
) {
    let Ok(callback) = v8::Local::<v8::Function>::try_from(arguments.get(0)) else {
        let message = v8::String::new(scope, "the callback must be a function").unwrap();
        let error = v8::Exception::type_error(scope, message);
        scope.throw_exception(error);
        return;
    };
    let (delay, first_argument) = if immediate {
        (0.0, 1)
    } else {
        let delay = arguments.get(1).number_value(scope).unwrap_or(0.0);
        (if delay.is_finite() { delay.max(0.0) } else { 0.0 }, 2)
    };
    let extra: Vec<v8::Global<v8::Value>> = (first_argument..arguments.length())
        .map(|index| v8::Global::new(scope, arguments.get(index)))
        .collect();

    let events = event_loop(scope);
    let mut events = events.borrow_mut();
    events.next_id += 1;
    let id = events.next_id;
    events.timers.push(Timer {
        id,
        due: Instant::now() + Duration::from_secs_f64(delay / 1000.0),
        callback: v8::Global::new(scope, callback),
        arguments: extra,
    });
    return_value.set(v8::Number::new(scope, id as f64).into());
}

fn set_timeout(scope: &mut v8::PinScope, arguments: v8::FunctionCallbackArguments, return_value: v8::ReturnValue) {
    add_timer(scope, &arguments, return_value, false);
}

fn set_immediate(scope: &mut v8::PinScope, arguments: v8::FunctionCallbackArguments, return_value: v8::ReturnValue) {
    add_timer(scope, &arguments, return_value, true);
}

fn clear_timer(scope: &mut v8::PinScope, arguments: v8::FunctionCallbackArguments, _: v8::ReturnValue) {
    if let Some(id) = arguments.get(0).number_value(scope) {
        event_loop(scope).borrow_mut().timers.retain(|timer| timer.id as f64 != id);
    }
}

fn performance_now(scope: &mut v8::PinScope, _: v8::FunctionCallbackArguments, mut return_value: v8::ReturnValue) {
    let elapsed = event_loop(scope).borrow().origin.elapsed().as_secs_f64() * 1000.0;
    return_value.set(v8::Number::new(scope, elapsed).into());
}

/// `console.log` and friends: the arguments, separated by spaces, on stderr.
fn console_write(scope: &mut v8::PinScope, arguments: v8::FunctionCallbackArguments, _: v8::ReturnValue) {
    let parts: Vec<String> = (0..arguments.length())
        .map(|index| arguments.get(index).to_rust_string_lossy(scope))
        .collect();
    eprintln!("{}", parts.join(" "));
}

/// Defines the host functions on the global object of the current context.
fn install_host(scope: &mut v8::PinScope) {
    fn define(scope: &mut v8::PinScope, target: v8::Local<v8::Object>, name: &str, function: impl v8::MapFnTo<v8::FunctionCallback>) {
        let key = v8::String::new(scope, name).unwrap();
        let function = v8::Function::new(scope, function).unwrap();
        target.set(scope, key.into(), function.into());
    }

    let global = scope.get_current_context().global(scope);
    define(scope, global, "setTimeout", set_timeout);
    define(scope, global, "setImmediate", set_immediate);
    define(scope, global, "clearTimeout", clear_timer);
    define(scope, global, "clearImmediate", clear_timer);

    let performance = v8::Object::new(scope);
    define(scope, performance, "now", performance_now);
    let key = v8::String::new(scope, "performance").unwrap();
    global.set(scope, key.into(), performance.into());

    let console = v8::Object::new(scope);
    for name in ["log", "info", "debug", "warn", "error"] {
        define(scope, console, name, console_write);
    }
    let key = v8::String::new(scope, "console").unwrap();
    global.set(scope, key.into(), console.into());
}

pub struct V8Runtime {
    // The context handle must be dropped before the isolate it belongs to,
    // and fields are dropped in declaration order.
    context: v8::Global<v8::Context>,
    isolate: RefCell<v8::OwnedIsolate>,
}

impl Drop for V8Runtime {
    fn drop(&mut self) {
        // The timers hold handles, which must be released while the isolate is alive.
        if let Some(events) = self.isolate.get_mut().get_slot::<SharedEventLoop>() {
            events.borrow_mut().timers.clear();
        }
    }
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
            let events = event_loop(tc);
            while promise.state() == v8::PromiseState::Pending {
                // Run the next timer, earliest first and in creation order.
                let next = {
                    let mut events = events.borrow_mut();
                    let position = (0..events.timers.len())
                        .min_by_key(|&index| (events.timers[index].due, events.timers[index].id));
                    position.map(|index| events.timers.remove(index))
                };
                let Some(timer) = next else {
                    return Err("the script is waiting for I/O, which this engine does not provide".to_string());
                };
                let wait = timer.due.saturating_duration_since(Instant::now());
                if !wait.is_zero() {
                    std::thread::sleep(wait);
                }
                let callback = v8::Local::new(tc, &timer.callback);
                let arguments: Vec<v8::Local<v8::Value>> =
                    timer.arguments.iter().map(|argument| v8::Local::new(tc, argument)).collect();
                let undefined: v8::Local<v8::Value> = v8::undefined(tc).into();
                if callback.call(tc, undefined, &arguments).is_none() {
                    return Err(exception_text(tc));
                }
                tc.perform_microtask_checkpoint();
            }
            match promise.state() {
                v8::PromiseState::Fulfilled => {
                    let value = promise.result(tc);
                    Ok(value.to_rust_string_lossy(tc))
                }
                _ => {
                    let value = promise.result(tc);
                    Err(value.to_rust_string_lossy(tc))
                }
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
        isolate.set_slot::<SharedEventLoop>(Rc::new(RefCell::new(EventLoop {
            next_id: 0,
            origin: Instant::now(),
            timers: Vec::new(),
        })));
        let context = {
            v8::scope!(let handle_scope, &mut isolate);
            let context = v8::Context::new(handle_scope, Default::default());
            let scope = &mut v8::ContextScope::new(handle_scope, context);
            install_host(scope);
            v8::Global::new(scope, context)
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
