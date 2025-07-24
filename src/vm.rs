use llrt_modules::module_builder::ModuleBuilder;
use rquickjs::{AsyncContext, AsyncRuntime, Error, Function, async_with, promise::MaybePromise, IntoJs};
use std::result::Result as StdResult;

pub struct VM {
    context: AsyncContext,
    runtime: AsyncRuntime, // Keep runtime accessible for idle() method
}

impl VM {
    pub async fn new() -> StdResult<Self, Box<dyn std::error::Error + Send + Sync>> {
        let runtime = AsyncRuntime::new()?;

        let module_builder = ModuleBuilder::default();
        let (module_resolver, module_loader, global_attachment) = module_builder.build();
        runtime
            .set_loader((module_resolver,), (module_loader,))
            .await;

        let context = AsyncContext::full(&runtime).await?;

        // Attach global modules like Buffer
        async_with!(context => |ctx| {
            global_attachment.attach(&ctx)?;
            Ok::<_, Error>(())
        })
        .await?;

        Ok(Self { context, runtime })
    }

    pub async fn eval_script(&self, script: &str) -> StdResult<(), Error> {
        async_with!(self.context => |ctx| {
            if let Err(Error::Exception) = ctx.eval::<(), _>(script) {
                let exception = ctx.catch();
                eprintln!("Script evaluation error: {:#?}", exception);
                return Err(Error::Exception);
            }
            Ok::<_, Error>(())
        })
        .await
    }

    // Helper method to check if a function exists
    pub async fn has_function(&self, name: &str) -> bool {
        async_with!(self.context => |ctx| {
            let globals = ctx.globals();
            globals.get(name).map(|_: rquickjs::Function| ()).is_ok()
                || ctx.eval(name).map(|_: rquickjs::Function| ()).is_ok()
        })
        .await
    }

    // Helper method for sorting operations
    pub async fn sort_results(&self, results: Vec<(String, serde_json::Value)>) -> StdResult<Vec<(String, serde_json::Value)>, Error> {
        async_with!(self.context => |ctx| {
            let globals = ctx.globals();
            let sort_fn: rquickjs::Function = globals
                .get("sort")
                .or_else(|_| ctx.eval("sort"))?;

            // Convert results to JavaScript array for sorting
            let js_array = rquickjs::Array::new(ctx.clone())?;
            for (i, (key, value)) in results.iter().enumerate() {
                let result_pair = rquickjs::Array::new(ctx.clone())?;
                result_pair.set(0, key.clone())?;
                let js_value: rquickjs::Value = ctx
                    .json_parse(serde_json::to_string(value).unwrap())?;
                result_pair.set(1, js_value)?;
                js_array.set(i, result_pair)?;
            }

            let sorted_array: rquickjs::Value = sort_fn.call((js_array,))?;

            // Convert back to Rust types
            let mut sorted_results = Vec::new();
            if let Some(array) = sorted_array.as_array() {
                for i in 0..array.len() {
                    if let Ok(pair) = array.get::<rquickjs::Value>(i) {
                        if let Some(pair_array) = pair.as_array() {
                            if pair_array.len() >= 2 {
                                let key: String = pair_array.get(0)?;
                                let value_js: rquickjs::Value = pair_array.get(1)?;
                                let json_str = ctx
                                    .json_stringify(value_js)?
                                    .unwrap()
                                    .to_string()
                                    .unwrap();
                                let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
                                sorted_results.push((key, value));
                            }
                        }
                    }
                }
            }

            Ok::<Vec<(String, serde_json::Value)>, Error>(sorted_results)
        })
        .await
    }

    // Unified function call method - handles single arguments
    pub async fn call_function(&self, name: &str, arg: &str) -> StdResult<String, Error> {
        async_with!(self.context => |ctx| {
            // Get the function
            let globals = ctx.globals();
            let function: Function = globals.get(name)
                .or_else(|_| ctx.eval(name))?;

            // Convert string directly to JS string (no JSON parsing needed for simple strings)
            let js_arg = arg.into_js(&ctx)?;

            // Call the function and get a MaybePromise
            let result: MaybePromise = function.call((js_arg,))?;

            // Convert to future and await, then stringify the result
            let awaited_result: rquickjs::Value = result.into_future().await?;
            let json_str = ctx.json_stringify(awaited_result)?
                .map(|s| s.to_string().unwrap_or_default())
                .unwrap_or_default();

            Ok::<String, Error>(json_str)
        })
        .await
    }

    // Unified function call method for two arguments
    pub async fn call_function_with_two_args(&self, name: &str, arg1: &str, arg2: &str) -> StdResult<String, Error> {
        async_with!(self.context => |ctx| {
            // Get the function
            let globals = ctx.globals();
            let function: Function = globals.get(name)
                .or_else(|_| ctx.eval(name))?;

            // Convert arguments - use JSON parsing for complex data
            let js_arg1 = if arg1.starts_with('[') || arg1.starts_with('{') || arg1.starts_with('"') {
                ctx.json_parse(arg1)?
            } else {
                arg1.into_js(&ctx)?
            };
            
            let js_arg2 = if arg2.starts_with('[') || arg2.starts_with('{') || arg2.starts_with('"') {
                ctx.json_parse(arg2)?
            } else {
                arg2.into_js(&ctx)?
            };

            // Call the function and get a MaybePromise
            let result: MaybePromise = function.call((js_arg1, js_arg2))?;

            // Convert to future and await, then stringify the result
            let awaited_result: rquickjs::Value = result.into_future().await?;
            let json_str = ctx.json_stringify(awaited_result)?
                .map(|s| s.to_string().unwrap_or_default())
                .unwrap_or_default();

            Ok::<String, Error>(json_str)
        })
        .await
    }

    // LLRT-style idle method to wait for all async operations to complete
    pub async fn idle(&self) -> StdResult<(), Box<dyn std::error::Error + Send + Sync>> {
        // Add a timeout to prevent indefinite waiting
        let timeout_future = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.runtime.idle()
        );
        
        match timeout_future.await {
            Ok(_) => Ok(()),
            Err(_) => {
                eprintln!("Warning: VM idle timeout reached after 30 seconds");
                Ok(()) // Don't fail, just warn and continue
            }
        }
    }
}
