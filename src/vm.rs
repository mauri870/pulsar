use llrt_modules::module_builder::ModuleBuilder;
use rquickjs::{
    AsyncContext, AsyncRuntime, Error, FromJs, Function, async_with, function::IntoArgs,
    promise::MaybePromise,
};
use std::result::Result as StdResult;

pub struct VM {
    _runtime: AsyncRuntime,
    context: AsyncContext,
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

        Ok(Self {
            _runtime: runtime,
            context,
        })
    }

    pub async fn eval<F, R>(&self, f: F) -> R
    where
        F: FnOnce(rquickjs::Ctx) -> R + Send,
        R: Send + 'static,
    {
        async_with!(self.context => |ctx| {
            f(ctx)
        })
        .await
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

    pub async fn call_function_with_string(
        &self,
        name: &str,
        arg: String,
    ) -> StdResult<String, Error> {
        async_with!(self.context => |ctx| {
            // Get the function
            let globals = ctx.globals();
            let function: Function = globals.get(name)
                .or_else(|_| ctx.eval(name))?;

            // Call the function and get a MaybePromise
            let result: MaybePromise = function.call((arg,))?;

            // Convert to future and await, then stringify the result
            let awaited_result: rquickjs::Value = result.into_future().await?;
            let json_str = ctx.json_stringify(awaited_result)?
                .map(|s| s.to_string().unwrap_or_default())
                .unwrap_or_default();

            Ok::<String, Error>(json_str)
        })
        .await
    }

    pub async fn call_function_with_json_args(
        &self,
        name: &str,
        args: Vec<String>,
    ) -> StdResult<String, Error> {
        async_with!(self.context => |ctx| {
            // Get the function
            let globals = ctx.globals();
            let function: Function = globals.get(name)
                .or_else(|_| ctx.eval(name))?;

            // Parse JSON args into JavaScript values
            let mut js_args = Vec::new();
            for arg in args {
                let js_val: rquickjs::Value = ctx.json_parse(arg)?;
                js_args.push(js_val);
            }

            // Convert Vec to tuple for call
            match js_args.len() {
                0 => {
                    let result: MaybePromise = function.call(())?;
                    let awaited_result: rquickjs::Value = result.into_future().await?;
                    let json_str = ctx.json_stringify(awaited_result)?
                        .map(|s| s.to_string().unwrap_or_default())
                        .unwrap_or_default();
                    Ok(json_str)
                }
                1 => {
                    let result: MaybePromise = function.call((&js_args[0],))?;
                    let awaited_result: rquickjs::Value = result.into_future().await?;
                    let json_str = ctx.json_stringify(awaited_result)?
                        .map(|s| s.to_string().unwrap_or_default())
                        .unwrap_or_default();
                    Ok(json_str)
                }
                2 => {
                    let result: MaybePromise = function.call((&js_args[0], &js_args[1]))?;
                    let awaited_result: rquickjs::Value = result.into_future().await?;
                    let json_str = ctx.json_stringify(awaited_result)?
                        .map(|s| s.to_string().unwrap_or_default())
                        .unwrap_or_default();
                    Ok(json_str)
                }
                _ => Err(Error::Exception)
            }
        })
        .await
    }
}
