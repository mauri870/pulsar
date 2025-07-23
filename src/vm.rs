use rquickjs::{Context, Ctx, Runtime};

use std::result::Result as StdResult;

pub struct VM {
    _runtime: Runtime,
    ctx: Context,
}

impl VM {
    pub fn new() -> StdResult<Self, Box<dyn std::error::Error + Send + Sync>> {
        let runtime = Runtime::new().unwrap();
        runtime.set_max_stack_size(256 * 1024);
        let ctx = Context::full(&runtime)?;

        Ok(Self {
            _runtime: runtime,
            ctx,
        })
    }

    pub fn eval<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Ctx) -> R,
    {
        self.ctx.with(f)
    }
}