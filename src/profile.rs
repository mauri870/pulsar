use crate::error::{Error, Result};

/// CPU profiler that writes `pprof.pb` to the working directory.
pub struct Profiler {
    guard: pprof2::ProfilerGuard<'static>,
}

impl Profiler {
    pub fn start() -> Self {
        Profiler {
            guard: pprof2::ProfilerGuard::new(999).unwrap(),
        }
    }

    pub fn write_report(self) -> Result<()> {
        if let Ok(report) = self.guard.report().build() {
            use pprof2::protos::Message;
            use std::io::Write;
            let profile = report
                .pprof()
                .map_err(|e| Error::Profile(e.to_string()))?;
            let mut content = Vec::new();
            profile
                .encode(&mut content)
                .map_err(|e| Error::Profile(e.to_string()))?;
            let mut file =
                std::fs::File::create("pprof.pb").map_err(|e| Error::Profile(e.to_string()))?;
            file.write_all(&content)
                .map_err(|e| Error::Profile(e.to_string()))?;
        }
        Ok(())
    }
}
