use crate::config::ScriptSource;
use crate::error::{Error, Result};

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");

/// Returns the source text of the configured user script.
pub async fn load(source: &ScriptSource) -> Result<String> {
    match source {
        ScriptSource::File(path) => {
            tokio::fs::read_to_string(path)
                .await
                .map_err(|source| Error::ReadScript {
                    path: path.clone(),
                    source,
                })
        }
        ScriptSource::Default => Ok(DEFAULT_SCRIPT.into()),
    }
}
