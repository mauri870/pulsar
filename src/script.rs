use crate::config::ScriptSource;
use crate::error::{Error, Result};

const DEFAULT_SCRIPT: &str = include_str!("../default_script.js");

/// The functions a run needs the user script to define.
#[derive(Debug, Clone, Copy, Default)]
pub struct Contract {
    /// `--sort` needs a `sort` function.
    pub sort: bool,
    /// `--test` needs a `test` function.
    pub test: bool,
}

impl Contract {
    /// Names of the functions the script must define. `combine` is optional.
    pub fn required_functions(&self) -> Vec<&'static str> {
        let mut names = vec!["map", "reduce"];
        if self.sort {
            names.push("sort");
        }
        if self.test {
            names.push("test");
        }
        names
    }
}

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
