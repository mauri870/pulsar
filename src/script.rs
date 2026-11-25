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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_and_reduce_are_always_required() {
        assert_eq!(Contract::default().required_functions(), ["map", "reduce"]);
    }

    #[test]
    fn sort_and_test_are_required_on_request() {
        let sort = Contract { sort: true, test: false };
        assert_eq!(sort.required_functions(), ["map", "reduce", "sort"]);
        let test = Contract { sort: false, test: true };
        assert_eq!(test.required_functions(), ["map", "reduce", "test"]);
        let both = Contract { sort: true, test: true };
        assert_eq!(both.required_functions(), ["map", "reduce", "sort", "test"]);
    }

    #[tokio::test]
    async fn the_default_script_defines_map_and_reduce() {
        let source = load(&ScriptSource::Default).await.unwrap();
        assert!(source.contains("const map"));
        assert!(source.contains("const reduce"));
    }

    #[tokio::test]
    async fn a_missing_script_file_is_a_read_error() {
        let result = load(&ScriptSource::File("no/such/script.js".into())).await;
        assert!(matches!(result, Err(Error::ReadScript { .. })));
    }

    #[tokio::test]
    async fn a_script_file_is_read_as_is() {
        let path = std::env::temp_dir().join(format!("pulsar-script-{}.js", std::process::id()));
        std::fs::write(&path, "const map = 1;\n").unwrap();
        let source = load(&ScriptSource::File(path.to_str().unwrap().into())).await;
        std::fs::remove_file(&path).unwrap();
        assert_eq!(source.unwrap(), "const map = 1;\n");
    }
}
