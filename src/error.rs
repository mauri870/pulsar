use std::io;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to open file {path}: {source}")]
    OpenInput { path: String, source: io::Error },

    #[error("Error reading input: {0}")]
    ReadInput(io::Error),

    #[error("Failed to read script file {path}: {source}")]
    ReadScript { path: String, source: io::Error },

    #[error("Error loading JS code: {0}")]
    ScriptLoad(String),

    #[error("Invalid script: {0}")]
    ScriptContract(String),

    #[error("{0}")]
    Map(String),

    #[error("{0}")]
    Reduce(String),

    #[error("{0}")]
    Sort(String),

    #[error("{0}")]
    Test(String),

    #[error("Error writing output: {0}")]
    WriteOutput(io::Error),

    #[error("Group storage error: {0}")]
    Spill(io::Error),

    #[error("Serialization error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("Profiling error: {0}")]
    Profile(String),

    #[error("{0}")]
    Worker(String),

    #[error("{0}")]
    Unsupported(String),
}

impl Error {
    /// Whether the reader of our stdout went away, as with `pulsar | head`.
    pub fn is_broken_pipe(&self) -> bool {
        matches!(self, Error::WriteOutput(e) if e.kind() == io::ErrorKind::BrokenPipe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_broken_pipe_on_output_is_recognised() {
        let error = Error::WriteOutput(io::Error::from(io::ErrorKind::BrokenPipe));
        assert!(error.is_broken_pipe());
    }

    #[test]
    fn other_errors_are_not_a_broken_pipe() {
        assert!(!Error::WriteOutput(io::Error::from(io::ErrorKind::PermissionDenied)).is_broken_pipe());
        assert!(!Error::ReadInput(io::Error::from(io::ErrorKind::BrokenPipe)).is_broken_pipe());
        assert!(!Error::Map("boom".into()).is_broken_pipe());
    }

    #[test]
    fn messages_name_the_failure() {
        let open = Error::OpenInput {
            path: "in.txt".into(),
            source: io::Error::from(io::ErrorKind::NotFound),
        };
        assert!(open.to_string().starts_with("Failed to open file in.txt: "));
        assert_eq!(
            Error::ScriptContract("the script must define a `map` function".into()).to_string(),
            "Invalid script: the script must define a `map` function"
        );
        assert_eq!(Error::ScriptLoad("bad".into()).to_string(), "Error loading JS code: bad");
        assert_eq!(Error::Map("map failed".into()).to_string(), "map failed");
    }
}
