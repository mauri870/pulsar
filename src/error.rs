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
    Spill(#[from] sled::Error),

    #[error("Serialization error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("Profiling error: {0}")]
    Profile(String),

    #[error("{0}")]
    Worker(String),
}
