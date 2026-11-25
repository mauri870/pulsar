use anyhow::Result;
use tokio::io::BufReader;

use crate::config::InputSource;

pub type InputReader = BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>;

/// Opens the configured input for reading.
pub async fn open(source: &InputSource) -> Result<InputReader> {
    match source {
        InputSource::Stdin => {
            let stdin = tokio::io::stdin();
            Ok(BufReader::new(Box::new(stdin)))
        }
        InputSource::File(path) => {
            let file = tokio::fs::File::open(path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open file {}: {}", path, e))?;
            Ok(BufReader::new(Box::new(file)))
        }
    }
}
