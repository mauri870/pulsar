use futures::Stream;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;

use crate::config::InputSource;
use crate::error::{Error, Result};

type InputReader = BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>;

/// Where the engine reads its lines from.
pub trait Source: Send + 'static {
    /// The input as a stream of lines.
    fn into_lines(self) -> impl Stream<Item = std::io::Result<String>> + Send + Unpin;
}

/// A source backed by stdin or a file.
pub struct ReaderSource(InputReader);

impl Source for ReaderSource {
    fn into_lines(self) -> impl Stream<Item = std::io::Result<String>> + Send + Unpin {
        LinesStream::new(self.0.lines())
    }
}

/// Opens the configured input for reading.
pub async fn open(source: &InputSource) -> Result<ReaderSource> {
    match source {
        InputSource::Stdin => {
            let stdin = tokio::io::stdin();
            Ok(ReaderSource(BufReader::new(Box::new(stdin))))
        }
        InputSource::File(path) => {
            let file = tokio::fs::File::open(path)
                .await
                .map_err(|source| Error::OpenInput {
                    path: path.clone(),
                    source,
                })?;
            Ok(ReaderSource(BufReader::new(Box::new(file))))
        }
    }
}
