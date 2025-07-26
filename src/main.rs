use anyhow::Result;
use clap::Parser;
use tracing::debug;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cli = pulsar::Cli::parse();
    debug!("Parsed command line arguments: {:?}", cli);
    let mr = pulsar::Pulsar::from_cli(cli).await?;
    mr.run().await
}
