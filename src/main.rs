use anyhow::Result;
use clap::Parser;
use log::debug;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = pulsar::Cli::parse();
    debug!("Parsed command line arguments: {:?}", cli);
    let mr = pulsar::Pulsar::from_cli(cli).await?;
    mr.run().await
}