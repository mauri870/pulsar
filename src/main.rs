use anyhow::Result;
use clap::Parser;
use log::debug;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = mapreduce::Cli::parse();
    debug!("Parsed command line arguments: {:?}", cli);
    let mr = mapreduce::MapReduce::from_cli(cli).await?;
    mr.run().await
}