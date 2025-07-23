use std::convert::TryInto;

use anyhow::Result;
use clap::Parser;
use log::debug;

fn main() -> Result<()> {
    env_logger::init();

    let cli = mapreduce::Cli::parse();
    debug!("Parsed command line arguments: {:?}", cli);
    let mr: mapreduce::MapReduce = cli.try_into()?;
    mr.run()
}