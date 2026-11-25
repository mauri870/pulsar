use clap::Parser;
use std::process::ExitCode;
use tracing::debug;
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> ExitCode {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cli = pulsar::Cli::parse();
    debug!("Parsed command line arguments: {:?}", cli);
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_broken_pipe() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {}", error);
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: pulsar::Cli) -> pulsar::Result<()> {
    let mr = pulsar::Pulsar::from_config(pulsar::Config::from(cli)).await?;
    mr.run().await
}
