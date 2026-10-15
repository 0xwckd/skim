use clap::Parser;

#[derive(Parser)]
#[command(
    name = "skim-robinhood",
    about = "Robinhood Chain selective RPC read cache (requires a Nitro RPC)"
)]
struct Cli {
    #[command(flatten)]
    args: skim_robinhood::Args,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    skim_robinhood::run(Cli::parse().args).await
}
