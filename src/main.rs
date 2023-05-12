use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
struct Args {
    /// Port to run the server
    #[arg(short, long, default_value_t = 7878)]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!(port = args.port, "Running rog");
}
