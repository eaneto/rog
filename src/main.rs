use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
struct Args {
    /// Port to run the server
    #[arg(short, long, default_value_t = 7878)]
    port: u16,
}

struct _Message {
    log: String,
    partition: Option<usize>,
    payload: Bytes,
    timestamp: Instant,
}

#[derive(Debug)]
struct CommitLog {
    _name: String,
    _partitions: usize,
}

impl CommitLog {
    fn _new(name: String, partitions: usize) -> CommitLog {
        CommitLog {
            _name: name,
            _partitions: partitions,
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!(port = args.port, "Running rog");

    // TODO Create log command
    // TODO Produce to log
    // TODO Subscribe to log
    // TODO Enable multiple subscribers to same log
    // TODO Distribute writes to multiple nodes

    loop {}
}
