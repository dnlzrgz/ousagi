use clap::Parser;
use tracing_subscriber::EnvFilter;

use ousagi::{cli::Cli, server};

fn verbosity_level(v: u8) -> &'static str {
    match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    }
}

fn main() {
    let args = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(verbosity_level(args.verbose))),
        )
        .init();

    let rt = ousagi::runtime::build(args.threads);
    rt.block_on(server::run(args));
}
