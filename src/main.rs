use std::process::ExitCode;

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

fn main() -> ExitCode {
    let args = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(verbosity_level(args.verbose))),
        )
        .init();

    let res = ousagi::runtime::build(args.threads)
        .map_err(|e| std::io::Error::new(e.kind(), format!("failed to build runtime: {e}")))
        .and_then(|rt| rt.block_on(server::run(args)));

    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "exiting");
            ExitCode::FAILURE
        }
    }
}
