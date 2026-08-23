use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use clap::{ArgAction, Parser};
use ousagi::{
    connection,
    store::{Store, StoreInner},
};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
pub struct Args {
    /// TCP port to listen on (0 to disable)
    #[arg(short = 'p', long, default_value_t = 11211)]
    pub port: u16,

    /// Interface to listen on, default INADDR_ANY
    #[arg(short = 'l', long)]
    pub listen: Option<String>,

    /// Number of threads to process incoming requests
    #[arg(short = 't', long, default_value_t = 4, value_parser = parse_threads)]
    pub threads: usize,

    /// Verbosity level
    #[arg(short = 'v', action = ArgAction::Count)]
    pub verbose: u8,
}

fn resolve_addr(args: &Args) -> SocketAddr {
    let ip = args.listen.as_deref().unwrap_or("0.0.0.0");
    format!("{ip}:{}", args.port)
        .parse()
        .expect("invalid --listen/--port")
}

fn parse_threads(s: &str) -> Result<usize, String> {
    let threads: usize = s
        .parse()
        .map_err(|_| format!("'{s}' isn't a valid number"))?;

    if threads == 0 {
        return Err("thread count must be at least 1".to_string());
    }

    Ok(threads)
}

fn verbosity_level(v: u8) -> &'static str {
    match v {
        0 => "warm",
        1 => "info",
        _ => "debug",
    }
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(verbosity_level(args.verbose))),
        )
        .init();

    let rt = ousagi::runtime::build(args.threads);
    rt.block_on(run(args));
}

async fn run(args: Args) {
    let addr = resolve_addr(&args);
    let listener = TcpListener::bind(addr).await.unwrap();
    tracing::info!(addr = %addr, threads = args.threads, "listening");

    let store: Store = Arc::new(RwLock::new(StoreInner::new()));

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        tracing::info!(%addr, "accepted connection");

        let store = store.clone();
        tokio::spawn(async move {
            match connection::process(socket, store).await {
                Ok(()) => tracing::info!(%addr, "connection closed"),
                Err(e) => tracing::warn!(%addr, error = %e, "connection error"),
            }
        });
    }
}
