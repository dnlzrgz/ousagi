use std::{net::SocketAddr, sync::Arc, time::Duration};

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
        0 => "warn",
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl+C"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}

async fn run(args: Args) {
    let addr = resolve_addr(&args);
    let listener = TcpListener::bind(addr).await.unwrap();
    tracing::info!(addr = %addr, threads = args.threads, "listening");

    let store: Store = Arc::new(StoreInner::new());

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, addr) = match accept_result {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                tracing::info!(%addr, "connection accepted");
                let store = store.clone();
                tokio::spawn(async move {
                    match connection::process(socket, store).await {
                        Ok(()) => tracing::info!(%addr, "connection closed"),
                        Err(e) => tracing::warn!(%addr, error = %e, "connection error"),
                    }
                });
            }
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received, exiting");
                break;
            }
        }
    }
}
