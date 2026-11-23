use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tokio::{net::TcpListener, sync::Semaphore};

use crate::{
    cli::Cli, clock::spawn_clock, session, shutdown::shutdown_signal, stats, store::Store,
};

pub async fn run(args: Cli) -> io::Result<()> {
    let ip = args.listen.unwrap_or(Ipv4Addr::UNSPECIFIED.into());
    let addr = SocketAddr::new(ip, args.port);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("failed to bind {addr}: {e}")))?;
    tracing::info!(addr = %addr, threads = args.threads, "listening");

    let shared_clock = spawn_clock();
    let store = Store::new(shared_clock, args.memory_limit_mb * 1024 * 1024)?;
    let connections = Arc::new(Semaphore::new(args.max_connections));

    tokio::select! {
        res = accept_loop(listener, store, connections) => res,
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, exiting");
            Ok(())
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    store: Store,
    connections: Arc<Semaphore>,
) -> io::Result<()> {
    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let permit = connections
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io::Error::other("connection semaphore closed"))?;

        if let Err(e) = socket.set_nodelay(true) {
            tracing::warn!(%addr, error = %e, "failed to set TCP_NODELAY");
        }

        tracing::info!(%addr, "connection accepted");
        stats::TOTAL_CONNECTIONS.add(1);
        stats::CURR_CONNECTIONS.add(1);

        let store = store.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let res = session::process(socket, store).await;
            stats::CURR_CONNECTIONS.sub(1);

            match res {
                Ok(()) => tracing::info!(%addr, "connection closed"),
                // The client went away
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    tracing::debug!(%addr, error = %e, "client disconnected")
                }
                Err(e) => tracing::warn!(%addr, error = %e, "connection error"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[tokio::test]
    async fn run_fails_when_port_is_taken() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = taken.local_addr().unwrap().port().to_string();
        let args = Cli::try_parse_from(["ousagi", "-l", "127.0.0.1", "-p", &port]).unwrap();

        // A timeout so a successful bind fails the test instead of serving forever.
        let res = tokio::time::timeout(Duration::from_secs(5), run(args)).await;
        let err = res.expect("run should fail fast").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    }
}
