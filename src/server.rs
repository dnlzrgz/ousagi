use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{net::TcpListener, sync::Semaphore};

use crate::{
    cli::Cli, clock::spawn_clock, session, shutdown::shutdown_signal, stats, store::Store,
};

fn resolve_addr(args: &Cli) -> SocketAddr {
    let ip = args.listen.as_deref().unwrap_or("0.0.0.0");
    format!("{ip}:{}", args.port)
        .parse()
        .expect("invalid --listen/--port")
}

pub async fn run(args: Cli) {
    let addr = resolve_addr(&args);
    let listener = TcpListener::bind(addr).await.unwrap();
    tracing::info!(addr = %addr, threads = args.threads, "listening");

    let shared_clock = spawn_clock();
    let store: Store = Store::new(shared_clock, args.threads);
    let connections = Arc::new(Semaphore::new(args.max_connections));

    tokio::select! {
        _ = accept_loop(listener, store, connections) => {}
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, exiting");
        }
    }
}

async fn accept_loop(listener: TcpListener, store: Store, connections: Arc<Semaphore>) {
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
            .expect("semaphore is never closed");

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
                Err(e) => tracing::warn!(%addr, error = %e, "connection error"),
            }
        });
    }
}
