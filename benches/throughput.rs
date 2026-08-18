use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ousagi::{connection, store::Store};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};

/// Number of operations performed by each client in a concurrent benchmark.
const OPS_PER_CLIENT: usize = 50;

/// Number of concurrent clients to test.
const CLIENT_COUNTS: &[usize] = &[1, 4, 16, 32, 64, 128];

/// Number of keys that are populated before read benchmarks run.
const READ_KEYSPACE: usize = 256;

/// Creates the Tokio runtime used by the benchmarks.
fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Starts a real server on an OS-assigned port and returns its address.
async fn spawn_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store: Store = Arc::new(RwLock::new(HashMap::new()));

    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let store = store.clone();
            tokio::spawn(async move {
                let _ = connection::process(socket, store).await;
            });
        }
    });

    addr
}

/// Sends one command and waits for one response.
async fn roundtrip(stream: &mut TcpStream, cmd: &str, buf: &mut [u8]) {
    stream.write_all(cmd.as_bytes()).await.unwrap();
    let _ = stream.read(buf).await.unwrap();
}

fn set_cmd(key: &str) -> String {
    format!("set {key} 0 0 3\r\nbar\r\n")
}

fn get_cmd(key: &str) -> String {
    format!("get {key}\r\n")
}

/// Fills the read keyspace before running the read-heavy benchmarks.
async fn populate(addr: SocketAddr) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 256];
    for i in 0..READ_KEYSPACE {
        roundtrip(&mut stream, &set_cmd(&format!("key{i}")), &mut buf).await;
    }
}

/// Measures the cost of establishing a new TCP connection.
fn bench_connection_setup(c: &mut Criterion) {
    let rt = runtime();
    let addr = rt.block_on(spawn_server());

    c.bench_function("connection_setup", |b| {
        b.to_async(&rt)
            .iter(|| async move { TcpStream::connect(addr).await.unwrap() });
    });
}

/// Benchmarks GET and SET operations using a single persistent client.
fn bench_single_client(c: &mut Criterion) {
    let rt = runtime();
    let addr = rt.block_on(spawn_server());
    rt.block_on(populate(addr));

    let mut group = c.benchmark_group("single_client");

    group.bench_function("set", |b| {
        b.to_async(&rt).iter_custom(move |iters| async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let mut buf = [0u8; 256];
            let start = Instant::now();
            for i in 0..iters {
                roundtrip(&mut stream, &set_cmd(&format!("bench-set-{i}")), &mut buf).await;
            }
            start.elapsed()
        });
    });

    group.bench_function("get", |b| {
        b.to_async(&rt).iter_custom(move |iters| async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let mut buf = [0u8; 256];
            let start = Instant::now();
            for i in 0..iters {
                let key = format!("key{}", i as usize % READ_KEYSPACE);
                roundtrip(&mut stream, &get_cmd(&key), &mut buf).await;
            }
            start.elapsed()
        });
    });

    group.finish();
}

#[derive(Clone, Copy)]
enum Workload {
    ReadHeavy,
    WriteHeavy,
}

/// Runs one client for a fixed number of operations.
async fn run_client(addr: SocketAddr, client_id: usize, workload: Workload) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 256];

    for i in 0..OPS_PER_CLIENT {
        let cmd = match workload {
            Workload::ReadHeavy => get_cmd(&format!("key{}", i % READ_KEYSPACE)),
            Workload::WriteHeavy => set_cmd(&format!("key-{client_id}-{i}")),
        };
        roundtrip(&mut stream, &cmd, &mut buf).await;
    }
}

/// Runs one concurrent batch and returns the total time spent running it.
async fn run_concurrent_batch(
    addr: SocketAddr,
    clients: usize,
    iters: u64,
    workload: Workload,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        let start = Instant::now();

        let handles: Vec<_> = (0..clients)
            .map(|client_id| tokio::spawn(run_client(addr, client_id, workload)))
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        total += start.elapsed();
    }

    total
}

/// Measures how throughput scales with increasing concurrency.
fn bench_concurrent_throughput(c: &mut Criterion) {
    let rt = runtime();
    let addr = rt.block_on(spawn_server());
    rt.block_on(populate(addr));

    let mut group = c.benchmark_group("concurrent_throughput");

    for &clients in CLIENT_COUNTS {
        group.throughput(Throughput::Elements((clients * OPS_PER_CLIENT) as u64));

        group.bench_with_input(
            BenchmarkId::new("read_heavy", clients),
            &clients,
            |b, &clients| {
                b.to_async(&rt).iter_custom(move |iters| {
                    run_concurrent_batch(addr, clients, iters, Workload::ReadHeavy)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("write_heavy", clients),
            &clients,
            |b, &clients| {
                b.to_async(&rt).iter_custom(move |iters| {
                    run_concurrent_batch(addr, clients, iters, Workload::WriteHeavy)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_connection_setup,
    bench_single_client,
    bench_concurrent_throughput
);
criterion_main!(benches);
