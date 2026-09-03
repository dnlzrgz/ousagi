use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ousagi::commands::Response;
use ousagi::connection::Connection;
use ousagi::runtime;
use tokio::runtime::Runtime;

#[inline]
fn rt() -> Runtime {
    runtime::build(1)
}

/// Leaks `bytes` to get a `'static` slice. Runs some times at setup, not per-iteration.
fn leak(bytes: Vec<u8>) -> &'static [u8] {
    Box::leak(bytes.into_boxed_slice())
}

fn setup(request: &'static [u8]) -> Connection<&'static [u8], Vec<u8>> {
    Connection::new(request, Vec::new())
}

fn bench_read_line(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("read_line");

    let short = b"get foo\r\n";
    group.throughput(Throughput::Bytes(short.len() as u64));
    group.bench_function("short_key", |b| {
        b.to_async(&rt).iter_batched(
            || setup(short),
            |mut conn| async move { black_box(conn.read_line().await.unwrap()) },
            BatchSize::SmallInput,
        );
    });

    let long_key_text = format!("get {}\r\n", "k".repeat(250));
    let long_key = leak(long_key_text.into_bytes());
    group.throughput(Throughput::Bytes(long_key.len() as u64));
    group.bench_function("long_key", |b| {
        b.to_async(&rt).iter_batched(
            || setup(long_key),
            |mut conn| async move { black_box(conn.read_line().await.unwrap()) },
            BatchSize::SmallInput,
        );
    });

    for &n in &[10usize, 100, 1_000] {
        let text: String = std::iter::repeat("get foo\r\n").take(n).collect();
        let request = leak(text.into_bytes());
        group.throughput(Throughput::Bytes(request.len() as u64));
        group.bench_with_input(BenchmarkId::new("pipelined", n), &request, |b, &request| {
            b.to_async(&rt).iter_batched(
                || setup(request),
                |mut conn| async move {
                    for _ in 0..n {
                        black_box(conn.read_line().await.unwrap());
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn payload_request(len: usize) -> Vec<u8> {
    let mut buf = vec![b'x'; len];
    buf.extend_from_slice(b"\r\n");
    buf
}

fn bench_read_payload(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("read_payload");

    for &len in &[64usize, 512, 4096, 65_536] {
        let request = leak(payload_request(len));
        group.throughput(Throughput::Bytes(request.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("value_size", len),
            &request,
            |b, &request| {
                b.to_async(&rt).iter_batched(
                    || setup(request),
                    move |mut conn| async move { black_box(conn.read_payload(len).await.unwrap()) },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn command_with_payload(header: &str, len: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend(std::iter::repeat(b'x').take(len));
    buf.extend_from_slice(b"\r\n");
    buf
}

fn bench_read_command(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("read_command");

    let get = b"get foo\r\n";
    group.throughput(Throughput::Bytes(get.len() as u64));
    group.bench_function("get", |b| {
        b.to_async(&rt).iter_batched(
            || setup(get),
            |mut conn| async move { black_box(conn.read_command().await.unwrap()) },
            BatchSize::SmallInput,
        );
    });

    let set_512 = leak(command_with_payload("set foo 0 0 512", 512));
    group.throughput(Throughput::Bytes(set_512.len() as u64));
    group.bench_function("set_512b", |b| {
        b.to_async(&rt).iter_batched(
            || setup(set_512),
            |mut conn| async move { black_box(conn.read_command().await.unwrap()) },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_write_response(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("write_response");

    group.bench_function("stored", |b| {
        b.to_async(&rt).iter_batched(
            || setup(b""),
            |mut conn| async move {
                conn.write_response(&Response::Stored).await.unwrap();
                black_box(conn.write_response(&Response::Stored).await.unwrap());
            },
            BatchSize::SmallInput,
        );
    });

    for &n in &[1usize, 10, 100] {
        let values: Vec<_> = (0..n)
            .map(|i| {
                (
                    Bytes::from(format!("key:{i}")),
                    0u32,
                    Bytes::from_static(b"a value stored in the cache"),
                    None,
                )
            })
            .collect();
        let resp = Response::Values(values);

        group.bench_with_input(BenchmarkId::new("values", n), &resp, |b, resp| {
            b.to_async(&rt).iter_batched(
                || setup(b""),
                |mut conn| async move { black_box(conn.write_response(resp).await.unwrap()) },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_read_line,
    bench_read_payload,
    bench_read_command,
    bench_write_response,
);
criterion_main!(benches);
