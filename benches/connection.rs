#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ousagi::commands::{Response, Value};
use ousagi::connection::Connection;
use ousagi::output::Output;
use ousagi::runtime;
use tokio::runtime::Runtime;

#[inline]
fn rt() -> Runtime {
    runtime::build(1).unwrap()
}

/// Leaks `bytes` to get a `'static` slice. Doesn't run per-iteration.
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
            |mut conn| async move {
                let line = conn.read_line().await.unwrap();
                (conn, line)
            },
            BatchSize::SmallInput,
        );
    });

    let long_key_text = format!("get {}\r\n", "k".repeat(250));
    let long_key = leak(long_key_text.into_bytes());
    group.throughput(Throughput::Bytes(long_key.len() as u64));
    group.bench_function("long_key", |b| {
        b.to_async(&rt).iter_batched(
            || setup(long_key),
            |mut conn| async move {
                let line = conn.read_line().await.unwrap();
                (conn, line)
            },
            BatchSize::SmallInput,
        );
    });

    for &n in &[10usize, 100, 1_000] {
        let text = "get foo\r\n".repeat(n);
        let request = leak(text.into_bytes());
        group.throughput(Throughput::Bytes(request.len() as u64));
        group.bench_with_input(BenchmarkId::new("pipelined", n), &request, |b, &request| {
            b.to_async(&rt).iter_batched(
                || setup(request),
                |mut conn| async move {
                    for _ in 0..n {
                        black_box(conn.read_line().await.unwrap());
                    }
                    conn
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
                    move |mut conn| async move {
                        let data = conn.read_payload(len).await.unwrap();
                        (conn, data)
                    },
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
    buf.resize(buf.len() + len, b'x');
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
            |mut conn| async move {
                let cmd = conn.read_command().await.unwrap();
                (conn, cmd)
            },
            BatchSize::SmallInput,
        );
    });

    let set_512 = leak(command_with_payload("set foo 0 0 512", 512));
    group.throughput(Throughput::Bytes(set_512.len() as u64));
    group.bench_function("set_512b", |b| {
        b.to_async(&rt).iter_batched(
            || setup(set_512),
            |mut conn| async move {
                let cmd = conn.read_command().await.unwrap();
                (conn, cmd)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Queues responses and sends them like a `Connection` before next read.
fn bench_write_response(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("write_response");

    group.bench_function("stored", |b| {
        b.to_async(&rt).iter_batched(
            || (Output::default(), Vec::with_capacity(64)),
            |(mut out, mut writer)| async move {
                out.response(&Response::Stored);
                out.response(&Response::Stored);
                out.write_to(&mut writer).await.unwrap();
                (out, writer)
            },
            BatchSize::SmallInput,
        );
    });

    for &n in &[1usize, 10, 100] {
        let keys: Vec<_> = (0..n).map(|i| format!("key:{i}").into_bytes()).collect();
        let data = Bytes::from_static(b"a value stored in the cache");

        group.bench_with_input(BenchmarkId::new("values", n), &keys, |b, keys| {
            b.to_async(&rt).iter_batched(
                || (Output::default(), Vec::with_capacity(64)),
                |(mut out, mut writer)| {
                    let data = &data;
                    async move {
                        for key in keys {
                            out.value(Value {
                                key,
                                flags: 0,
                                data,
                                cas: None,
                            });
                        }
                        out.response(&Response::End);
                        out.write_to(&mut writer).await.unwrap();
                        (out, writer)
                    }
                },
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
