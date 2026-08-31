use std::hint::black_box;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ousagi::parser::parse_command_line;

fn line(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_get");

    let single = line("get foo");
    group.throughput(Throughput::Bytes(single.len() as u64));
    group.bench_function("single_key", |b| {
        b.iter(|| parse_command_line(black_box(&single)));
    });

    let gets = line("gets foo");
    group.throughput(Throughput::Bytes(gets.len() as u64));
    group.bench_function("single_key_with_cas", |b| {
        b.iter(|| parse_command_line(black_box(&gets)))
    });

    for &n in &[10usize, 100, 1000] {
        let keys: Vec<String> = (0..n).map(|i| format!("key:{i}")).collect();
        let text = format!("get {}", keys.join(" "));
        let bytes = line(&text);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("multiple_keys", n), &bytes, |b, bytes| {
            b.iter(|| parse_command_line(black_box(bytes)))
        });
    }

    group.finish();
}

fn bench_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_store");

    let set = line("set foo 0 0 512");
    group.throughput(Throughput::Bytes(set.len() as u64));
    group.bench_function("set", |b| b.iter(|| parse_command_line(black_box(&set))));

    let add = line("add foo 0 0 512");
    group.bench_function("add", |b| b.iter(|| parse_command_line(black_box(&add))));

    let append = line("append foo 0 0 512");
    group.bench_function("append", |b| {
        b.iter(|| parse_command_line(black_box(&append)))
    });

    let prepend = line("prepend foo 0 0 512");
    group.bench_function("prepend", |b| {
        b.iter(|| parse_command_line(black_box(&prepend)))
    });

    let cas = line("cas foo 0 0 512 123456789");
    group.bench_function("cas", |b| b.iter(|| parse_command_line(black_box(&cas))));

    let noreply = line("set foo 0 0 512 noreply");
    group.bench_function("set_noreply", |b| {
        b.iter(|| parse_command_line(black_box(&noreply)))
    });

    let long_key_text = format!("set {} 0 0 512", "k".repeat(250));
    let long_key = line(&long_key_text);
    group.bench_function("set_max_key_len", |b| {
        b.iter(|| parse_command_line(black_box(&long_key)))
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_delete");

    let bytes = line("delete foo");
    group.bench_function("existing_style_key", |b| {
        b.iter(|| parse_command_line(black_box(&bytes)))
    });

    let noreply = line("delete foo noreply");
    group.bench_function("noreply", |b| {
        b.iter(|| parse_command_line(black_box(&noreply)))
    });

    group.finish();
}

fn bench_arithmetic(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_arithmetic");

    let incr = line("incr foo 5");
    group.bench_function("incr", |b| b.iter(|| parse_command_line(black_box(&incr))));

    let decr = line("decr foo 5");
    group.bench_function("decr", |b| b.iter(|| parse_command_line(black_box(&decr))));

    group.finish();
}

fn bench_flush_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_flush_all");

    let bare = line("flush_all");
    group.bench_function("bare", |b| b.iter(|| parse_command_line(black_box(&bare))));

    let delayed = line("flush_all 3600");
    group.bench_function("with_delay", |b| {
        b.iter(|| parse_command_line(black_box(&delayed)))
    });

    let noreply = line("flush_all noreply");
    group.bench_function("noreply", |b| {
        b.iter(|| parse_command_line(black_box(&noreply)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_store,
    bench_delete,
    bench_arithmetic,
    bench_flush_all,
);

criterion_main!(benches);
