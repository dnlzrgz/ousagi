#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ousagi::clock::Clock;
use ousagi::commands::{ArithmeticOp, StoreArgs, StoreOp};
use ousagi::store::Store;

/// The server's default `-m 64`, so the cache gets the same shard count as in production.
const MEMORY_LIMIT: u64 = 64 * 1024 * 1024;

fn empty_store() -> Store {
    Store::new(Clock::mock(1_000_000), MEMORY_LIMIT).unwrap()
}

fn populated_store(key: &str, data: &[u8]) -> Store {
    let store = empty_store();
    store.store(
        StoreOp::Set,
        StoreArgs {
            key: Bytes::copy_from_slice(key.as_bytes()),
            flags: 0,
            exptime: 0,
            data: Bytes::copy_from_slice(data),
            noreply: false,
        },
    );
    store
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_get");
    let key = [Bytes::from_static(b"foo")];

    let hit_store = populated_store("foo", b"a value stored in the cache");
    group.bench_function("hit", |b| {
        b.iter(|| hit_store.get(black_box(&key), false, |value| _ = black_box(value)))
    });

    let miss_store = empty_store();
    group.bench_function("miss", |b| {
        b.iter(|| miss_store.get(black_box(&key), false, |value| _ = black_box(value)))
    });

    group.finish();
}

fn bench_get_and_touch(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_get_and_touch");
    let key = [Bytes::from_static(b"foo")];

    group.bench_function("hit", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"a value stored in the cache"),
            |store| store.get_and_touch(black_box(&key), 100, false, |value| _ = black_box(value)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn store_args(key: &Bytes, data: &'static [u8]) -> StoreArgs {
    StoreArgs {
        key: key.clone(),
        flags: 0,
        exptime: 0,
        data: Bytes::from_static(data),
        noreply: false,
    }
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_store");
    let key = Bytes::from_static(b"foo");

    group.bench_function("set", |b| {
        b.iter_batched_ref(
            empty_store,
            |store| black_box(store.store(StoreOp::Set, store_args(&key, b"value"))),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("add_new_key", |b| {
        b.iter_batched_ref(
            empty_store,
            |store| black_box(store.store(StoreOp::Add, store_args(&key, b"value"))),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("append", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"value"),
            |store| black_box(store.store(StoreOp::Append, store_args(&key, b" more"))),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_cas(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_cas");
    let key = Bytes::from_static(b"foo");

    group.bench_function("success", |b| {
        b.iter_batched_ref(
            || {
                let store = populated_store("foo", b"value");
                let mut cas = None;
                store.get(std::slice::from_ref(&key), true, |value| cas = value.cas);
                let cas = cas.expect("cas token");
                (store, cas)
            },
            |(store, cas)| {
                black_box(store.store(StoreOp::Cas(*cas), store_args(&key, b"new value")))
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_delete");
    let key = Bytes::from_static(b"foo");

    group.bench_function("existing_key", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"value"),
            |store| black_box(store.delete(black_box(&key))),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_arithmetic(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_arithmetic");
    let key = Bytes::from_static(b"foo");

    group.bench_function("incr", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"10"),
            |store| black_box(store.arithmetic(ArithmeticOp::Incr, black_box(&key), 5)),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("decr", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"10"),
            |store| black_box(store.arithmetic(ArithmeticOp::Decr, black_box(&key), 5)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_touch(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_touch");
    let key = Bytes::from_static(b"foo");

    group.bench_function("existing_key", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"value"),
            |store| black_box(store.touch(black_box(&key), 100)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_flush");

    group.bench_function("immediate", |b| {
        b.iter_batched_ref(
            || populated_store("foo", b"value"),
            |store| black_box(store.flush(None)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_get_and_touch,
    bench_set,
    bench_cas,
    bench_delete,
    bench_arithmetic,
    bench_touch,
    bench_flush,
);

criterion_main!(benches);
