use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ousagi::clock::Clock;
use ousagi::commands::{ArithmeticOp, Response, StoreArgs, StoreOp};
use ousagi::store::Store;

const THREADS: usize = 4;

fn empty_store() -> Store {
    Store::new(Clock::mock(1_000_000), THREADS)
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
            cas: None,
        },
    );
    store
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_get");
    let key = [Bytes::from_static(b"foo")];

    let hit_store = populated_store("foo", b"a value stored in the cache");
    group.bench_function("hit", |b| {
        b.iter(|| black_box(hit_store.get(black_box(&key), false)))
    });

    let miss_store = empty_store();
    group.bench_function("miss", |b| {
        b.iter(|| black_box(miss_store.get(black_box(&key), false)))
    });

    group.finish();
}

fn bench_get_and_touch(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_get_and_touch");
    let key = [Bytes::from_static(b"foo")];

    group.bench_function("hit", |b| {
        b.iter_batched(
            || populated_store("foo", b"a value stored in the cache"),
            |store| black_box(store.get_and_touch(black_box(&key), 100, false)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn store_args(key: &Bytes, data: &[u8], cas: Option<u64>) -> StoreArgs {
    StoreArgs {
        key: key.clone(),
        flags: 0,
        exptime: 0,
        data: Bytes::copy_from_slice(data),
        noreply: false,
        cas,
    }
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_store");
    let key = Bytes::from_static(b"foo");

    group.bench_function("set", |b| {
        b.iter_batched(
            empty_store,
            |store| black_box(store.store(StoreOp::Set, store_args(&key, b"value", None))),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("add_new_key", |b| {
        b.iter_batched(
            empty_store,
            |store| black_box(store.store(StoreOp::Add, store_args(&key, b"value", None))),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("append", |b| {
        b.iter_batched(
            || populated_store("foo", b"value"),
            |store| black_box(store.store(StoreOp::Append, store_args(&key, b" more", None))),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_cas(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_cas");
    let key = Bytes::from_static(b"foo");

    group.bench_function("success", |b| {
        b.iter_batched(
            || {
                let store = populated_store("foo", b"value");
                let cas = match store.get(&[key.clone()], true) {
                    Response::Values(values) => values[0].3.expect("cas token"),
                    other => panic!("expected Values, got {other:?}"),
                };
                (store, cas)
            },
            |(store, cas)| {
                black_box(store.store(StoreOp::Cas, store_args(&key, b"new value", Some(cas))))
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
        b.iter_batched(
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
        b.iter_batched(
            || populated_store("foo", b"10"),
            |store| black_box(store.arithmetic(ArithmeticOp::Incr, black_box(&key), 5)),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("decr", |b| {
        b.iter_batched(
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
        b.iter_batched(
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
        b.iter_batched(
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
