# Ousagi (大兎 Ōusagi)

A [memcached text protocol](https://docs.memcached.org/protocols/basic/) clone written in Rust, built on top of [tokio](https://tokio.rs) and [quick_cache](https://github.com/arthurprs/quick-cache).

## Motivation

All my career I have been working with caching services on some level or another, with only a conceptual understanding of how they work. Recently, with some free time on my hands, I thought it would be nice to learn more about them and really grasp the nitty-gritty details of what something like memcached is actually doing.

This is just a learning project. The initial goal was to get some level of parity with memcached, but somewhere along the way I started digging deeper into the low-level code and trying to figure out how to push its performance as far as possible.

To my surprise, the end result is still quite small: around 1,400 lines of code (plus tests and benchmarks).

## AI usage

AI assistance was used only for research, testing, and documentation.

## Features

- Storage: `set`, `add`, `replace`, `append`, `prepend`, `cas`.
- Retrieval: `get`, `gets`, `gat`, `gats`.
- `delete`, `incr`, `decr`, `touch`, `flush_all [delay]`.
- `stats`, `version`, `verbosity` (accepted, does nothing) and `quit`.
- `noreply` on every command that supports it.
- A memory limit with eviction (`-m`), a connection limit (`-c`) and a configurable number of worker threads (`-t`).
- memcached's default limits: keys up to 250 bytes, values up to 1 MiB.

## Usage

```console
$ cargo run --release -- -p 11211 -m 64 -t 4
```

Then talk to it with any memcached client, or by hand (`-C` makes `nc` send the `\r\n` line endings the protocol expects):

```console
$ nc -C localhost 11211
set greeting 0 0 5
hello
STORED
get greeting
VALUE greeting 0 5
hello
END
set counter 0 0 1
1
STORED
incr counter 41
42
gets greeting
VALUE greeting 0 5 1
hello
END
delete greeting
DELETED
```

| Option | Default | Description |
| --- | --- | --- |
| `-p, --port` | `11211` | TCP port to listen on |
| `-l, --listen` | `0.0.0.0` | Interface to listen on (IPv4 or IPv6) |
| `-t, --threads` | `4` | Worker threads (`1` uses a single-threaded runtime) |
| `-c, --max-connections` | `1024` | Max simultaneous client connections |
| `-m, --memory-limit-mb` | `64` | Memory for cached items, in megabytes |
| `-v, --verbose` | `warn` | `-v` info, `-vv` debug, `-vvv` trace. `RUST_LOG` overrides it. |

There is also a Docker image:

```console
$ docker build -f docker/Dockerfile -t ousagi .
$ docker run --rm -p 11211:11211 ousagi
```

## Performance

[memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) 2.6.1 against both servers, each with 4 threads in a container limited to 4 CPUs and 1 GiB:

- 200 clients (4 threads × 50), 128-byte values, 100k keys.
- 1 `set` per 10 `get`s, for 30 seconds after warming up the keyspace.
- Intel Core i7-13650HX (20 threads), Linux. Three runs per server, alternating between servers.

| Server | Ops/sec (median) | Range | Avg latency | p99 latency |
| --- | --- | --- | --- | --- |
| memcached 1.6.45 | 605k | 601k - 694k | 0.330 ms | 0.895 ms |
| ousagi | 611k | 562k - 696k | 0.327 ms | 0.783 ms |

On this load, ousagi and memcached are about even. To reproduce it, run `scripts/bench.sh` and `scripts/bench.sh -t memcached`.

Criterion micro-benchmarks (single thread, no network, medians):

| Benchmark | What it measures | Time |
| --- | --- | --- |
| `parse_get/single_key` | parse `get foo` | 25 ns |
| `parse_store/set` | parse `set foo 0 0 512` | 28 ns |
| `store_get/hit` | `get` of an existing key | 44 ns |
| `store_get/miss` | `get` of a missing key | 25 ns |
| `store_store/set` | `set` of a new key | 85 ns |
| `store_get_and_touch/hit` | `gat` of an existing key | 59 ns |
| `store_arithmetic/incr` | `incr` of an existing key | 70 ns |
| `read_line/pipelined/1000` | read 1,000 pipelined `get foo` lines | 20.9 µs (~21 ns per line) |
| `write_response/values/10` | encode a 10-value `get` response | 235 ns |

## Design

**A task per connection.** A tokio runtime with `-t` worker threads runs one task per connection. A semaphore caps connections at `-c`, and `TCP_NODELAY` is on.

**Parsing.** Each connection reads into one reusable buffer and splits lines with `memchr`. Storage commands only parse their header, and the data block is read into a buffer of its exact size. Keys that get stored are copied, so a cached item doesn't keep the read buffer alive. A rejected store still reads its data block to keep the connection in sync.

**Batched writes.** Responses are encoded into an output buffer that is only sent before the connection waits for more input, so pipelined commands get their responses back in a few writes. `get` copies values straight from the cache into that buffer, except large ones, which are queued by reference.

**Storage.** Items live in [quick_cache](https://github.com/arthurprs/quick-cache), a sharded cache with S3-FIFO-style eviction. Each item weighs its key and value plus 152 bytes of overhead. The cache has 4 shards per CPU, or fewer if needed for a shard to hold a 1 MiB item. `get` takes a shard read lock; commands that read and then write (`add`, `cas`, `incr`, `append`, ...) take the write lock, so they're atomic per key.

**Expiry.** A background task updates a shared clock once per second, and items are checked for expiry when they're accessed. As in memcached, an `exptime` over 30 days is a Unix timestamp. `flush_all <delay>` sets a cutoff, and items stored before it disappear once it passes.

**Stats.** Counters are sharded per thread with [contatori](https://crates.io/crates/contatori) and only added up when a client runs `stats`.

## Limitations

- Only the text protocol: no binary protocol, no meta commands (`mg`, `ms`, ...), no UDP, SASL or TLS.
- No slab allocator, so `-m` is an estimate: at `-m 64` full of 128-byte values, the process uses about 82 MiB.
- Expiry is lazy: there is no LRU crawler, so expired items keep using memory until they're accessed or evicted.
- `stats` has no subcommands (`stats items`, `stats slabs`, ...) and reports a subset of memcached's fields.
- `verbosity` is accepted but does nothing; logging is set at startup with `-v` or `RUST_LOG`.
- The value size limit (1 MiB, slightly less at `-m 1`) isn't configurable like memcached's `-I`.
- Command lines are limited to 8 KiB, except `get`/`gets` lines, which can be up to 4 MiB.
- When the connection limit is reached, new connections wait for a free slot instead of being refused with an error.
- There's no idle timeout for connections.

## Development

```console
$ cargo test
$ cargo bench --bench parser --bench store --bench connection
$ scripts/bench.sh -n 4                 # memtier vs ousagi in Docker
$ scripts/bench.sh -t memcached -n 4    # same load against memcached
```

`scripts/profiling.sh` runs memtier against a server that is already running locally, which is handy when profiling a native build.
