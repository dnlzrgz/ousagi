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
| `-v, --verbose` | `warn` | `-v` info, `-vv` debug, `-vvv` trace. `RUST_LOG` overrides it |

There is also a Docker image:

```console
$ docker build -f docker/Dockerfile -t ousagi .
$ docker run --rm -p 11211:11211 ousagi
```

## Performance

memtier_benchmark 2.6.1 against ousagi and memcached 1.6.45, both with 4 threads and in containers limited to 4 CPUs and 1 GiB of memory. Load: 4 memtier threads with 50 clients each, 128-byte values, 100k keys, 1 `set` per 10 `get`s, 30 seconds after warming up the keyspace. Host: 13th Gen Intel Core i7-13650HX (20 threads), Linux. Three runs per server:

| Server | Ops/sec (median) | Range | Avg latency | p99 latency |
| --- | --- | --- | --- | --- |
| memcached 1.6.45 | 692k | 691k - 698k | 0.289 ms | 0.727 ms |
| ousagi | 528k | 506k - 618k | 0.378 ms | 0.791 ms |

So right now ousagi does about 76% of memcached's throughput at the median, and its results vary a lot more from run to run. To reproduce it, run `scripts/bench.sh` and `scripts/bench.sh -t memcached`.

Some criterion micro-benchmarks (single thread, no network, medians):

| Benchmark | What it measures | Time |
| --- | --- | --- |
| `parse_get/single_key` | parse `get foo` | 26 ns |
| `parse_store/set` | parse `set foo 0 0 512` | 26 ns |
| `store_get/hit` | `get` of an existing key | 57 ns |
| `store_get/miss` | `get` of a missing key | 34 ns |
| `store_store/set` | `set` of a new key | 134 ns |
| `store_get_and_touch/hit` | `gat` of an existing key | 110 ns |
| `store_arithmetic/incr` | `incr` of an existing key | 100 ns |
| `read_line/pipelined/1000` | read 1,000 pipelined `get foo` lines | 19.6 µs (~20 ns per line) |
| `write_response/values/10` | encode a 10-value `get` response | 397 ns |

## Design decisions

**A task per connection.** Ousagi runs a multi-threaded tokio runtime with `-t` workers (or a single-threaded one with `-t 1`). Every accepted connection gets its own task, a semaphore caps them at `-c`, and `TCP_NODELAY` is enabled so small responses don't wait on Nagle's algorithm.

**Streaming parser over a reusable buffer.** Each connection reads into one `BytesMut`, finds the end of the line with `memchr`, and tokenizes it in place. Storage commands only parse their header; the data block is read separately into a buffer of the exact size, so the parser never touches value bytes. Lookup keys are zero-copy slices of the line, while keys that end up in the cache are copied, so a cached item doesn't keep a whole read buffer alive. When a storage command is rejected, its data block is still read and discarded so the connection stays in sync.

**Flush when idle, not per response.** Responses are encoded into an output buffer that is sent right before the connection blocks on a read. A client that pipelines 100 commands gets its responses back in a few large writes instead of 100 small ones. `get` hits are encoded straight from the cache into the buffer; values too big to copy are queued by reference instead.

**quick_cache for storage and eviction.** Items live in a sharded, weighted [quick_cache](https://github.com/arthurprs/quick-cache), which evicts with an S3-FIFO-style policy. Each item weighs its key plus its value plus 64 bytes, so `-m` bounds memory approximately rather than exactly. `get` only takes a shard read lock. Commands that read and then write (`add`, `cas`, `incr`, `append`, ...) run under the shard's write lock, so they're atomic per key.

**Lazy expiration and a coarse clock.** A background task updates an atomic clock once per second, and items are checked for expiry when they're accessed, then removed. As in memcached, an `exptime` over 30 days is treated as a Unix timestamp. `flush_all <delay>` stores a cutoff time: every item stored before it becomes invisible once the cutoff passes, and a later `flush_all` replaces it.

**Stats without contention.** Counters are sharded per thread with [contatori](https://crates.io/crates/contatori) and only summed when someone runs `stats`, so counting hits and misses doesn't become a shared hot spot.

## Limitations

- Only the text protocol: no binary protocol, no meta commands (`mg`, `ms`, ...), no UDP, SASL or TLS.
- No slab allocator. Memory accounting is an estimate, and `-m` doesn't include the cache's own metadata.
- Expiry is lazy: there is no LRU crawler, so expired items keep using memory until they're accessed or evicted.
- `stats` has no subcommands (`stats items`, `stats slabs`, ...) and reports a subset of memcached's fields.
- `verbosity` is accepted but does nothing; logging is set at startup with `-v` or `RUST_LOG`.
- The value size limit (1 MiB) isn't configurable like memcached's `-I`.
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
