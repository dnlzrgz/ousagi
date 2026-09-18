# Ousagi (大兎 Ōusagi)

Ousagi is a [memcached's text protocol](https://docs.memcached.org/protocols/basic/) clone written in Rust that uses the [tokio](https://tokio.rs) runtime and the [quick_cache](https://github.com/arthurprs/quick-cache) crate for eviction. Right now, under the same conditions, performance is about 15% slower than memcached. I'm still tracking down why and how to close the gap.

All my career I've worked with caching services on some level or another, with just a conceptual understanding of how they worked. Now, with a bit more free time, I decided to look closer and settled on memcached as the object of study.

At first, this was going to be a learn-only project: I wanted to get to rough parity before moving on, but somewhere along the way I found myself wanting to dig deeper in the lowe-level code and figure out how to push its performance as far as possible.
