use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default, Debug)]
pub struct Stats {
    pub cmd_get: AtomicU64,
    pub cmd_set: AtomicU64,
    pub cmd_flush: AtomicU64,

    pub get_hits: AtomicU64,
    pub get_misses: AtomicU64,
    pub get_expired: AtomicU64,

    pub cas_hits: AtomicU64,
    pub cas_misses: AtomicU64,
    pub cas_badval: AtomicU64,

    pub delete_hits: AtomicU64,
    pub delete_misses: AtomicU64,

    pub incr_hits: AtomicU64,
    pub incr_misses: AtomicU64,
    pub decr_hits: AtomicU64,
    pub decr_misses: AtomicU64,

    pub curr_items: AtomicU64,
    pub total_items: AtomicU64,
    pub evictions: AtomicU64,

    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,

    pub curr_connections: AtomicU64,
    pub total_connections: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn incr(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Relaxed);
    }

    pub fn report(&self, curr_items: usize) -> Vec<(&'static str, String)> {
        vec![
            (
                "curr_connections",
                self.curr_connections.load(Relaxed).to_string(),
            ),
            (
                "total_connections",
                self.total_connections.load(Relaxed).to_string(),
            ),
            ("cmd_get", self.cmd_get.load(Relaxed).to_string()),
            ("cmd_set", self.cmd_set.load(Relaxed).to_string()),
            ("cmd_flush", self.cmd_flush.load(Relaxed).to_string()),
            ("get_hits", self.get_hits.load(Relaxed).to_string()),
            ("get_misses", self.get_misses.load(Relaxed).to_string()),
            ("get_expired", self.get_expired.load(Relaxed).to_string()),
            ("delete_hits", self.delete_hits.load(Relaxed).to_string()),
            (
                "delete_misses",
                self.delete_misses.load(Relaxed).to_string(),
            ),
            ("incr_hits", self.incr_hits.load(Relaxed).to_string()),
            ("incr_misses", self.incr_misses.load(Relaxed).to_string()),
            ("decr_hits", self.decr_hits.load(Relaxed).to_string()),
            ("decr_misses", self.decr_misses.load(Relaxed).to_string()),
            ("cas_hits", self.cas_hits.load(Relaxed).to_string()),
            ("cas_misses", self.cas_misses.load(Relaxed).to_string()),
            ("cas_badval", self.cas_badval.load(Relaxed).to_string()),
            ("curr_items", curr_items.to_string()),
            ("total_items", self.total_items.load(Relaxed).to_string()),
            ("evictions", self.evictions.load(Relaxed).to_string()),
            ("bytes_read", self.bytes_read.load(Relaxed).to_string()),
            (
                "bytes_written",
                self.bytes_written.load(Relaxed).to_string(),
            ),
        ]
    }
}
