use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use dashmap::DashMap;

use crate::clock::SharedClock;

const THIRTY_DAYS_SECS: i64 = 60 * 60 * 24 * 30;

/// A value stored in the cache.
pub struct Item {
    data: Bytes,
    flags: u32,
    expires_at: Option<u64>,
    cas: u64,
    stored_at: u64,
}

impl Item {
    pub(crate) fn new(data: Bytes, flags: u32, exptime: i64, cas: u64, now: u64) -> Self {
        let expires_at = match exptime {
            0 => None,
            n if n < 0 => Some(0),
            n if n <= THIRTY_DAYS_SECS => Some(now + n as u64),
            n => Some(n as u64),
        };

        Self {
            data,
            flags,
            expires_at,
            cas,
            stored_at: now,
        }
    }

    pub(crate) fn with_parts(
        data: Bytes,
        flags: u32,
        expires_at: Option<u64>,
        cas: u64,
        stored_at: u64,
    ) -> Self {
        Self {
            data,
            flags,
            expires_at,
            cas,
            stored_at,
        }
    }

    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    pub(crate) fn flags(&self) -> u32 {
        self.flags
    }

    pub(crate) fn cas(&self) -> u64 {
        self.cas
    }

    pub(crate) fn expires_at(&self) -> Option<u64> {
        self.expires_at
    }

    pub(crate) fn stored_at(&self) -> u64 {
        self.stored_at
    }

    pub(crate) fn is_expired(&self, now: u64, oldest_live: Option<u64>) -> bool {
        if self.expires_at.is_some_and(|t| now >= t) {
            return true;
        }

        if let Some(cutoff) = oldest_live {
            if self.stored_at() < cutoff {
                return true;
            }
        }

        false
    }
}

pub struct StoreInner {
    pub items: DashMap<Bytes, Item>,
    next_cas: AtomicU64,
    oldest_live: AtomicU64,
    shared_clock: SharedClock,
}

impl StoreInner {
    pub fn new(shared_clock: SharedClock) -> Self {
        Self {
            items: DashMap::new(),
            next_cas: AtomicU64::new(1),
            oldest_live: AtomicU64::new(0),
            shared_clock,
        }
    }

    pub fn next_cas(&self) -> u64 {
        self.next_cas.fetch_add(1, Ordering::Relaxed)
    }

    pub fn now(&self) -> u64 {
        self.shared_clock.now()
    }

    pub fn oldest_live(&self) -> Option<u64> {
        match self.oldest_live.load(Ordering::Relaxed) {
            0 => None,
            secs => Some(secs),
        }
    }

    pub fn flush_all(&self, delay_secs: u32) {
        let new_oldest = self.shared_clock.now() + delay_secs as u64;
        self.oldest_live.store(new_oldest, Ordering::Relaxed);
    }
}

/// Shared handle to the whole cache.
pub type Store = Arc<StoreInner>;
