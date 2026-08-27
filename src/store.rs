use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use parking_lot::RwLock;

const THIRTY_DAYS_SECS: i64 = 60 * 60 * 24 * 30;

/// A value stored in the cache.
pub struct Item {
    data: Bytes,
    flags: u32,
    expires_at: Option<SystemTime>,
    cas: u64,
    stored_at: SystemTime,
}

impl Item {
    pub(crate) fn new(data: Bytes, flags: u32, exptime: i64, cas: u64) -> Self {
        let expires_at = match exptime {
            0 => None,
            n if n < 0 => Some(SystemTime::UNIX_EPOCH),
            n if n <= THIRTY_DAYS_SECS => {
                SystemTime::now().checked_add(Duration::from_secs(n as u64))
            }
            n => SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(n as u64)),
        };

        Self {
            data,
            flags,
            expires_at,
            cas,
            stored_at: SystemTime::now(),
        }
    }

    pub(crate) fn with_parts(
        data: Bytes,
        flags: u32,
        expires_at: Option<SystemTime>,
        cas: u64,
        stored_at: SystemTime,
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

    pub(crate) fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    pub(crate) fn stored_at(&self) -> SystemTime {
        self.stored_at
    }

    pub(crate) fn is_expired(&self, now: SystemTime, oldest_live: Option<SystemTime>) -> bool {
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
    pub items: RwLock<HashMap<Bytes, Item>>,
    next_cas: AtomicU64,
    oldest_live: AtomicU64,
}

impl StoreInner {
    pub fn new() -> Self {
        Self {
            items: RwLock::new(HashMap::new()),
            next_cas: AtomicU64::new(1),
            oldest_live: AtomicU64::new(0),
        }
    }

    pub fn next_cas(&self) -> u64 {
        self.next_cas.fetch_add(1, Ordering::Relaxed)
    }

    pub fn oldest_live(&self) -> Option<SystemTime> {
        let secs = self.oldest_live.load(Ordering::Relaxed);
        if secs == 0 {
            None
        } else {
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
        }
    }

    pub fn flush_all(&self, delay_secs: u32) {
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let new_oldest = now_secs + delay_secs as u64;
        self.oldest_live.store(new_oldest, Ordering::Relaxed);
    }
}

/// Shared handle to the whole cache.
pub type Store = Arc<StoreInner>;
