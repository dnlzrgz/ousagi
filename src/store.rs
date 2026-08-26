use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use bytes::Bytes;

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

#[derive(Default)]
pub struct StoreInner {
    pub items: HashMap<Bytes, Item>,
    pub next_cas: u64,
    pub oldest_live: Option<SystemTime>,
}

impl StoreInner {
    pub fn new() -> Self {
        Self {
            next_cas: 1,
            ..Default::default()
        }
    }

    pub fn flush_all(&mut self, delay_secs: u32) {
        let now = SystemTime::now();
        self.oldest_live = if delay_secs == 0 {
            Some(now)
        } else {
            now.checked_add(Duration::from_secs(delay_secs as u64))
        }
    }
}

/// Shared handle to the whole cache.
pub type Store = Arc<RwLock<StoreInner>>;
