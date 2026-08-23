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
        }
    }

    pub(crate) fn with_parts(
        data: Bytes,
        flags: u32,
        expires_at: Option<SystemTime>,
        cas: u64,
    ) -> Self {
        Self {
            data,
            flags,
            expires_at,
            cas,
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

    pub(crate) fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|t| now >= t)
    }
}

#[derive(Default)]
pub struct StoreInner {
    pub items: HashMap<Bytes, Item>,
    pub next_cas: u64,
}

impl StoreInner {
    pub fn new() -> Self {
        Self {
            next_cas: 1,
            ..Default::default()
        }
    }
}

/// Shared handle to the whole cache.
pub type Store = Arc<RwLock<StoreInner>>;
