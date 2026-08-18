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
}

impl Item {
    pub(crate) fn new(data: Bytes, flags: u32, exptime: i64) -> Self {
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
        }
    }

    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    pub(crate) fn flags(&self) -> u32 {
        self.flags
    }

    pub(crate) fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}

/// Shared handle to the whole cache.
pub type Store = Arc<RwLock<HashMap<String, Item>>>;
