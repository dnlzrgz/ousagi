use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use dashmap::DashMap;

use crate::{clock::SharedClock, stats::Stats};

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
    pub(crate) fn resolve_expiry(exptime: i64, now: u64) -> Option<u64> {
        match exptime {
            0 => None,
            n if n < 0 => Some(0),
            n if n <= THIRTY_DAYS_SECS => Some(now + n as u64),
            n => Some(n as u64),
        }
    }

    pub(crate) fn new(data: Bytes, flags: u32, exptime: i64, cas: u64, now: u64) -> Self {
        Self {
            data,
            flags,
            expires_at: Self::resolve_expiry(exptime, now),
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
    pub stats: Stats,
    next_cas: AtomicU64,
    oldest_live: AtomicU64,
    shared_clock: SharedClock,
}

impl StoreInner {
    pub fn new(shared_clock: SharedClock, threads: usize) -> Self {
        let shard_amount = (threads.max(1) * 4).next_power_of_two();

        Self {
            items: DashMap::with_hasher_and_shard_amount(
                std::hash::RandomState::default(),
                shard_amount,
            ),
            stats: Stats::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;

    fn item_at(secs_from_now: i64, now: u64) -> Item {
        Item::new(Bytes::from_static(b"data"), 0, secs_from_now, 1, now)
    }

    #[test]
    fn resolve_expiry_zero_means_never_expires() {
        assert_eq!(Item::resolve_expiry(0, 1_000), None);
    }

    #[test]
    fn resolve_expiry_negative_means_already_expired() {
        assert_eq!(Item::resolve_expiry(-1, 1_000), Some(0));
    }

    #[test]
    fn resolve_expiry_relative_time_under_threshold() {
        assert_eq!(Item::resolve_expiry(60, 1_000), Some(1_060));
    }

    #[test]
    fn resolve_expiry_at_thirty_day_boundary_is_relative() {
        assert_eq!(
            Item::resolve_expiry(THIRTY_DAYS_SECS, 1_000),
            Some(1_000 + THIRTY_DAYS_SECS as u64)
        );
    }

    #[test]
    fn resolve_expiry_just_over_thirty_day_boundary_is_absolute() {
        let absolute_ts = THIRTY_DAYS_SECS + 1;
        assert_eq!(
            Item::resolve_expiry(absolute_ts, 1_000),
            Some(absolute_ts as u64)
        );
    }

    #[test]
    fn item_expired_at_expires_at() {
        let item = item_at(60, 1_000); // expires_at = 1_060
        assert!(item.is_expired(1_060, None));
    }

    #[test]
    fn item_not_expired_before_expires_at() {
        let item = item_at(60, 1_000);
        assert!(!item.is_expired(1_059, None));
    }

    #[test]
    fn item_with_no_expiry_never_expires_by_time() {
        let item = item_at(0, 1_000);
        assert!(!item.is_expired(u64::MAX, None));
    }

    #[test]
    fn item_expired_by_oldest_live_cutoff() {
        let item = item_at(0, 1_000);
        assert!(item.is_expired(1_001, Some(1_001)));
    }

    #[test]
    fn item_stored_exactly_at_oldest_live_cutoff_not_expired() {
        let item = item_at(0, 1_000);
        assert!(!item.is_expired(1_000, Some(1_000)));
    }

    #[test]
    fn next_cas_starts_at_one_and_increments() {
        let clock = Clock::mock(1_000);
        let store = StoreInner::new(clock, 4);

        assert_eq!(store.next_cas(), 1);
        assert_eq!(store.next_cas(), 2);
    }

    #[test]
    fn oldest_live_defaults_to_none() {
        let clock = Clock::mock(1_000);
        let store = StoreInner::new(clock, 4);

        assert_eq!(store.oldest_live(), None);
    }

    #[test]
    fn flush_all_sets_oldest_live() {
        let clock = Clock::mock(1_000);
        let store = StoreInner::new(clock, 4);

        store.flush_all(30);

        assert_eq!(store.oldest_live(), Some(1_030));
    }
}
