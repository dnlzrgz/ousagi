use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::{Bytes, BytesMut};
use contatori::counters::unsigned::Unsigned;
use quick_cache::{
    DefaultHashBuilder, Lifecycle, Weighter,
    sync::{Cache, EntryAction, EntryResult},
};

use crate::{
    clock::{Clock, SharedClock},
    commands::{ArithmeticOp, Response, StoreArgs, StoreOp},
    parser::MAX_ITEM_SIZE,
    stats,
};

const THIRTY_DAYS_SECS: i64 = 60 * 60 * 24 * 30;
const ITEM_OVERHEAD_BYTES: u64 = 64;
/// quick_cache's default `hot_allocation`, used by `Cache::with`.
const HOT_ALLOCATION: f64 = 0.97;

fn unexpected_entry() -> Response {
    tracing::error!("unexpected cache entry result");
    Response::ServerError("unexpected cache state")
}

#[derive(Clone)]
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

    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    pub(crate) fn flags(&self) -> u32 {
        self.flags
    }

    pub(crate) fn cas(&self) -> u64 {
        self.cas
    }

    #[cfg(test)]
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

        if let Some(cutoff) = oldest_live
            && now >= cutoff
            && self.stored_at() < cutoff
        {
            return true;
        }

        false
    }
}

#[derive(Clone)]
struct ItemWeighter;

impl Weighter<Bytes, Item> for ItemWeighter {
    fn weight(&self, key: &Bytes, item: &Item) -> u64 {
        item_weight(key, item.data())
    }
}

fn item_weight(key: &[u8], data: &[u8]) -> u64 {
    (key.len() + data.len()) as u64 + ITEM_OVERHEAD_BYTES
}

#[derive(Clone)]
struct ItemLifecycle {
    evictions: &'static Unsigned,
}

impl Lifecycle<Bytes, Item> for ItemLifecycle {
    type RequestState = ();

    // Runs when an item is evicted.
    fn before_evict(&self, _state: &mut Self::RequestState, _key: &Bytes, _item: &mut Item) {
        self.evictions.add(1);
    }
}

struct StoreInner {
    items: Cache<Bytes, Item, ItemWeighter, DefaultHashBuilder, ItemLifecycle>,
    /// Heaviest item a shard accepts. quick_cache silently drops heavier inserts
    /// (and the key's old value), so they're refused before reaching it.
    max_item_weight: u64,
    next_cas: AtomicU64,
    oldest_live: AtomicU64,
    shared_clock: SharedClock,
}

impl StoreInner {
    pub fn new(shared_clock: SharedClock, memory_limit_bytes: u64) -> Self {
        Self::with_eviction_counter(shared_clock, memory_limit_bytes, &stats::EVICTIONS)
    }

    fn with_eviction_counter(
        shared_clock: SharedClock,
        memory_limit_bytes: u64,
        evictions: &'static Unsigned,
    ) -> Self {
        let items = Cache::with(
            1000,
            memory_limit_bytes,
            ItemWeighter,
            DefaultHashBuilder::default(),
            ItemLifecycle { evictions },
        );
        let shard_capacity = items.shard_capacity();
        let max_item_weight = ((shard_capacity as f64 * HOT_ALLOCATION) as u64)
            .clamp(shard_capacity.min(1), shard_capacity);

        Self {
            items,
            max_item_weight,
            next_cas: AtomicU64::new(1),
            oldest_live: AtomicU64::new(0),
            shared_clock,
        }
    }

    fn next_cas(&self) -> u64 {
        self.next_cas.fetch_add(1, Ordering::Relaxed)
    }

    fn now(&self) -> u64 {
        self.shared_clock.now()
    }

    fn oldest_live(&self) -> Option<u64> {
        match self.oldest_live.load(Ordering::Relaxed) {
            0 => None,
            secs => Some(secs),
        }
    }

    fn flush_all(&self, delay_secs: u32) {
        let new_oldest = self.shared_clock.now() + delay_secs as u64;
        self.oldest_live.store(new_oldest, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

impl Store {
    pub fn new(shared_clock: SharedClock, memory_limit_bytes: u64) -> Self {
        Self {
            inner: Arc::new(StoreInner::new(shared_clock, memory_limit_bytes)),
        }
    }

    pub fn get(&self, keys: &[Bytes], with_cas: bool) -> Response {
        let now = self.inner.now();
        stats::CMD_GET.add(keys.len());

        let oldest_live = self.inner.oldest_live();
        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            match self.inner.items.get(key) {
                Some(item) if !item.is_expired(now, oldest_live) => {
                    stats::GET_HITS.add(1);
                    let cas = with_cas.then_some(item.cas());
                    values.push((key.clone(), item.flags(), item.data, cas));
                }
                Some(_) => {
                    self.inner
                        .items
                        .remove_if(key, |item| item.is_expired(now, oldest_live));
                    stats::GET_EXPIRED.add(1);
                    stats::GET_MISSES.add(1);
                }
                None => {
                    stats::GET_MISSES.add(1);
                }
            }
        }

        Response::Values(values)
    }

    pub fn get_and_touch(&self, keys: &[Bytes], exptime: i64, with_cas: bool) -> Response {
        let now = self.inner.now();
        stats::CMD_TOUCH.add(keys.len());

        let oldest_live = self.inner.oldest_live();
        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            let result = self.inner.items.entry(key, None, |_key, item| {
                if item.is_expired(now, oldest_live) {
                    return EntryAction::Remove;
                }

                item.expires_at = Item::resolve_expiry(exptime, now);

                EntryAction::Retain((item.flags(), item.data().clone(), item.cas))
            });

            match result {
                EntryResult::Retained((flags, data, cas)) => {
                    stats::TOUCH_HITS.add(1);
                    values.push((key.clone(), flags, data, with_cas.then_some(cas)));
                }
                EntryResult::Removed(..) => {
                    stats::GET_EXPIRED.add(1);
                    stats::TOUCH_MISSES.add(1);
                }
                EntryResult::Vacant(_) => {
                    stats::TOUCH_MISSES.add(1);
                }
                // Counted as a miss so one key can't throw the entire response.
                EntryResult::Replaced(..) | EntryResult::Timeout => {
                    tracing::error!("unexpected cache entry result");
                    stats::TOUCH_MISSES.add(1);
                }
            }
        }

        Response::Values(values)
    }

    pub fn store(&self, op: StoreOp, args: StoreArgs) -> Response {
        stats::CMD_SET.add(1);

        if item_weight(&args.key, &args.data) > self.inner.max_item_weight {
            tracing::debug!(key = ?args.key, len = args.data.len(), "item heavier than a shard");
            match op {
                // Like memcached a failed `set` doesn't leave old value behind
                StoreOp::Set => {
                    self.inner.items.remove(&args.key);
                    return Response::ServerError("out of memory storing object");
                }
                StoreOp::Add | StoreOp::Replace | StoreOp::Cas(_) => {
                    return Response::ServerError("out of memory storing object");
                }
                // Combined value is checked by `store_append_or_prepend`
                StoreOp::Append | StoreOp::Prepend => {}
            }
        }

        match op {
            StoreOp::Set => self.store_set(args),
            StoreOp::Add => self.store_add(args),
            StoreOp::Replace => self.store_replace(args),
            StoreOp::Append | StoreOp::Prepend => self.store_append_or_prepend(op, args),
            StoreOp::Cas(expected_cas) => self.store_cas(expected_cas, args),
        }
    }

    fn store_set(&self, args: StoreArgs) -> Response {
        let now = self.inner.now();
        let cas = self.inner.next_cas();

        let item = Item::new(args.data, args.flags, args.exptime, cas, now);
        self.inner.items.insert(args.key, item);
        stats::TOTAL_ITEMS.add(1);
        Response::Stored
    }

    fn store_add(&self, args: StoreArgs) -> Response {
        let now = self.inner.now();
        let oldest_live = self.inner.oldest_live();
        let cas = self.inner.next_cas();

        let result = self.inner.items.entry(&args.key, None, |_k, item| {
            if item.is_expired(now, oldest_live) {
                *item = Item::new(args.data.clone(), args.flags, args.exptime, cas, now);
                EntryAction::Retain(true)
            } else {
                EntryAction::Retain(false)
            }
        });

        match result {
            EntryResult::Retained(true) => {
                stats::TOTAL_ITEMS.add(1);
                Response::Stored
            }
            EntryResult::Retained(false) => Response::NotStored,
            EntryResult::Vacant(guard) => {
                let item = Item::new(args.data, args.flags, args.exptime, cas, now);
                if guard.insert(item).is_ok() {
                    stats::TOTAL_ITEMS.add(1);
                    Response::Stored
                } else {
                    Response::NotStored
                }
            }
            // Callback never returns `Remove`.
            EntryResult::Removed(..) | EntryResult::Replaced(..) | EntryResult::Timeout => {
                unexpected_entry()
            }
        }
    }

    fn store_replace(&self, args: StoreArgs) -> Response {
        let now = self.inner.now();
        let oldest_live = self.inner.oldest_live();
        let cas = self.inner.next_cas();

        let result = self.inner.items.entry(&args.key, None, |_k, item| {
            if item.is_expired(now, oldest_live) {
                EntryAction::Remove
            } else {
                *item = Item::new(args.data.clone(), args.flags, args.exptime, cas, now);
                EntryAction::Retain(())
            }
        });

        match result {
            EntryResult::Retained(()) => {
                stats::TOTAL_ITEMS.add(1);
                Response::Stored
            }
            EntryResult::Removed(..) => {
                stats::GET_EXPIRED.add(1);
                Response::NotStored
            }
            EntryResult::Vacant(_) => Response::NotStored,
            EntryResult::Replaced(..) | EntryResult::Timeout => unexpected_entry(),
        }
    }

    fn store_append_or_prepend(&self, op: StoreOp, args: StoreArgs) -> Response {
        let now = self.inner.now();
        let oldest_live = self.inner.oldest_live();
        let cas = self.inner.next_cas();
        let is_append = op == StoreOp::Append;

        let max_item_weight = self.inner.max_item_weight;

        let result = self.inner.items.entry(&args.key, None, |key, item| {
            if item.is_expired(now, oldest_live) {
                return EntryAction::Remove;
            }

            let new_len = item.data.len() + args.data.len();
            let new_weight = item_weight(key, &[]) + new_len as u64;
            if new_len > MAX_ITEM_SIZE || new_weight > max_item_weight {
                return EntryAction::Retain(false);
            }

            let mut new_data = BytesMut::with_capacity(new_len);
            let (first, second) = if is_append {
                (item.data.as_ref(), args.data.as_ref())
            } else {
                (args.data.as_ref(), item.data.as_ref())
            };

            new_data.extend_from_slice(first);
            new_data.extend_from_slice(second);

            item.data = new_data.freeze();
            item.cas = cas;
            EntryAction::Retain(true)
        });

        match result {
            EntryResult::Retained(true) => {
                stats::TOTAL_ITEMS.add(1);
                Response::Stored
            }
            EntryResult::Retained(false) => Response::NotStored,
            EntryResult::Removed(..) => {
                stats::GET_EXPIRED.add(1);
                Response::NotStored
            }
            EntryResult::Vacant(_) => Response::NotStored,
            EntryResult::Replaced(..) | EntryResult::Timeout => unexpected_entry(),
        }
    }

    fn store_cas(&self, expected_cas: u64, args: StoreArgs) -> Response {
        let now = self.inner.now();
        let oldest_live = self.inner.oldest_live();
        let cas = self.inner.next_cas();

        let result = self.inner.items.entry(&args.key, None, |_k, item| {
            if item.is_expired(now, oldest_live) {
                return EntryAction::Remove;
            }

            if item.cas != expected_cas {
                tracing::debug!(expected = expected_cas, actual = item.cas, "cas mismatch");
                return EntryAction::Retain(false);
            }

            *item = Item::new(args.data.clone(), args.flags, args.exptime, cas, now);
            EntryAction::Retain(true)
        });

        match result {
            EntryResult::Retained(true) => {
                stats::CAS_HITS.add(1);
                stats::TOTAL_ITEMS.add(1);
                Response::Stored
            }
            EntryResult::Retained(false) => {
                stats::CAS_BADVAL.add(1);
                Response::Exists
            }
            EntryResult::Removed(..) => {
                stats::GET_EXPIRED.add(1);
                stats::CAS_MISSES.add(1);
                Response::NotFound
            }
            EntryResult::Vacant(_) => {
                stats::CAS_MISSES.add(1);
                Response::NotFound
            }
            EntryResult::Replaced(..) | EntryResult::Timeout => unexpected_entry(),
        }
    }

    pub fn delete(&self, key: &Bytes) -> Response {
        let now = self.inner.now();
        let oldest_live = self.inner.oldest_live();

        match self.inner.items.remove(key) {
            Some((_, item)) if item.is_expired(now, oldest_live) => {
                stats::GET_EXPIRED.add(1);
                stats::DELETE_MISSES.add(1);
                Response::NotFound
            }
            Some(_) => {
                stats::DELETE_HITS.add(1);
                Response::Deleted
            }
            None => {
                stats::DELETE_MISSES.add(1);
                Response::NotFound
            }
        }
    }

    pub fn arithmetic(&self, op: ArithmeticOp, key: &Bytes, delta: u64) -> Response {
        let now = self.inner.now();
        let (cmd, hits, misses) = match op {
            ArithmeticOp::Incr => (&stats::CMD_INCR, &stats::INCR_HITS, &stats::INCR_MISSES),
            ArithmeticOp::Decr => (&stats::CMD_DECR, &stats::DECR_HITS, &stats::DECR_MISSES),
        };
        cmd.add(1);

        let oldest_live = self.inner.oldest_live();
        let cas = self.inner.next_cas();
        let error = match op {
            ArithmeticOp::Incr => "cannot increment non-numeric value",
            ArithmeticOp::Decr => "cannot decrement non-numeric value",
        };

        let result = self.inner.items.entry(key, None, |_k, item| {
            if item.is_expired(now, oldest_live) {
                return EntryAction::Remove;
            }

            let Ok(s) = std::str::from_utf8(item.data.as_ref()) else {
                tracing::debug!(key = ?key, "arithmetic operation failed: non-utf8 content");
                return EntryAction::Retain(Err(error));
            };

            let Ok(val) = s.trim().parse::<u64>() else {
                tracing::debug!(key = ?key, "arithmetic operation failed: non-numeric content");
                return EntryAction::Retain(Err(error));
            };

            let new_val = match op {
                ArithmeticOp::Incr => val.wrapping_add(delta),
                ArithmeticOp::Decr => val.saturating_sub(delta),
            };

            item.data = Bytes::copy_from_slice(itoa::Buffer::new().format(new_val).as_bytes());
            item.cas = cas;

            EntryAction::Retain(Ok(new_val))
        });

        match result {
            EntryResult::Retained(Ok(new_val)) => {
                hits.add(1);
                Response::Number(new_val)
            }
            EntryResult::Retained(Err(msg)) => Response::ClientError(msg),
            EntryResult::Removed(..) => {
                stats::GET_EXPIRED.add(1);
                misses.add(1);
                Response::NotFound
            }
            EntryResult::Vacant(_) => {
                misses.add(1);
                Response::NotFound
            }
            EntryResult::Replaced(..) | EntryResult::Timeout => unexpected_entry(),
        }
    }

    pub fn touch(&self, key: &Bytes, exptime: i64) -> Response {
        let now = self.inner.now();
        let oldest_live = self.inner.oldest_live();

        stats::CMD_TOUCH.add(1);

        let result = self.inner.items.entry(key, None, |_k, item| {
            if item.is_expired(now, oldest_live) {
                return EntryAction::Remove;
            }

            item.expires_at = Item::resolve_expiry(exptime, now);

            EntryAction::Retain(())
        });

        match result {
            EntryResult::Retained(()) => {
                stats::TOUCH_HITS.add(1);
                Response::Touched
            }
            EntryResult::Removed(..) => {
                stats::GET_EXPIRED.add(1);
                stats::TOUCH_MISSES.add(1);
                Response::NotFound
            }
            EntryResult::Vacant(_) => {
                stats::TOUCH_MISSES.add(1);
                Response::NotFound
            }
            EntryResult::Replaced(..) | EntryResult::Timeout => unexpected_entry(),
        }
    }

    pub fn flush(&self, delay: Option<u32>) -> Response {
        tracing::info!(delay = ?delay, "flushing store");
        stats::CMD_FLUSH.add(1);

        match delay {
            Some(0) | None => {
                // Immediate flush replaces any delayed one
                self.inner.oldest_live.store(0, Ordering::Relaxed);
                self.inner.items.clear();
            }
            Some(n) => {
                self.inner.flush_all(n);
            }
        }

        Response::Ok
    }

    pub fn item_count(&self) -> usize {
        self.inner.items.len()
    }

    pub fn clock(&self) -> &Clock {
        &self.inner.shared_clock
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;
    use contatori::counters::{Observable, unsigned::Unsigned};

    const TEST_NOW: u64 = 1_000_000;
    const TEST_MEMORY_LIMIT: u64 = 1024 * 1024;

    fn item_at(secs_from_now: i64, now: u64) -> Item {
        Item::new(Bytes::from_static(b"data"), 0, secs_from_now, 1, now)
    }

    /// Empty Store with a mock clock
    fn empty_store() -> Store {
        Store::new(Clock::mock(TEST_NOW), TEST_MEMORY_LIMIT)
    }

    /// Pre-populated Store with one item
    fn store_with(key: &str, item: Item) -> Store {
        let store = empty_store();
        store
            .inner
            .items
            .insert(Bytes::copy_from_slice(key.as_bytes()), item);
        store
    }

    fn store_with_clock(key: &str, item: Item) -> (Store, SharedClock) {
        let clock = Clock::mock(TEST_NOW);
        let store = Store::new(clock.clone(), TEST_MEMORY_LIMIT);
        store
            .inner
            .items
            .insert(Bytes::copy_from_slice(key.as_bytes()), item);
        (store, clock)
    }

    fn mock_item(data: &[u8], flags: u32, exptime: i64, cas: u64) -> Item {
        Item::new(Bytes::copy_from_slice(data), flags, exptime, cas, TEST_NOW)
    }

    fn store_args(key: &str, flags: u32, exptime: i64, data: &[u8]) -> StoreArgs {
        StoreArgs {
            key: Bytes::copy_from_slice(key.as_bytes()),
            flags,
            exptime,
            data: Bytes::copy_from_slice(data),
            noreply: false,
        }
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
        let store = Store::new(clock, TEST_MEMORY_LIMIT);

        assert_eq!(store.inner.next_cas(), 1);
        assert_eq!(store.inner.next_cas(), 2);
    }

    #[test]
    fn oldest_live_defaults_to_none() {
        let clock = Clock::mock(1_000);
        let store = Store::new(clock, TEST_MEMORY_LIMIT);

        assert_eq!(store.inner.oldest_live(), None);
    }

    #[test]
    fn get_existing_key_returns_value_with_correct_flag_and_data() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.get(&[Bytes::from_static(b"foo")], false);

        match resp {
            Response::Values(values) => {
                assert_eq!(values.len(), 1);
                let (key, flags, data, cas) = &values[0];
                assert_eq!(key, &Bytes::from_static(b"foo"));
                assert_eq!(*flags, 42);
                assert_eq!(data, &Bytes::from_static(b"hello"));
                assert_eq!(*cas, None);
            }
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn get_missing_key_returns_empty_values() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.get(&[Bytes::from_static(b"bar")], false);

        match resp {
            Response::Values(values) => assert!(values.is_empty()),
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn get_multiple_existing_keys_returns_all_values() {
        let store = empty_store();
        store.store(
            StoreOp::Set,
            StoreArgs {
                key: "foo".into(),
                flags: 0,
                exptime: 0,
                data: Bytes::from_static(b"a"),
                noreply: false,
            },
        );
        store.store(
            StoreOp::Set,
            StoreArgs {
                key: "bar".into(),
                flags: 0,
                exptime: 0,
                data: Bytes::from_static(b"b"),
                noreply: false,
            },
        );
        let resp = store.get(
            &[Bytes::from_static(b"foo"), Bytes::from_static(b"bar")],
            false,
        );

        match resp {
            Response::Values(values) => assert_eq!(values.len(), 2),
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn get_multiple_keys_skips_missing_one() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.get(
            &[Bytes::from_static(b"foo"), Bytes::from_static(b"bar")],
            false,
        );

        match resp {
            Response::Values(values) => assert_eq!(values.len(), 1),
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn get_expired_key_is_treated_as_missing_and_lazily_removed() {
        let store = store_with("foo", mock_item(b"hello", 42, -1, 1));
        let resp = store.get(&[Bytes::from_static(b"foo")], false);

        match resp {
            Response::Values(values) => assert!(values.is_empty()),
            other => panic!("expected Values, got {other:?}"),
        }

        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn gat_hit_updates_expiry_and_returns_value() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 999));

        let resp = store.get_and_touch(&[Bytes::from_static(b"foo")], 100, false);

        match resp {
            Response::Values(values) => {
                assert_eq!(values.len(), 1);
                let (key, flags, data, cas) = &values[0];
                assert_eq!(key, &Bytes::from_static(b"foo"));
                assert_eq!(*flags, 42);
                assert_eq!(data, &Bytes::from_static(b"hello"));
                assert_eq!(*cas, None); // with_cas was false
            }
            other => panic!("expected Values, got {other:?}"),
        }

        let item = store
            .inner
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.expires_at(), Some(TEST_NOW + 100));
    }

    #[test]
    fn gat_hit_with_cas_keeps_cas_token() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 999));

        let resp = store.get_and_touch(&[Bytes::from_static(b"foo")], 100, true);

        match resp {
            Response::Values(values) => {
                let (_, _, _, cas) = &values[0];
                assert_eq!(*cas, Some(999));
            }
            other => panic!("expected Values, got {other:?}"),
        }

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.cas(), 999);
    }

    #[test]
    fn add_new_key_stores_it_and_returns_stored() {
        let store = empty_store();
        let resp = store.store(StoreOp::Add, store_args("foo", 42, 0, b"hello"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn add_existing_key_fails_and_returns_not_stored() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.store(StoreOp::Add, store_args("foo", 50, 0, b"jello"));

        assert!(matches!(resp, Response::NotStored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn add_existing_expired_key_overwrites_it_and_returns_stored() {
        let store = store_with("foo", mock_item(b"hello", 42, -1, 1));
        let resp = store.store(StoreOp::Add, store_args("foo", 50, 0, b"jello"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 50);
    }

    #[test]
    fn set_new_key_stores_value() {
        let store = empty_store();
        let resp = store.store(StoreOp::Set, store_args("foo", 42, 0, b"hello"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn set_overwrites_existing_key() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.store(StoreOp::Set, store_args("foo", 50, 0, b"jello"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 50);
    }

    #[test]
    fn replace_existing_key_stores_new_value() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.store(StoreOp::Replace, store_args("foo", 50, 0, b"jello"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 50);
    }

    #[test]
    fn replace_missing_key_returns_not_stored() {
        let store = empty_store();
        let resp = store.store(StoreOp::Replace, store_args("foo", 42, 0, b"hello"));

        assert!(matches!(resp, Response::NotStored));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn replace_expired_key_returns_not_stored() {
        let store = store_with("foo", mock_item(b"hello", 42, -1, 1));
        let resp = store.store(StoreOp::Replace, store_args("foo", 50, 0, b"jello"));

        assert!(matches!(resp, Response::NotStored));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn append_existing_key_appends_data_keeping_flags_and_expiry() {
        let original = mock_item(b"hello", 42, 100, 1);
        let original_expires_at = original.expires_at();
        let store = store_with("foo", original);
        let resp = store.store(StoreOp::Append, store_args("foo", 0, 0, b" world"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello world");
        assert_eq!(item.flags(), 42); // append's flags is ignored
        assert_eq!(item.expires_at(), original_expires_at);
    }

    #[test]
    fn prepend_existing_key_prepends_data() {
        let store = store_with("foo", mock_item(b"world", 42, 0, 1));
        let resp = store.store(StoreOp::Prepend, store_args("foo", 0, 0, b"hello "));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello world");
    }

    #[test]
    fn append_or_prepend_past_max_item_size_returns_not_stored() {
        let store = Store::new(Clock::mock(TEST_NOW), 64 * TEST_MEMORY_LIMIT);
        let full = vec![b'x'; MAX_ITEM_SIZE];
        store
            .inner
            .items
            .insert(Bytes::from_static(b"foo"), mock_item(&full, 0, 0, 1));

        for op in [StoreOp::Append, StoreOp::Prepend] {
            let resp = store.store(op, store_args("foo", 0, 0, b"y"));
            assert!(matches!(resp, Response::NotStored));
        }

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().len(), MAX_ITEM_SIZE);
        assert_eq!(item.cas(), 1);
    }

    #[test]
    fn append_missing_key_returns_not_stored() {
        let store = empty_store();
        let resp = store.store(StoreOp::Append, store_args("foo", 0, 0, b"hello"));

        assert!(matches!(resp, Response::NotStored));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn append_expired_key_returns_not_stored() {
        let store = store_with("foo", mock_item(b"hello", 42, -1, 1));
        let resp = store.store(StoreOp::Append, store_args("foo", 0, 0, b" world"));

        assert!(matches!(resp, Response::NotStored));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn cas_matching_token_updates_value_and_returns_stored() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 7));
        let resp = store.store(StoreOp::Cas(7), store_args("foo", 50, 0, b"jello"));

        assert!(matches!(resp, Response::Stored));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 50);
        assert_ne!(item.cas(), 7);
    }

    #[test]
    fn cas_mismatched_token_returns_exists_and_leaves_value_unchanged() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 7));
        let resp = store.store(StoreOp::Cas(999), store_args("foo", 42, 0, b"jello"));

        assert!(matches!(resp, Response::Exists));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.cas(), 7);
    }

    #[test]
    fn cas_missing_key_returns_not_found() {
        let store = empty_store();
        let resp = store.store(StoreOp::Cas(1), store_args("foo", 42, 0, b"hello"));

        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn cas_expired_key_returns_not_found() {
        let store = store_with("foo", mock_item(b"hello", 42, -1, 7));
        let resp = store.store(StoreOp::Cas(7), store_args("foo", 42, 0, b"jello"));

        assert!(matches!(resp, Response::NotFound));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn delete_existing_key_returns_deleted_and_removes_it() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.delete(&Bytes::from_static(b"foo"));

        assert!(matches!(resp, Response::Deleted));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn delete_missing_key_returns_not_found() {
        let store = empty_store();
        let resp = store.delete(&Bytes::from_static(b"foo"));

        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn incr_existing_key_increments_value_and_returns_it() {
        let store = store_with("foo", mock_item(b"10", 42, 0, 1));
        let resp = store.arithmetic(ArithmeticOp::Incr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::Number(15)));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"15");
    }

    #[test]
    fn incr_missing_key_returns_not_found() {
        let store = empty_store();
        let resp = store.arithmetic(ArithmeticOp::Incr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn incr_overflow_wraps_around() {
        let max = u64::MAX.to_string();
        let store = store_with("foo", mock_item(max.as_bytes(), 42, 0, 1));
        let resp = store.arithmetic(ArithmeticOp::Incr, &Bytes::from_static(b"foo"), 1);

        assert!(matches!(resp, Response::Number(0)));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"0");
    }

    #[test]
    fn decr_existing_key_decrements_value_and_returns_it() {
        let store = store_with("foo", mock_item(b"10", 42, 0, 1));
        let resp = store.arithmetic(ArithmeticOp::Decr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::Number(5)));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"5");
    }

    #[test]
    fn decr_missing_key_returns_not_found() {
        let store = empty_store();
        let resp = store.arithmetic(ArithmeticOp::Decr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn decr_underflow_saturates_at_zero() {
        let store = store_with("foo", mock_item(b"0", 42, 0, 1));
        let resp = store.arithmetic(ArithmeticOp::Decr, &Bytes::from_static(b"foo"), 1);

        assert!(matches!(resp, Response::Number(0)));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"0");
    }

    #[test]
    fn incr_expired_key_is_evicted_and_returns_not_found() {
        let store = store_with("foo", mock_item(b"10", 42, -1, 1));
        let resp = store.arithmetic(ArithmeticOp::Incr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::NotFound));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn incr_non_numeric_value_returns_client_error() {
        let store = store_with("foo", mock_item(b"not_a_number", 42, 0, 1));
        let resp = store.arithmetic(ArithmeticOp::Incr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::ClientError(_)));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"not_a_number"); // unchanged
    }

    #[test]
    fn decr_expired_key_is_evicted_and_returns_not_found() {
        let store = store_with("foo", mock_item(b"10", 42, -1, 1));
        let resp = store.arithmetic(ArithmeticOp::Decr, &Bytes::from_static(b"foo"), 5);

        assert!(matches!(resp, Response::NotFound));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn touch_existing_key_updates_expiry_and_returns_touched() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.touch(&Bytes::from_static(b"foo"), 100);

        assert!(matches!(resp, Response::Touched));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.expires_at(), Some(TEST_NOW + 100));
    }

    #[test]
    fn touch_missing_key_returns_not_found() {
        let store = empty_store();
        let resp = store.touch(&Bytes::from_static(b"foo"), 100);

        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn touch_keeps_cas_token() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 7));
        store.touch(&Bytes::from_static(b"foo"), 100);

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.cas(), 7);
    }

    #[test]
    fn touch_preserves_data_and_flags() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        store.touch(&Bytes::from_static(b"foo"), 100);

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn touch_expired_key_is_evicted_and_returns_not_found() {
        let store = store_with("foo", mock_item(b"hello", 42, -1, 1));

        let resp = store.touch(&Bytes::from_static(b"foo"), 100);

        assert!(matches!(resp, Response::NotFound));
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn flush_all_immediate_clears_all_items_and_returns_ok() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.flush(None);

        assert!(matches!(resp, Response::Ok));
        assert!(store.inner.items.is_empty());
    }

    #[test]
    fn flush_all_with_delay_does_not_immediately_remove_items() {
        let store = store_with("foo", mock_item(b"hello", 42, 0, 1));
        let resp = store.flush(Some(3600));

        assert!(matches!(resp, Response::Ok));

        let item = store.inner.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
    }

    #[test]
    fn flush_all_sets_oldest_live() {
        let clock = Clock::mock(1_000);
        let store = Store::new(clock, TEST_MEMORY_LIMIT);

        store.flush(Some(30));

        assert_eq!(store.inner.oldest_live(), Some(1_030));
    }

    #[test]
    fn flush_all_immediate_cancels_pending_delayed_flush() {
        let clock = Clock::mock(TEST_NOW);
        let store = Store::new(clock.clone(), TEST_MEMORY_LIMIT);

        store.flush(Some(10));
        store.flush(None);
        store.store(StoreOp::Set, store_args("foo", 0, 0, b"hello"));
        clock.advance(11);

        match store.get(&[Bytes::from_static(b"foo")], false) {
            Response::Values(values) => assert_eq!(values.len(), 1),
            other => panic!("expected Values, got {other:?}"),
        }
        assert_eq!(store.inner.oldest_live(), None);
    }

    #[test]
    fn flush_all_delayed_item_becomes_invisible_after_clock_advances_past_delay() {
        let (store, clock) = store_with_clock("foo", mock_item(b"hello", 42, 0, 1));
        store.flush(Some(10));

        match store.get(&[Bytes::from_static(b"foo")], false) {
            Response::Values(values) => assert_eq!(values.len(), 1), // not expired yet
            other => panic!("expected Values, got {other:?}"),
        }

        clock.advance(11);

        match store.get(&[Bytes::from_static(b"foo")], false) {
            Response::Values(values) => assert!(values.is_empty()),
            other => panic!("expected Values, got {other:?}"),
        }
        assert!(store.inner.items.get("foo".as_bytes()).is_none());
    }

    const MIB: u64 = 1024 * 1024;

    fn big_args(key: &str, data: &[u8]) -> StoreArgs {
        StoreArgs {
            key: Bytes::copy_from_slice(key.as_bytes()),
            flags: 0,
            exptime: 0,
            data: Bytes::copy_from_slice(data),
            noreply: false,
        }
    }

    fn get_data(store: &Store, key: &str) -> Option<Bytes> {
        store
            .inner
            .items
            .get(key.as_bytes())
            .map(|item| item.data().clone())
    }

    /// A value which item weighs one byte more than a shard accepts.
    fn too_heavy(store: &Store, key: &str) -> Vec<u8> {
        let max = store.inner.max_item_weight;
        vec![b'x'; (max + 1 - ITEM_OVERHEAD_BYTES) as usize - key.len()]
    }

    #[test]
    fn one_mib_item_is_stored_at_64_mib() {
        let store = Store::new(Clock::mock(1), 64 * MIB);
        let data = vec![b'x'; MAX_ITEM_SIZE];

        let resp = store.store(StoreOp::Set, big_args("big", &data));

        assert!(matches!(resp, Response::Stored));
        assert_eq!(
            get_data(&store, "big").map(|d| d.len()),
            Some(MAX_ITEM_SIZE)
        );
    }

    #[test]
    fn set_at_max_item_weight_is_stored() {
        let store = Store::new(Clock::mock(1), MIB);
        let max = store.inner.max_item_weight;
        let data = vec![b'x'; (max - ITEM_OVERHEAD_BYTES) as usize - 3];

        let resp = store.store(StoreOp::Set, big_args("big", &data));

        assert!(matches!(resp, Response::Stored));
        assert_eq!(get_data(&store, "big").map(|d| d.len()), Some(data.len()));
    }

    #[test]
    fn set_heavier_than_a_shard_fails_and_drops_old_value() {
        let store = Store::new(Clock::mock(1), MIB);
        store.store(StoreOp::Set, big_args("big", b"old"));
        let data = too_heavy(&store, "big");

        let resp = store.store(StoreOp::Set, big_args("big", &data));

        assert!(matches!(
            resp,
            Response::ServerError("out of memory storing object")
        ));
        assert_eq!(get_data(&store, "big"), None);
    }

    #[test]
    fn add_replace_cas_heavier_than_a_shard_fail_and_keep_old_value() {
        let store = Store::new(Clock::mock(1), MIB);
        store.store(StoreOp::Set, big_args("big", b"old"));
        let cas = store.inner.items.get(b"big".as_slice()).unwrap().cas();
        let data = too_heavy(&store, "big");

        for op in [StoreOp::Add, StoreOp::Replace, StoreOp::Cas(cas)] {
            let resp = store.store(op, big_args("big", &data));
            assert!(
                matches!(resp, Response::ServerError("out of memory storing object")),
                "{op:?} answered {resp:?}"
            );
        }
        assert_eq!(get_data(&store, "big").as_deref(), Some(b"old".as_slice()));

        let resp = store.store(StoreOp::Add, big_args("new", &too_heavy(&store, "new")));
        assert!(matches!(resp, Response::ServerError(_)));
        assert_eq!(get_data(&store, "new"), None);
    }

    #[test]
    fn append_or_prepend_heavier_than_a_shard_returns_not_stored() {
        let store = Store::new(Clock::mock(1), MIB);
        store.store(StoreOp::Set, big_args("big", b"old"));
        let data = too_heavy(&store, "big");
        let data = &data[3..]; // old + data is one byte too heavy

        for op in [StoreOp::Append, StoreOp::Prepend] {
            let resp = store.store(op, big_args("big", data));
            assert!(
                matches!(resp, Response::NotStored),
                "{op:?} answered {resp:?}"
            );
        }
        assert_eq!(get_data(&store, "big").as_deref(), Some(b"old".as_slice()));
    }

    #[test]
    fn quick_cache_drops_items_just_over_max_item_weight() {
        let store = Store::new(Clock::mock(1), MIB);
        let data = too_heavy(&store, "big");

        store.inner.items.insert(
            Bytes::from_static(b"big"),
            Item::new(Bytes::from(data), 0, 0, 1, 1),
        );

        assert_eq!(get_data(&store, "big"), None);
    }

    fn store_counting_evictions(counter: &'static Unsigned) -> Store {
        Store {
            inner: Arc::new(StoreInner::with_eviction_counter(
                Clock::mock(TEST_NOW),
                TEST_MEMORY_LIMIT,
                counter,
            )),
        }
    }

    #[test]
    fn overwriting_a_key_is_not_an_eviction() {
        static EVICTIONS: Unsigned = Unsigned::new();
        let store = store_counting_evictions(&EVICTIONS);

        for data in [b"a", b"b", b"c"] {
            store.store(StoreOp::Set, store_args("foo", 0, 0, data));
        }

        assert_eq!(EVICTIONS.value().as_u64(), 0);
    }

    #[test]
    fn evicting_to_make_room_counts_evictions() {
        static EVICTIONS: Unsigned = Unsigned::new();
        let store = store_counting_evictions(&EVICTIONS);
        let data = vec![b'x'; 1024];

        // 2 MiB into a 1 MiB store
        for i in 0..2048 {
            store.store(StoreOp::Set, store_args(&format!("key{i}"), 0, 0, &data));
        }

        let evictions = EVICTIONS.value().as_u64();
        assert!(evictions > 0);
        assert_eq!(evictions, 2048 - store.item_count() as u64);
    }
}
