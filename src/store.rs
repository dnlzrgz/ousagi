use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

/// A value stored in the cache.
pub struct Item {
    data: Bytes,
    flags: u32,
}

/// Shared handle to the whole cache.
pub type Store = Arc<Mutex<HashMap<String, Item>>>;
