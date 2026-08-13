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

impl Item {
    pub(crate) fn new(data: Bytes, flags: u32) -> Self {
        Self { data, flags }
    }

    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    pub(crate) fn flags(&self) -> u32 {
        self.flags
    }
}

/// Shared handle to the whole cache.
pub type Store = Arc<Mutex<HashMap<String, Item>>>;
