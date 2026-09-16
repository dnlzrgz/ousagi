use contatori::counters::{Observable, unsigned::Unsigned};

pub static CURR_CONNECTIONS: Unsigned = Unsigned::new();
pub static TOTAL_CONNECTIONS: Unsigned = Unsigned::new();
pub static CMD_GET: Unsigned = Unsigned::new();
pub static CMD_SET: Unsigned = Unsigned::new();
pub static CMD_FLUSH: Unsigned = Unsigned::new();
pub static GET_HITS: Unsigned = Unsigned::new();
pub static GET_MISSES: Unsigned = Unsigned::new();
pub static GET_EXPIRED: Unsigned = Unsigned::new();
pub static DELETE_HITS: Unsigned = Unsigned::new();
pub static DELETE_MISSES: Unsigned = Unsigned::new();
pub static CMD_INCR: Unsigned = Unsigned::new();
pub static CMD_DECR: Unsigned = Unsigned::new();
pub static INCR_HITS: Unsigned = Unsigned::new();
pub static INCR_MISSES: Unsigned = Unsigned::new();
pub static DECR_HITS: Unsigned = Unsigned::new();
pub static DECR_MISSES: Unsigned = Unsigned::new();
pub static CAS_HITS: Unsigned = Unsigned::new();
pub static CAS_MISSES: Unsigned = Unsigned::new();
pub static CAS_BADVAL: Unsigned = Unsigned::new();
pub static TOTAL_ITEMS: Unsigned = Unsigned::new();
pub static EVICTIONS: Unsigned = Unsigned::new();
pub static BYTES_READ: Unsigned = Unsigned::new();
pub static BYTES_WRITTEN: Unsigned = Unsigned::new();

pub fn report(curr_items: usize) -> Vec<(&'static str, String)> {
    vec![
        (
            "curr_connections",
            CURR_CONNECTIONS.value().as_u64().to_string(),
        ),
        (
            "total_connections",
            TOTAL_CONNECTIONS.value().as_u64().to_string(),
        ),
        ("cmd_get", CMD_GET.value().as_u64().to_string()),
        ("cmd_set", CMD_SET.value().as_u64().to_string()),
        ("cmd_flush", CMD_FLUSH.value().as_u64().to_string()),
        ("get_hits", GET_HITS.value().as_u64().to_string()),
        ("get_misses", GET_MISSES.value().as_u64().to_string()),
        ("get_expired", GET_EXPIRED.value().as_u64().to_string()),
        ("delete_hits", DELETE_HITS.value().as_u64().to_string()),
        ("delete_misses", DELETE_MISSES.value().as_u64().to_string()),
        ("cmd_incr", CMD_INCR.value().as_u64().to_string()),
        ("cmd_decr", CMD_DECR.value().as_u64().to_string()),
        ("incr_hits", INCR_HITS.value().as_u64().to_string()),
        ("incr_misses", INCR_MISSES.value().as_u64().to_string()),
        ("decr_hits", DECR_HITS.value().as_u64().to_string()),
        ("decr_misses", DECR_MISSES.value().as_u64().to_string()),
        ("cas_hits", CAS_HITS.value().as_u64().to_string()),
        ("cas_misses", CAS_MISSES.value().as_u64().to_string()),
        ("cas_badval", CAS_BADVAL.value().as_u64().to_string()),
        ("curr_items", curr_items.to_string()),
        ("total_items", TOTAL_ITEMS.value().as_u64().to_string()),
        ("evictions", EVICTIONS.value().as_u64().to_string()),
        ("bytes_read", BYTES_READ.value().as_u64().to_string()),
        ("bytes_written", BYTES_WRITTEN.value().as_u64().to_string()),
    ]
}
