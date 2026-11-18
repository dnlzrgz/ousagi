use contatori::counters::{Observable, unsigned::Unsigned};

use crate::{clock::Clock, handler::VERSION};

pub static CURR_CONNECTIONS: Unsigned = Unsigned::new();
pub static TOTAL_CONNECTIONS: Unsigned = Unsigned::new();

pub static CMD_GET: Unsigned = Unsigned::new();
pub static CMD_SET: Unsigned = Unsigned::new();
pub static CMD_INCR: Unsigned = Unsigned::new();
pub static CMD_DECR: Unsigned = Unsigned::new();
pub static CMD_TOUCH: Unsigned = Unsigned::new();
pub static CMD_FLUSH: Unsigned = Unsigned::new();

pub static GET_HITS: Unsigned = Unsigned::new();
pub static GET_MISSES: Unsigned = Unsigned::new();
pub static GET_EXPIRED: Unsigned = Unsigned::new();

pub static CAS_HITS: Unsigned = Unsigned::new();
pub static CAS_MISSES: Unsigned = Unsigned::new();
pub static CAS_BADVAL: Unsigned = Unsigned::new();

pub static INCR_HITS: Unsigned = Unsigned::new();
pub static INCR_MISSES: Unsigned = Unsigned::new();
pub static DECR_HITS: Unsigned = Unsigned::new();
pub static DECR_MISSES: Unsigned = Unsigned::new();

pub static TOUCH_HITS: Unsigned = Unsigned::new();
pub static TOUCH_MISSES: Unsigned = Unsigned::new();

pub static DELETE_HITS: Unsigned = Unsigned::new();
pub static DELETE_MISSES: Unsigned = Unsigned::new();

pub static TOTAL_ITEMS: Unsigned = Unsigned::new();
pub static EVICTIONS: Unsigned = Unsigned::new();
pub static BYTES_READ: Unsigned = Unsigned::new();
pub static BYTES_WRITTEN: Unsigned = Unsigned::new();

pub fn report(curr_items: usize, clock: &Clock) -> Vec<(&'static str, String)> {
    vec![
        ("pid", std::process::id().to_string()),
        ("uptime", clock.uptime().to_string()),
        ("time", clock.now().to_string()),
        ("version", VERSION.to_string()),
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
        ("cmd_incr", CMD_INCR.value().as_u64().to_string()),
        ("cmd_decr", CMD_DECR.value().as_u64().to_string()),
        ("cmd_touch", CMD_TOUCH.value().as_u64().to_string()),
        ("cmd_flush", CMD_FLUSH.value().as_u64().to_string()),
        ("get_hits", GET_HITS.value().as_u64().to_string()),
        ("get_misses", GET_MISSES.value().as_u64().to_string()),
        ("get_expired", GET_EXPIRED.value().as_u64().to_string()),
        ("cas_hits", CAS_HITS.value().as_u64().to_string()),
        ("cas_misses", CAS_MISSES.value().as_u64().to_string()),
        ("cas_badval", CAS_BADVAL.value().as_u64().to_string()),
        ("incr_hits", INCR_HITS.value().as_u64().to_string()),
        ("incr_misses", INCR_MISSES.value().as_u64().to_string()),
        ("decr_hits", DECR_HITS.value().as_u64().to_string()),
        ("decr_misses", DECR_MISSES.value().as_u64().to_string()),
        ("touch_hits", TOUCH_HITS.value().as_u64().to_string()),
        ("touch_misses", TOUCH_MISSES.value().as_u64().to_string()),
        ("delete_hits", DELETE_HITS.value().as_u64().to_string()),
        ("delete_misses", DELETE_MISSES.value().as_u64().to_string()),
        ("curr_items", curr_items.to_string()),
        ("total_items", TOTAL_ITEMS.value().as_u64().to_string()),
        ("evictions", EVICTIONS.value().as_u64().to_string()),
        ("bytes_read", BYTES_READ.value().as_u64().to_string()),
        ("bytes_written", BYTES_WRITTEN.value().as_u64().to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat<'a>(report: &'a [(&'static str, String)], name: &str) -> &'a str {
        report
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.as_str())
            .unwrap_or_else(|| panic!("missing stat {name}"))
    }

    #[test]
    fn report_includes_server_info() {
        let clock = Clock::mock(1_000);
        clock.advance(5);
        let report = report(0, &clock);

        assert_eq!(stat(&report, "pid"), std::process::id().to_string());
        assert_eq!(stat(&report, "uptime"), "5");
        assert_eq!(stat(&report, "time"), "1005");
        assert_eq!(stat(&report, "version"), VERSION);
    }
}
