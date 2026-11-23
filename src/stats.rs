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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatValue {
    Number(u64),
    Text(&'static str),
}

pub fn report(curr_items: usize, clock: &Clock) -> [(&'static str, StatValue); 31] {
    use StatValue::{Number, Text};

    [
        ("pid", Number(std::process::id().into())),
        ("uptime", Number(clock.uptime())),
        ("time", Number(clock.now())),
        ("version", Text(VERSION)),
        (
            "curr_connections",
            Number(CURR_CONNECTIONS.value().as_u64()),
        ),
        (
            "total_connections",
            Number(TOTAL_CONNECTIONS.value().as_u64()),
        ),
        ("cmd_get", Number(CMD_GET.value().as_u64())),
        ("cmd_set", Number(CMD_SET.value().as_u64())),
        ("cmd_incr", Number(CMD_INCR.value().as_u64())),
        ("cmd_decr", Number(CMD_DECR.value().as_u64())),
        ("cmd_touch", Number(CMD_TOUCH.value().as_u64())),
        ("cmd_flush", Number(CMD_FLUSH.value().as_u64())),
        ("get_hits", Number(GET_HITS.value().as_u64())),
        ("get_misses", Number(GET_MISSES.value().as_u64())),
        ("get_expired", Number(GET_EXPIRED.value().as_u64())),
        ("cas_hits", Number(CAS_HITS.value().as_u64())),
        ("cas_misses", Number(CAS_MISSES.value().as_u64())),
        ("cas_badval", Number(CAS_BADVAL.value().as_u64())),
        ("incr_hits", Number(INCR_HITS.value().as_u64())),
        ("incr_misses", Number(INCR_MISSES.value().as_u64())),
        ("decr_hits", Number(DECR_HITS.value().as_u64())),
        ("decr_misses", Number(DECR_MISSES.value().as_u64())),
        ("touch_hits", Number(TOUCH_HITS.value().as_u64())),
        ("touch_misses", Number(TOUCH_MISSES.value().as_u64())),
        ("delete_hits", Number(DELETE_HITS.value().as_u64())),
        ("delete_misses", Number(DELETE_MISSES.value().as_u64())),
        ("curr_items", Number(curr_items as u64)),
        ("total_items", Number(TOTAL_ITEMS.value().as_u64())),
        ("evictions", Number(EVICTIONS.value().as_u64())),
        ("bytes_read", Number(BYTES_READ.value().as_u64())),
        ("bytes_written", Number(BYTES_WRITTEN.value().as_u64())),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(report: &[(&'static str, StatValue)], name: &str) -> StatValue {
        report
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("missing stat {name}"))
    }

    #[test]
    fn report_includes_server_info() {
        let clock = Clock::mock(1_000);
        clock.advance(5);
        let report = report(0, &clock);

        assert_eq!(
            stat(&report, "pid"),
            StatValue::Number(std::process::id().into())
        );
        assert_eq!(stat(&report, "uptime"), StatValue::Number(5));
        assert_eq!(stat(&report, "time"), StatValue::Number(1005));
        assert_eq!(stat(&report, "version"), StatValue::Text(VERSION));
    }
}
