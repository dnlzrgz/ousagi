use tracing::{Level, instrument};

use crate::{
    commands::{Command, Response},
    output::Output,
    stats,
    store::Store,
};

pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");

#[instrument(skip(store, out), level = Level::DEBUG)]
pub fn handle(cmd: Command, store: &Store, out: &mut Output) {
    let noreply = cmd.noreply();
    let resp = match cmd {
        Command::Get { keys, with_cas } => {
            store.get(&keys, with_cas, |value| out.value(value));
            Response::End
        }
        Command::GetAndTouch {
            keys,
            exptime,
            with_cas,
        } => {
            store.get_and_touch(&keys, exptime, with_cas, |value| out.value(value));
            Response::End
        }
        Command::Store(op, args) => store.store(op, args),
        Command::StoreTooLarge { op, key, .. } => store.reject_too_large(op, &key),
        Command::Delete { key, .. } => store.delete(&key),
        Command::Arithmetic { op, key, delta, .. } => store.arithmetic(op, &key, delta),
        Command::Touch { key, exptime, .. } => store.touch(&key, exptime),
        Command::FlushAll { delay, .. } => store.flush(delay),
        Command::Version => Response::Version(VERSION),
        Command::Verbosity { .. } => Response::Ok,
        Command::Stats => {
            for (name, value) in stats::report(store.item_count(), store.clock()) {
                out.stat(name, value);
            }
            Response::End
        }
    };

    if !noreply {
        out.response(&resp);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        clock::Clock,
        commands::{StoreArgs, StoreOp},
    };

    fn store() -> Store {
        Store::new(Clock::mock(1_000_000), 1024 * 1024)
    }

    fn set(key: &str, data: &str, noreply: bool) -> Command {
        Command::Store(
            StoreOp::Set,
            StoreArgs {
                key: Bytes::copy_from_slice(key.as_bytes()),
                flags: 3,
                exptime: 0,
                data: Bytes::copy_from_slice(data.as_bytes()),
                noreply,
            },
        )
    }

    fn get(keys: &[&str]) -> Command {
        Command::Get {
            keys: keys
                .iter()
                .map(|k| Bytes::copy_from_slice(k.as_bytes()))
                .collect(),
            with_cas: false,
        }
    }

    async fn queued(out: &mut Output) -> Vec<u8> {
        let mut written = Vec::new();
        out.write_to(&mut written).await.unwrap();
        written
    }

    #[tokio::test]
    async fn noreply_store_queues_nothing() {
        let store = store();
        let mut out = Output::default();
        handle(set("k", "v", true), &store, &mut out);
        assert!(out.is_empty());
        assert_eq!(store.item_count(), 1);
    }

    #[tokio::test]
    async fn store_queues_stored() {
        let store = store();
        let mut out = Output::default();
        handle(set("k", "v", false), &store, &mut out);
        assert_eq!(queued(&mut out).await, b"STORED\r\n");
    }

    #[tokio::test]
    async fn get_hit_queues_value_then_end() {
        let store = store();
        let mut out = Output::default();
        handle(set("k", "hello", true), &store, &mut out);
        handle(get(&["k"]), &store, &mut out);
        assert_eq!(queued(&mut out).await, b"VALUE k 3 5\r\nhello\r\nEND\r\n");
    }

    #[tokio::test]
    async fn get_miss_queues_only_end() {
        let store = store();
        let mut out = Output::default();
        handle(get(&["missing"]), &store, &mut out);
        assert_eq!(queued(&mut out).await, b"END\r\n");
    }
}
