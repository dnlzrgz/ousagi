use tracing::{Level, instrument};

use crate::{
    commands::{Command, Response},
    store::Store,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[instrument(skip(store), level = Level::DEBUG)]
pub fn handle(cmd: Command, store: &Store) -> Response {
    match cmd {
        Command::Get { keys, with_cas } => store.get(&keys, with_cas),
        Command::GetAndTouch {
            keys,
            exptime,
            with_cas,
        } => store.get_and_touch(&keys, exptime, with_cas),
        Command::Store(op, args) => store.store(op, args),
        Command::Delete { key, .. } => store.delete(&key),
        Command::Arithmetic { op, key, delta, .. } => store.arithmetic(op, &key, delta),
        Command::Touch { key, exptime, .. } => store.touch(&key, exptime),
        Command::FlushAll { delay, .. } => store.flush(delay),
        Command::Version => Response::Version(VERSION),
        Command::Verbosity { .. } => Response::Ok,
        Command::Stats => store.stats_report(),
    }
}
