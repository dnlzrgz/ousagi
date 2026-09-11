use bytes::{Bytes, BytesMut};
use dashmap::Entry;

use crate::{
    commands::{ArithmeticOp, Command, Response, StoreOp},
    stats::Stats,
    store::{Item, Store},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn handle(cmd: Command, store: &Store) -> Response {
    let now = store.now();

    match cmd {
        Command::Get { keys, with_cas } => {
            tracing::debug!(?keys, with_cas, "get");

            Stats::incr(&store.stats.cmd_get, keys.len() as u64);

            let oldest_live = store.oldest_live();
            let mut expired_keys = Vec::with_capacity(6);
            let mut values = Vec::with_capacity(6);

            {
                for key in keys {
                    match store.items.get(&key) {
                        Some(item) if item.is_expired(now, oldest_live) => {
                            Stats::incr(&store.stats.get_expired, 1);
                            Stats::incr(&store.stats.get_misses, 1);
                            expired_keys.push(key);
                        }
                        Some(item) => {
                            Stats::incr(&store.stats.get_hits, 1);
                            let cas = with_cas.then(|| item.cas());
                            values.push((key, item.flags(), item.data().clone(), cas));
                        }
                        None => {
                            Stats::incr(&store.stats.get_misses, 1);
                        }
                    }
                }
            }

            if !expired_keys.is_empty() {
                for key in expired_keys {
                    if let Entry::Occupied(entry) = store.items.entry(key)
                        && entry.get().is_expired(now, oldest_live)
                    {
                        entry.remove();
                    }
                }
            }

            Response::Values(values)
        }
        Command::GetAndTouch {
            keys,
            exptime,
            with_cas,
        } => {
            tracing::debug!(?keys, exptime, with_cas, "gat");

            Stats::incr(&store.stats.cmd_get, keys.len() as u64);

            let oldest_live = store.oldest_live();
            let mut expired_keys = Vec::with_capacity(6);
            let mut values = Vec::with_capacity(6);

            for key in keys {
                match store.items.entry(key.clone()) {
                    Entry::Occupied(entry) if entry.get().is_expired(now, oldest_live) => {
                        Stats::incr(&store.stats.get_expired, 1);
                        Stats::incr(&store.stats.get_misses, 1);
                        expired_keys.push(key);
                    }
                    Entry::Occupied(mut entry) => {
                        Stats::incr(&store.stats.get_hits, 1);

                        let old_item = entry.get();
                        let cas = store.next_cas();
                        let expires_at = Item::resolve_expiry(exptime, now);
                        let touched = Item::with_parts(
                            old_item.data().clone(),
                            old_item.flags(),
                            expires_at,
                            cas,
                            old_item.stored_at(),
                        );

                        let flags = touched.flags();
                        let data = touched.data().clone();
                        entry.insert(touched);

                        values.push((key, flags, data, with_cas.then_some(cas)));
                    }
                    Entry::Vacant(_) => {
                        Stats::incr(&store.stats.get_misses, 1);
                    }
                }
            }

            for key in expired_keys {
                if let Entry::Occupied(entry) = store.items.entry(key)
                    && entry.get().is_expired(now, oldest_live)
                {
                    entry.remove();
                }
            }

            Response::Values(values)
        }
        Command::Store(op, args) => {
            tracing::debug!(?op, key = ?args.key, len = args.data.len(), exptime = args.exptime, "store");

            Stats::incr(&store.stats.cmd_set, 1);

            let oldest_live = store.oldest_live();
            let cas = store.next_cas();

            match op {
                StoreOp::Add => match store.items.entry(args.key) {
                    Entry::Occupied(entry) if !entry.get().is_expired(now, oldest_live) => {
                        Response::NotStored
                    }
                    Entry::Occupied(mut entry) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas, now));
                        Stats::incr(&store.stats.total_items, 1);
                        Response::Stored
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas, now));
                        Stats::incr(&store.stats.total_items, 1);
                        Response::Stored
                    }
                },
                StoreOp::Set => {
                    let item = Item::new(args.data, args.flags, args.exptime, cas, now);
                    store.items.insert(args.key, item);
                    Stats::incr(&store.stats.total_items, 1);
                    Response::Stored
                }
                StoreOp::Replace => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas, now));
                        Stats::incr(&store.stats.total_items, 1);
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
                StoreOp::Append | StoreOp::Prepend => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
                        let old_item = entry.get();

                        let mut new_data =
                            BytesMut::with_capacity(old_item.data().len() + args.data.len());

                        let (first, second) = if op == StoreOp::Append {
                            (old_item.data(), &args.data)
                        } else {
                            (&args.data, old_item.data())
                        };
                        new_data.extend_from_slice(first);
                        new_data.extend_from_slice(second);
                        let new_data = new_data.freeze();

                        let item = Item::with_parts(
                            new_data,
                            old_item.flags(),
                            old_item.expires_at(),
                            cas,
                            old_item.stored_at(),
                        );
                        entry.insert(item);
                        Stats::incr(&store.stats.total_items, 1);
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
                StoreOp::Cas => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
                        let Some(expected_cas) = args.cas else {
                            tracing::warn!("received cas command without a cas token");
                            return Response::ServerError("missing cas value");
                        };

                        if entry.get().cas() != expected_cas {
                            tracing::debug!(
                                expected = expected_cas,
                                actual = entry.get().cas(),
                                "cas mismatch"
                            );

                            Stats::incr(&store.stats.cas_badval, 1);
                            return Response::Exists;
                        }

                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas, now));
                        Stats::incr(&store.stats.cas_hits, 1);
                        Stats::incr(&store.stats.total_items, 1);
                        Response::Stored
                    }
                    _ => {
                        Stats::incr(&store.stats.cas_misses, 1);
                        Response::NotFound
                    }
                },
            }
        }
        Command::Delete { key, noreply: _ } => {
            tracing::debug!(?key, "delete");

            match store.items.remove(&key) {
                Some(_) => {
                    Stats::incr(&store.stats.delete_hits, 1);
                    Response::Deleted
                }
                None => {
                    Stats::incr(&store.stats.delete_misses, 1);
                    Response::NotFound
                }
            }
        }
        Command::Arithmetic {
            op,
            key,
            delta,
            noreply: _,
        } => {
            tracing::debug!(?op, ?key, delta, "incr/decr");

            let (hits, misses) = match op {
                ArithmeticOp::Incr => (&store.stats.incr_hits, &store.stats.incr_misses),
                ArithmeticOp::Decr => (&store.stats.decr_hits, &store.stats.decr_misses),
            };

            let oldest_live = store.oldest_live();
            let cas = store.next_cas();
            let error = match op {
                ArithmeticOp::Incr => "cannot increment non-numeric value",
                ArithmeticOp::Decr => "cannot decrement non-numeric value",
            };

            match store.items.entry(key) {
                Entry::Occupied(entry) if entry.get().is_expired(now, oldest_live) => {
                    entry.remove();
                    Stats::incr(misses, 1);
                    Response::NotFound
                }
                Entry::Occupied(mut entry) => {
                    let old_item = entry.get();

                    let Ok(s) = std::str::from_utf8(old_item.data()) else {
                        return Response::ClientError(error);
                    };

                    let Ok(val) = s.trim().parse::<u64>() else {
                        return Response::ClientError(error);
                    };

                    let new_val = match op {
                        ArithmeticOp::Incr => val.wrapping_add(delta),
                        ArithmeticOp::Decr => val.saturating_sub(delta),
                    };
                    let new_data = Bytes::from(new_val.to_string());

                    let new_item = Item::with_parts(
                        new_data,
                        old_item.flags(),
                        old_item.expires_at(),
                        cas,
                        old_item.stored_at(),
                    );

                    entry.insert(new_item);
                    Stats::incr(hits, 1);
                    Response::Number(new_val)
                }
                Entry::Vacant(_) => {
                    Stats::incr(misses, 1);
                    Response::NotFound
                }
            }
        }
        Command::Touch {
            key,
            exptime,
            noreply: _,
        } => {
            tracing::debug!(?key, ?exptime, "touch");

            let oldest_live = store.oldest_live();
            let cas = store.next_cas();

            match store.items.entry(key) {
                Entry::Occupied(entry) if entry.get().is_expired(now, oldest_live) => {
                    entry.remove();
                    Response::NotFound
                }
                Entry::Occupied(mut entry) => {
                    let old_item = entry.get();

                    let expires_at = Item::resolve_expiry(exptime, now);
                    let touched = Item::with_parts(
                        old_item.data().clone(),
                        old_item.flags(),
                        expires_at,
                        cas,
                        old_item.stored_at(),
                    );

                    entry.insert(touched);
                    Response::Touched
                }
                _ => Response::NotFound,
            }
        }
        Command::FlushAll { delay, noreply: _ } => {
            tracing::debug!(?delay, "flush_all");

            Stats::incr(&store.stats.cmd_flush, 1);

            match delay {
                Some(0) | None => {
                    store.items.clear();
                }
                Some(n) => {
                    store.flush_all(n);
                }
            }

            Response::Ok
        }
        Command::Version => {
            tracing::debug!("version");
            Response::Version(VERSION)
        }
        // `verbosity` is handled for compatibility but is a no-op.
        Command::Verbosity { level, noreply: _ } => {
            tracing::debug!(?level, "verbosity");
            Response::Ok
        }
        Command::Stats => {
            tracing::debug!("stats");
            Response::Stats(store.stats.report(store.items.len()))
        }
    }
}
