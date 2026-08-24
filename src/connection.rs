use std::{collections::hash_map::Entry, io, time::SystemTime};

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{
        AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
    },
    net::TcpStream,
};

use crate::{
    commands::{Command, Response, StoreArgs, StoreOp},
    store::{Item, Store},
};

pub(crate) struct Connection<R, W> {
    reader: BufReader<R>,
    writer: BufWriter<W>,
    line: Vec<u8>,
}

const MAX_ITEM_SIZE: usize = 1024 * 1024; // 1 MiB
const MAX_KEY_LEN: usize = 250; // bytes
const MAX_LINE_LEN: u64 = 8 * 1024; // bytes

fn validate_key(key: &[u8]) -> Result<(), ()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(());
    }

    if key.iter().any(|&b| b <= 0x20 || b == 0x7F) {
        return Err(());
    }

    Ok(())
}

fn parse_field<T: std::str::FromStr>(tok: &[u8]) -> Result<T, ()> {
    std::str::from_utf8(tok)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(())
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Connection<R, W> {
    fn new(reader: R, writer: W) -> Self {
        Connection {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            line: Vec::new(),
        }
    }

    async fn read_command(&mut self) -> io::Result<Option<Command>> {
        loop {
            self.line.clear();

            let n = {
                let mut limited = (&mut self.reader).take(MAX_LINE_LEN);
                limited.read_until(b'\n', &mut self.line).await?
            };

            if n == 0 {
                return Ok(None); // EOF
            }

            if self.line.last() != Some(&b'\n') {
                if n as u64 == MAX_LINE_LEN {
                    tracing::warn!("line exceeds {MAX_LINE_LEN} bytes");
                    self.discard_until_newline().await?;
                    self.write_response(&Response::ClientError("line too long"))
                        .await?;
                    continue;
                } else {
                    return Ok(None); // EOF
                }
            }

            while matches!(self.line.last(), Some(b'\n') | Some(b'\r')) {
                self.line.pop();
            }

            let parts: Vec<&[u8]> = self
                .line
                .split(|&b| b == b' ')
                .filter(|s| !s.is_empty())
                .collect();

            match parts.as_slice() {
                [op @ (b"get" | b"gets"), keys @ ..] if !keys.is_empty() => {
                    let with_cas = *op == b"gets";

                    let parsed: Result<Vec<Bytes>, ()> = keys
                        .iter()
                        .map(|k| {
                            validate_key(k)?;
                            Ok(Bytes::copy_from_slice(k))
                        })
                        .collect();

                    match parsed {
                        Ok(out) => {
                            return Ok(Some(Command::Get {
                                keys: out,
                                with_cas,
                            }));
                        }
                        Err(()) => {
                            self.write_response(&Response::ClientError("bad command line format"))
                                .await?;
                            continue;
                        }
                    }
                }
                [
                    op @ (b"add" | b"set" | b"replace" | b"append" | b"prepend" | b"cas"),
                    key,
                    flags,
                    exptime,
                    bytes_len,
                    rest @ ..,
                ] => {
                    let store_op = match *op {
                        b"add" => StoreOp::Add,
                        b"replace" => StoreOp::Replace,
                        b"set" => StoreOp::Set,
                        b"append" => StoreOp::Append,
                        b"prepend" => StoreOp::Prepend,
                        b"cas" => StoreOp::Cas,
                        _ => unreachable!(),
                    };
                    let Ok(bytes_len) = parse_field::<usize>(bytes_len) else {
                        tracing::warn!(line = %&String::from_utf8_lossy(&self.line), "malformed byte length");
                        self.write_response(&Response::ClientError("bad command line format"))
                            .await?;
                        return Ok(None);
                    };

                    if bytes_len > MAX_ITEM_SIZE {
                        tracing::warn!(bytes_len, max = MAX_ITEM_SIZE, "item exceeds max size");
                        self.discard_exact(bytes_len + 2).await?;
                        self.write_response(&Response::ServerError("object too large for cache"))
                            .await?;
                        continue;
                    }

                    if validate_key(key).is_err() {
                        tracing::warn!(key = %String::from_utf8_lossy(key), "invalid key");
                        self.discard_exact(bytes_len + 2).await?;
                        self.write_response(&Response::ClientError("bad command line format"))
                            .await?;
                        continue;
                    }

                    let (Ok(flags), Ok(exptime)) =
                        (parse_field::<u32>(flags), parse_field::<i64>(exptime))
                    else {
                        tracing::warn!(line = %String::from_utf8_lossy(&self.line), "malformed flags/exptime");
                        self.discard_exact(bytes_len + 2).await?;
                        self.write_response(&Response::ClientError("bad command line format"))
                            .await?;
                        continue;
                    };

                    let mut data = BytesMut::zeroed(bytes_len);
                    self.reader.read_exact(&mut data).await?;
                    let data = data.freeze();

                    let mut crlf = [0u8; 2];
                    self.reader.read_exact(&mut crlf).await?;
                    if crlf != *b"\r\n" {
                        tracing::warn!("missing trailing CRLF after data block");
                        self.write_response(&Response::ClientError("bad data chunk"))
                            .await?;
                        continue;
                    }

                    let (cas, rest) = if store_op == StoreOp::Cas {
                        match rest {
                            [cas_unique, rest @ ..] => {
                                let Ok(cas_unique) = parse_field::<u64>(cas_unique) else {
                                    self.write_response(&Response::ClientError(
                                        "bad command line format",
                                    ))
                                    .await?;
                                    continue;
                                };
                                (Some(cas_unique), rest)
                            }
                            [] => {
                                self.write_response(&Response::ClientError(
                                    "bad command line format",
                                ))
                                .await?;
                                continue;
                            }
                        }
                    } else {
                        (None, rest)
                    };

                    let noreply = match rest {
                        [] => false,
                        [b"noreply"] => true,
                        _ => {
                            self.write_response(&Response::ClientError("bad command line format"))
                                .await?;
                            continue;
                        }
                    };

                    return Ok(Some(Command::Store(
                        store_op,
                        StoreArgs {
                            key: Bytes::copy_from_slice(key),
                            flags,
                            exptime,
                            data,
                            noreply,
                            cas,
                        },
                    )));
                }
                [b"delete", key, rest @ ..] => {
                    if validate_key(key).is_err() {
                        self.write_response(&Response::ClientError("bad command line format"))
                            .await?;
                        continue;
                    }

                    let noreply = match rest {
                        [] => false,
                        [b"noreply"] => true,
                        _ => {
                            self.write_response(&Response::ClientError("bad command line format"))
                                .await?;
                            continue;
                        }
                    };

                    return Ok(Some(Command::Delete {
                        key: Bytes::copy_from_slice(key),
                        noreply,
                    }));
                }
                _ => {
                    tracing::warn!(line = %String::from_utf8_lossy(&self.line), "unrecognized command");
                    self.write_response(&Response::Error).await?;
                    continue;
                }
            }
        }
    }

    async fn write_response(&mut self, resp: &Response) -> io::Result<()> {
        match resp {
            Response::Stored => self.writer.write_all(b"STORED\r\n").await?,
            Response::NotStored => self.writer.write_all(b"NOT_STORED\r\n").await?,
            Response::Deleted => self.writer.write_all(b"DELETED\r\n").await?,
            Response::NotFound => self.writer.write_all(b"NOT_FOUND\r\n").await?,
            Response::Exists => self.writer.write_all(b"EXISTS\r\n").await?,
            Response::Error => self.writer.write_all(b"ERROR\r\n").await?,
            Response::ClientError(msg) => {
                self.writer.write_all(b"CLIENT_ERROR ").await?;
                self.writer.write_all(msg.as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Response::ServerError(msg) => {
                self.writer.write_all(b"SERVER_ERROR ").await?;
                self.writer.write_all(msg.as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Response::Values(values) => {
                for (key, flags, data, cas) in values {
                    self.writer.write_all(b"VALUE ").await?;
                    self.writer.write_all(key).await?;
                    let meta = match cas {
                        Some(cas) => format!(" {flags} {} {cas}\r\n", data.len()),
                        None => format!(" {flags} {}\r\n", data.len()),
                    };
                    self.writer.write_all(meta.as_bytes()).await?;
                    self.writer.write_all(data).await?;
                    self.writer.write_all(b"\r\n").await?;
                }
                self.writer.write_all(b"END\r\n").await?;
            }
        }

        self.writer.flush().await
    }

    async fn discard_exact(&mut self, n: usize) -> io::Result<()> {
        let mut limited = (&mut self.reader).take(n as u64);
        tokio::io::copy(&mut limited, &mut tokio::io::sink()).await?;
        Ok(())
    }

    async fn discard_until_newline(&mut self) -> io::Result<()> {
        let mut junk = Vec::new();
        loop {
            junk.clear();

            let mut limited = (&mut self.reader).take(MAX_LINE_LEN);
            let n = limited.read_until(b'\n', &mut junk).await?;
            if n == 0 || junk.last() == Some(&b'\n') {
                return Ok(());
            }
        }
    }
}

pub(crate) fn execute(cmd: Command, store: &Store) -> Response {
    match cmd {
        Command::Get { keys, with_cas } => {
            tracing::debug!(?keys, with_cas, "get");

            let now = SystemTime::now();
            let mut expired_keys = Vec::new();
            let mut values = Vec::new();

            {
                let store = store.read().unwrap();
                for key in keys {
                    match store.items.get(&key) {
                        Some(item) if item.is_expired(now) => expired_keys.push(key),
                        Some(item) => {
                            let cas = if with_cas { Some(item.cas()) } else { None };
                            values.push((key, item.flags(), item.data().clone(), cas));
                        }
                        _ => {}
                    }
                }
            }

            if !expired_keys.is_empty() {
                let mut store = store.write().unwrap();
                for key in expired_keys {
                    if let Entry::Occupied(entry) = store.items.entry(key)
                        && entry.get().is_expired(now)
                    {
                        entry.remove();
                    }
                }
            }

            Response::Values(values)
        }
        Command::Store(op, args) => {
            tracing::debug!(?op, key = ?args.key, len = args.data.len(), exptime = args.exptime, "store");

            let now = SystemTime::now();
            let mut store = store.write().unwrap();

            let cas = store.next_cas;
            store.next_cas += 1;

            match op {
                StoreOp::Add => match store.items.entry(args.key) {
                    Entry::Occupied(entry) if !entry.get().is_expired(now) => Response::NotStored,
                    Entry::Occupied(mut entry) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas));
                        Response::Stored
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas));
                        Response::Stored
                    }
                },
                StoreOp::Set => {
                    let item = Item::new(args.data, args.flags, args.exptime, cas);
                    store.items.insert(args.key, item);
                    Response::Stored
                }
                StoreOp::Replace => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas));
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
                StoreOp::Append | StoreOp::Prepend => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now) => {
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
                        );
                        entry.insert(item);
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
                StoreOp::Cas => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now) => {
                        if entry.get().cas() != args.cas.unwrap() {
                            tracing::debug!(
                                expected = args.cas.unwrap(),
                                actual = entry.get().cas(),
                                "cas mismatch"
                            );

                            return Response::Exists;
                        }

                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas));
                        Response::Stored
                    }
                    _ => Response::NotFound,
                },
            }
        }
        Command::Delete { key, noreply: _ } => {
            tracing::debug!(?key, "delete");

            let mut store = store.write().unwrap();
            match store.items.remove(&key) {
                Some(_) => Response::Deleted,
                None => Response::NotFound,
            }
        }
    }
}

pub async fn process(socket: TcpStream, store: Store) -> io::Result<()> {
    let (r, w) = socket.into_split();
    let mut conn = Connection::new(r, w);

    while let Some(cmd) = conn.read_command().await? {
        let noreply = cmd.noreply();
        let resp = execute(cmd, &store);
        if !noreply {
            conn.write_response(&resp).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoreInner;
    use bytes::Bytes;
    use std::sync::{Arc, RwLock};
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf, duplex, split};

    /// Fresh, empty Store.
    fn empty_store() -> Store {
        Arc::new(RwLock::new(StoreInner::new()))
    }

    /// Store pre-populated with one item under `key`.
    fn store_with(key: &str, item: Item) -> Store {
        let mut inner = StoreInner::new();
        inner
            .items
            .insert(Bytes::copy_from_slice(key.as_bytes()), item);
        Arc::new(RwLock::new(inner))
    }

    /// Builds a mock connection wired to an in-memory duplex pipeline instead of
    /// a real `TcpStream`. It also gives back the client end of the pipe for the test.
    fn mock_connection(
        cap: usize,
    ) -> (
        Connection<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>,
        DuplexStream,
    ) {
        let (server_end, client_end) = duplex(cap);
        let (server_r, server_w) = split(server_end);
        (Connection::new(server_r, server_w), client_end)
    }

    #[tokio::test]
    async fn key_exceeding_max_len_returns_error() {
        let (mut conn, mut client) = mock_connection(1024);
        let long_key = "k".repeat(251);
        let request = format!("get {}\r\n", long_key);

        tokio::spawn(async move {
            let _ = conn.read_command().await;
        });

        client.write_all(request.as_bytes()).await.unwrap();

        let mut buf = [0; 128];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"CLIENT_ERROR bad command line format\r\n");
    }

    #[tokio::test]
    async fn line_exceeding_max_len_returns_error() {
        let (mut conn, mut client) = mock_connection(16384);
        let mut request = vec![b'a'; 8192];
        request.extend_from_slice(b"\n");

        tokio::spawn(async move {
            let _ = conn.read_command().await;
        });

        client.write_all(&request).await.unwrap();

        let mut buf = [0; 128];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"CLIENT_ERROR line too long\r\n");
    }

    #[test]
    fn get_missing_key_returns_not_found() {
        let store = empty_store();
        let cmd = Command::Get {
            keys: vec!["foo".into()],
            with_cas: false,
        };

        match execute(cmd, &store) {
            Response::Values(values) => assert!(values.is_empty()),
            other => panic!("expected Values, got: {:?}", other),
        }
    }

    #[test]
    fn get_multiple_keys_skips_missing_one() {
        let store = store_with(
            &"foo".to_string(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1),
        );
        let cmd = Command::Get {
            keys: vec!["foo".into(), "bar".into()],
            with_cas: false,
        };

        match execute(cmd, &store) {
            Response::Values(values) => assert!(values.len() == 1),
            other => panic!("expected Values, got: {:?}", other),
        }
    }

    #[test]
    fn get_expired_key_is_treated_as_missing_and_lazily_removed() {
        let store = store_with(
            "foo",
            Item::new(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
        );

        let resp = execute(
            Command::Get {
                keys: vec!["foo".into()],
                with_cas: false,
            },
            &store,
        );

        match resp {
            Response::Values(values) => assert!(values.is_empty()),
            other => panic!("expected Values, got {:?}", other),
        }

        let s = store.read().unwrap();
        assert!(s.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn add_new_key_stores_it_and_returns_stored() {
        let store = empty_store();
        let cmd = Command::Store(
            StoreOp::Add,
            StoreArgs {
                key: "foo".into(),
                flags: 42,
                exptime: 0,
                data: Bytes::from_static(b"hello"),
                noreply: false,
                cas: None,
            },
        );

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Stored));
    }

    #[test]
    fn add_existing_key_fails_and_returns_not_stored() {
        let item = Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1);
        let cmd = Command::Store(
            StoreOp::Add,
            StoreArgs {
                key: "foo".into(),
                flags: 50,
                exptime: 0,
                data: Bytes::from_static(b"jello"),
                noreply: false,
                cas: None,
            },
        );
        let store = store_with("foo", item);

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotStored));

        let s = store.read().unwrap();
        let item = s
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn add_existing_expired_key_overwrites_and_returns_stored() {
        let item = Item::new(Bytes::copy_from_slice(b"hello"), 42, -1, 1);
        let cmd = Command::Store(
            StoreOp::Add,
            StoreArgs {
                key: "foo".into(),
                flags: item.flags(),
                exptime: 0,
                data: item.data().clone(),
                noreply: false,
                cas: None,
            },
        );
        let store = store_with("foo", item);

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Stored));
    }

    #[test]
    fn replace_existing_key_stores_new_value() {
        let store = store_with(
            "foo".into(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1),
        );

        let cmd = Command::Store(
            StoreOp::Replace,
            StoreArgs {
                key: "foo".into(),
                flags: 42,
                exptime: 0,
                data: Bytes::from_static(b"jello"),
                noreply: false,
                cas: None,
            },
        );

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Stored));

        let s = store.read().unwrap();
        let item = s
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"jello");
    }

    #[test]
    fn replace_missing_key_returns_not_stored() {
        let store = store_with(
            "foo".into(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1),
        );

        let cmd = Command::Store(
            StoreOp::Replace,
            StoreArgs {
                key: "bar".into(),
                flags: 42,
                exptime: 0,
                data: Bytes::from_static(b"hello"),
                noreply: false,
                cas: None,
            },
        );

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotStored));
    }

    #[test]
    fn replace_expired_key_returns_not_stored() {
        let store = store_with(
            "foo".into(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
        );

        let cmd = Command::Store(
            StoreOp::Replace,
            StoreArgs {
                key: "foo".into(),
                flags: 42,
                exptime: 0,
                data: Bytes::from_static(b"jello"),
                noreply: false,
                cas: None,
            },
        );

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotStored));
    }

    #[test]
    fn set_new_key_stores_value() {
        let store = empty_store();
        let cmd = Command::Store(
            StoreOp::Set,
            StoreArgs {
                key: "foo".into(),
                flags: 42,
                exptime: 0,
                data: Bytes::from_static(b"hello"),
                noreply: false,
                cas: None,
            },
        );

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Stored));

        let s = store.read().unwrap();
        let item = s.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn set_overwrites_existing_key() {
        let store = store_with(
            "foo".into(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1),
        );
        let cmd = Command::Store(
            StoreOp::Set,
            StoreArgs {
                key: "foo".into(),
                flags: 21,
                exptime: 0,
                data: Bytes::from_static(b"jello"),
                noreply: false,
                cas: None,
            },
        );

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Stored));

        let s = store.read().unwrap();
        let item = s.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 21);
    }

    #[test]
    fn delete_existing_key_returns_deleted_and_removes_it() {
        let store = store_with(
            "foo".into(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1),
        );
        let cmd = Command::Delete {
            key: "foo".into(),
            noreply: true,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Deleted));

        let s = store.read().unwrap();
        assert!(s.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn delete_missing_key_returns_not_found() {
        let store = store_with(
            "foo".into(),
            Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1),
        );
        let cmd = Command::Delete {
            key: "bar".into(),
            noreply: true,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotFound));

        let s = store.read().unwrap();
        assert!(s.items.get("foo".as_bytes()).is_some());
    }
}
