use std::{io, time::SystemTime};

use bytes::{Bytes, BytesMut};
use dashmap::Entry;
use tokio::{
    io::{
        AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
    },
    net::TcpStream,
};

use crate::{
    commands::{ArithmeticOp, Command, Response, StoreOp},
    parser::{CommandHeader, parse_command_line},
    store::{Item, Store},
};

const MAX_LINE_LEN: u64 = 8 * 1024; // bytes

enum ReadLineError {
    Io(io::Error),
    TooLong,
}

impl From<io::Error> for ReadLineError {
    fn from(e: io::Error) -> Self {
        ReadLineError::Io(e)
    }
}

enum PayloadError {
    Io(io::Error),
    BadChunk,
}

impl From<io::Error> for PayloadError {
    fn from(e: io::Error) -> Self {
        PayloadError::Io(e)
    }
}

pub(crate) struct Connection<R, W> {
    reader: BufReader<R>,
    writer: BufWriter<W>,
    line: Vec<u8>,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Connection<R, W> {
    fn new(reader: R, writer: W) -> Self {
        Connection {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            line: Vec::with_capacity(128),
        }
    }

    async fn read_line(&mut self) -> Result<Option<Bytes>, ReadLineError> {
        self.line.clear();

        if self.reader.buffer().is_empty() {
            self.writer.flush().await?;
        }

        let n = {
            let mut limited = (&mut self.reader).take(MAX_LINE_LEN);
            limited.read_until(b'\n', &mut self.line).await?
        };

        if n == 0 {
            return Ok(None); // EOF
        }

        if self.line.last() != Some(&b'\n') {
            if n as u64 == MAX_LINE_LEN {
                return Err(ReadLineError::TooLong);
            } else {
                return Ok(None); // EOF without newline
            }
        }

        while matches!(self.line.last(), Some(b'\n') | Some(b'\r')) {
            self.line.pop();
        }

        let line = std::mem::replace(&mut self.line, Vec::with_capacity(128));
        Ok(Some(Bytes::from(line)))
    }

    async fn read_payload(&mut self, len: usize) -> Result<Bytes, PayloadError> {
        let mut data = BytesMut::with_capacity(len);
        while data.len() < len {
            if self.reader.read_buf(&mut data).await? == 0 {
                return Err(PayloadError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-payload",
                )));
            }
        }

        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf).await?;
        if crlf != *b"\r\n" {
            return Err(PayloadError::BadChunk);
        }

        Ok(data.freeze())
    }

    async fn read_command(&mut self) -> io::Result<Option<Command>> {
        loop {
            let line = match self.read_line().await {
                Ok(Some(line)) => line,
                Ok(None) => return Ok(None),
                Err(ReadLineError::TooLong) => {
                    self.discard_until_newline().await?;
                    self.write_response(&Response::ClientError("line too long"))
                        .await?;
                    continue;
                }
                Err(ReadLineError::Io(e)) => return Err(e),
            };

            let header = match parse_command_line(&line) {
                Ok(header) => header,
                Err(err) => {
                    if let Some(n) = err.discard() {
                        self.discard_exact(n).await?;
                    }

                    self.write_response(&err.response()).await?;
                    continue;
                }
            };

            let command = match header {
                CommandHeader::Immediate(cmd) => cmd,
                CommandHeader::Store(pending) => match self.read_payload(pending.len).await {
                    Ok(data) => pending.into_command(data),
                    Err(PayloadError::BadChunk) => {
                        self.write_response(&Response::ClientError("bad data chunk"))
                            .await?;
                        continue;
                    }
                    Err(PayloadError::Io(e)) => return Err(e),
                },
            };

            return Ok(Some(command));
        }
    }

    async fn write_response(&mut self, resp: &Response) -> io::Result<()> {
        match resp {
            Response::Stored => self.writer.write_all(b"STORED\r\n").await?,
            Response::NotStored => self.writer.write_all(b"NOT_STORED\r\n").await?,
            Response::Deleted => self.writer.write_all(b"DELETED\r\n").await?,
            Response::NotFound => self.writer.write_all(b"NOT_FOUND\r\n").await?,
            Response::Exists => self.writer.write_all(b"EXISTS\r\n").await?,
            Response::Number(val) => {
                let mut buf = itoa::Buffer::new();
                self.writer.write_all(buf.format(*val).as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Response::Error => self.writer.write_all(b"ERROR\r\n").await?,
            Response::Ok => self.writer.write_all(b"OK\r\n").await?,
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
                let mut itoa_buf = itoa::Buffer::new();

                for (key, flags, data, cas) in values {
                    self.writer.write_all(b"VALUE ").await?;
                    self.writer.write_all(key).await?;
                    self.writer.write_all(b" ").await?;
                    self.writer
                        .write_all(itoa_buf.format(*flags).as_bytes())
                        .await?;
                    self.writer.write_all(b" ").await?;
                    self.writer
                        .write_all(itoa_buf.format(data.len()).as_bytes())
                        .await?;

                    if let Some(c) = cas {
                        self.writer.write_all(b" ").await?;
                        self.writer
                            .write_all(itoa_buf.format(*c).as_bytes())
                            .await?;
                    }

                    self.writer.write_all(b"\r\n").await?;
                    self.writer.write_all(data).await?;
                    self.writer.write_all(b"\r\n").await?;
                }
                self.writer.write_all(b"END\r\n").await?;
            }
        }

        Ok(())
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
    let now = SystemTime::now();

    match cmd {
        Command::Get { keys, with_cas } => {
            tracing::debug!(?keys, with_cas, "get");

            let oldest_live = store.oldest_live();
            let mut expired_keys = Vec::new();
            let mut values = Vec::new();

            {
                for key in keys {
                    match store.items.get(&key) {
                        Some(item) if item.is_expired(now, oldest_live) => expired_keys.push(key),
                        Some(item) => {
                            let cas = with_cas.then(|| item.cas());
                            values.push((key, item.flags(), item.data().clone(), cas));
                        }
                        None => {}
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
        Command::Store(op, args) => {
            tracing::debug!(?op, key = ?args.key, len = args.data.len(), exptime = args.exptime, "store");

            let oldest_live = store.oldest_live();
            let cas = store.next_cas();

            match op {
                StoreOp::Add => match store.items.entry(args.key) {
                    Entry::Occupied(entry) if !entry.get().is_expired(now, oldest_live) => {
                        Response::NotStored
                    }
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
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime, cas));
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
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
                StoreOp::Cas => match store.items.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
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

            match store.items.remove(&key) {
                Some(_) => Response::Deleted,
                None => Response::NotFound,
            }
        }
        Command::Arithmetic {
            op,
            key,
            delta,
            noreply: _,
        } => {
            tracing::debug!(?op, ?key, delta, "incr/decr");

            let oldest_live = store.oldest_live();
            let cas = store.next_cas();
            let error = match op {
                ArithmeticOp::Incr => "cannot increment non-numeric value",
                ArithmeticOp::Decr => "cannot decrement non-numeric value",
            };

            match store.items.entry(key) {
                Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
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
                    Response::Number(new_val)
                }
                _ => Response::NotFound,
            }
        }
        Command::FlushAll { delay, noreply: _ } => {
            tracing::debug!(?delay, "flush_all");

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
    use crate::commands::StoreArgs;
    use crate::store::StoreInner;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf, duplex, split};

    /// Fresh, empty Store.
    fn empty_store() -> Store {
        Arc::new(StoreInner::new())
    }

    /// Store pre-populated with one item under `key`.
    fn store_with(key: &str, item: Item) -> Store {
        let inner = StoreInner::new();
        inner
            .items
            .insert(Bytes::copy_from_slice(key.as_bytes()), item);
        Arc::new(inner)
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
        let store = store_with("foo", Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
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

        assert!(store.items.get("foo".as_bytes()).is_none());
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

        let item = store
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
        let store = store_with("foo", Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1));

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

        let item = store
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"jello");
    }

    #[test]
    fn replace_missing_key_returns_not_stored() {
        let store = store_with("foo", Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1));

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
            "foo",
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

        let item = store.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"hello");
        assert_eq!(item.flags(), 42);
    }

    #[test]
    fn set_overwrites_existing_key() {
        let store = store_with("foo", Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
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

        let item = store.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 21);
    }

    #[test]
    fn delete_existing_key_returns_deleted_and_removes_it() {
        let store = store_with("foo", Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
        let cmd = Command::Delete {
            key: "foo".into(),
            noreply: true,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Deleted));

        assert!(store.items.get("foo".as_bytes()).is_none());
    }

    #[test]
    fn delete_missing_key_returns_not_found() {
        let store = store_with("foo", Item::new(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
        let cmd = Command::Delete {
            key: "bar".into(),
            noreply: true,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotFound));

        assert!(store.items.get("foo".as_bytes()).is_some());
    }

    #[test]
    fn incr_existing_key_increments_value_and_returns_it() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"10"), 42, 0, 1));
        let cmd = Command::Arithmetic {
            op: ArithmeticOp::Incr,
            key: "foo".into(),
            delta: 5,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Number(15)));

        let item = store
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"15");
    }

    #[test]
    fn incr_missing_key_returns_not_found() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"10"), 42, 0, 1));
        let cmd = Command::Arithmetic {
            op: ArithmeticOp::Incr,
            key: "bar".into(),
            delta: 5,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn incr_overflow_wraps_around() {
        let store = store_with(
            "foo",
            Item::new(Bytes::from(u64::MAX.to_string()), 42, 0, 1),
        );
        let cmd = Command::Arithmetic {
            op: ArithmeticOp::Incr,
            key: "foo".into(),
            delta: 1,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Number(0)));

        let item = store
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"0");
    }

    #[test]
    fn decr_existing_key_decrements_value_and_returns_it() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"10"), 42, 0, 1));
        let cmd = Command::Arithmetic {
            op: ArithmeticOp::Decr,
            key: "foo".into(),
            delta: 5,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Number(5)));

        let item = store
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"5");
    }

    #[test]
    fn decr_missing_key_returns_not_found() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"10"), 42, 0, 1));
        let cmd = Command::Arithmetic {
            op: ArithmeticOp::Decr,
            key: "bar".into(),
            delta: 5,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn decr_underflow_saturates_at_zero() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"0"), 42, 0, 1));
        let cmd = Command::Arithmetic {
            op: ArithmeticOp::Decr,
            key: "foo".into(),
            delta: 1,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Number(0)));

        let item = store
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"0");
    }

    #[test]
    fn flush_all_immediate_clears_all_items_and_returns_ok() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"0"), 42, 0, 1));
        let cmd = Command::FlushAll {
            delay: None,
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Ok));

        assert!(store.items.is_empty());
    }

    #[test]
    fn flush_all_with_delay_does_not_immediately_remove_items() {
        let store = store_with("foo", Item::new(Bytes::from_static(b"0"), 42, 0, 1));
        let cmd = Command::FlushAll {
            delay: Some(3600),
            noreply: false,
        };

        let resp = execute(cmd, &store);
        assert!(matches!(resp, Response::Ok));

        let item = store
            .items
            .get("foo".as_bytes())
            .expect("key should still be present");
        assert_eq!(item.data().as_ref(), b"0");
    }
}
