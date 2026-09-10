use std::io;

use bytes::{Buf, Bytes, BytesMut};
use dashmap::Entry;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    commands::{ArithmeticOp, Command, Response, StoreOp},
    parser::{CommandHeader, parse_command_line},
    stats::Stats,
    store::{Item, Store},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

const MAX_LINE_LEN: u64 = 8 * 1024; // bytes

#[derive(Debug)]
pub enum ReadLineError {
    Io(io::Error),
    TooLong,
}

impl From<io::Error> for ReadLineError {
    fn from(e: io::Error) -> Self {
        ReadLineError::Io(e)
    }
}

#[derive(Debug)]
pub enum PayloadError {
    Io(io::Error),
    BadChunk,
}

impl From<io::Error> for PayloadError {
    fn from(e: io::Error) -> Self {
        PayloadError::Io(e)
    }
}

pub struct Connection<R, W> {
    reader: R,
    writer: BufWriter<W>,
    buffer: BytesMut,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Connection<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Connection {
            reader,
            writer: BufWriter::new(writer),
            buffer: BytesMut::new(),
        }
    }

    pub async fn read_line(&mut self) -> Result<Option<Bytes>, ReadLineError> {
        loop {
            if let Some(pos) = memchr::memchr(b'\n', &self.buffer) {
                let mut line = self.buffer.split_to(pos + 1).freeze();

                while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
                    line.truncate(line.len() - 1);
                }

                return Ok(Some(line));
            }

            if self.buffer.len() as u64 >= MAX_LINE_LEN {
                return Err(ReadLineError::TooLong);
            }

            if self.fill_buffer().await? == 0 {
                return Ok(None);
            }
        }
    }

    pub async fn read_payload(&mut self, len: usize) -> Result<Bytes, PayloadError> {
        let mut data = BytesMut::with_capacity(len);

        let from_buffer = self.buffer.len().min(len);
        data.extend_from_slice(&self.buffer[..from_buffer]);
        self.buffer.advance(from_buffer);

        while data.len() < len {
            self.writer.flush().await?;
            if self.reader.read_buf(&mut data).await? == 0 {
                return Err(PayloadError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-payload",
                )));
            }
        }

        while self.buffer.len() < 2 {
            if self.fill_buffer().await? == 0 {
                return Err(PayloadError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-payload",
                )));
            }
        }
        let crlf_ok = &self.buffer[..2] == b"\r\n";
        self.buffer.advance(2);

        if !crlf_ok {
            return Err(PayloadError::BadChunk);
        }

        Ok(data.freeze())
    }

    pub async fn read_command(&mut self) -> io::Result<Option<Command>> {
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
                CommandHeader::Quit => {
                    tracing::debug!("quit");
                    return Ok(None);
                }
            };

            return Ok(Some(command));
        }
    }

    pub async fn write_response(&mut self, resp: &Response) -> io::Result<()> {
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
            Response::Version(version) => {
                self.writer.write_all(b"VERSION ").await?;
                self.writer.write_all(version.as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
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
            Response::Touched => self.writer.write_all(b"TOUCHED\r\n").await?,
            Response::Values(values) => {
                let mut itoa_buf = itoa::Buffer::new();
                let mut header_buf = BytesMut::with_capacity(256);

                for (key, flags, data, cas) in values {
                    header_buf.extend_from_slice(b"VALUE ");
                    header_buf.extend_from_slice(key);
                    header_buf.extend_from_slice(b" ");
                    header_buf.extend_from_slice(itoa_buf.format(*flags).as_bytes());
                    header_buf.extend_from_slice(b" ");
                    header_buf.extend_from_slice(itoa_buf.format(data.len()).as_bytes());

                    if let Some(c) = cas {
                        header_buf.extend_from_slice(b" ");
                        header_buf.extend_from_slice(itoa_buf.format(*c).as_bytes());
                    }
                    header_buf.extend_from_slice(b"\r\n");

                    self.writer.write_all(&header_buf).await?;
                    self.writer.write_all(data).await?;
                    self.writer.write_all(b"\r\n").await?;

                    header_buf.clear();
                }

                self.writer.write_all(b"END\r\n").await?;
            }
            Response::Stats(entries) => {
                for (name, value) in entries {
                    self.writer.write_all(b"STAT ").await?;
                    self.writer.write_all(name.as_bytes()).await?;
                    self.writer.write_all(b" ").await?;
                    self.writer.write_all(value.as_bytes()).await?;
                    self.writer.write_all(b"\r\n").await?;
                }
                self.writer.write_all(b"END\r\n").await?;
            }
        }

        Ok(())
    }

    async fn discard_exact(&mut self, mut n: usize) -> io::Result<()> {
        let from_buffer = self.buffer.len().min(n);
        self.buffer.advance(from_buffer);
        n -= from_buffer;

        if n > 0 {
            self.writer.flush().await?;
            let mut limited = (&mut self.reader).take(n as u64);
            tokio::io::copy(&mut limited, &mut tokio::io::sink()).await?;
        }

        Ok(())
    }

    async fn discard_until_newline(&mut self) -> io::Result<()> {
        loop {
            if let Some(pos) = memchr::memchr(b'\n', &self.buffer) {
                self.buffer.advance(pos + 1);
                return Ok(());
            }

            self.buffer.clear();
            if self.reader.read_buf(&mut self.buffer).await? == 0 {
                return Ok(()); // EOF
            }
        }
    }

    async fn fill_buffer(&mut self) -> io::Result<usize> {
        self.writer.flush().await?;
        self.buffer.reserve(4096);
        self.reader.read_buf(&mut self.buffer).await
    }
}

pub(crate) fn execute(cmd: Command, store: &Store) -> Response {
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
                        if entry.get().cas() != args.cas.unwrap() {
                            tracing::debug!(
                                expected = args.cas.unwrap(),
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
                    Stats::incr(hits, 1);
                    Response::Number(new_val)
                }
                _ => {
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
                Entry::Occupied(mut entry) if !entry.get().is_expired(now, oldest_live) => {
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
    use crate::{clock::Clock, commands::StoreArgs};
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf, duplex, split};

    const TEST_NOW: u64 = 1_000_000;
    const TEST_THREADS: usize = 1;

    /// Fresh, empty Store.
    fn empty_store() -> Store {
        let shared_clock = Clock::mock(TEST_NOW);
        Arc::new(StoreInner::new(shared_clock, TEST_THREADS))
    }

    /// Store pre-populated with one item under `key`.
    fn store_with(key: &str, item: Item) -> Store {
        let shared_clock = Clock::mock(TEST_NOW);
        let inner = StoreInner::new(shared_clock, TEST_THREADS);
        inner
            .items
            .insert(Bytes::copy_from_slice(key.as_bytes()), item);
        Arc::new(inner)
    }

    fn mock_item(data: Bytes, flags: u32, exptime: i64, cas: u64) -> Item {
        Item::new(data, flags, exptime, cas, TEST_NOW)
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
        let store = store_with("foo", mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
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
            mock_item(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
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
        let item = mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1);
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
        let item = mock_item(Bytes::copy_from_slice(b"hello"), 42, -1, 1);
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
        let store = store_with("foo", mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1));

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
        let store = store_with(
            "foo",
            mock_item(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
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
            "foo",
            mock_item(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
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
        let store = store_with(
            "foo",
            mock_item(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
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

        let item = store.items.get("foo".as_bytes()).unwrap();
        assert_eq!(item.data().as_ref(), b"jello");
        assert_eq!(item.flags(), 21);
    }

    #[test]
    fn delete_existing_key_returns_deleted_and_removes_it() {
        let store = store_with("foo", mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"10"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"10"), 42, 0, 1));
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
            mock_item(Bytes::from(u64::MAX.to_string()), 42, 0, 1),
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"10"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"10"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"0"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"0"), 42, 0, 1));
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
        let store = store_with("foo", mock_item(Bytes::from_static(b"0"), 42, 0, 1));
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

    #[test]
    fn get_hit_increments_cmd_get_and_get_hits() {
        let store = store_with("foo", mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
        let cmd = Command::Get {
            keys: vec!["foo".into()],
            with_cas: false,
        };

        execute(cmd, &store);

        assert_eq!(store.stats.cmd_get.load(Relaxed), 1);
        assert_eq!(store.stats.get_hits.load(Relaxed), 1);
        assert_eq!(store.stats.get_misses.load(Relaxed), 0);
    }

    #[test]
    fn get_miss_increments_get_misses_not_hits() {
        let store = empty_store();
        let cmd = Command::Get {
            keys: vec!["foo".into()],
            with_cas: false,
        };

        execute(cmd, &store);

        assert_eq!(store.stats.get_misses.load(Relaxed), 1);
        assert_eq!(store.stats.get_hits.load(Relaxed), 0);
    }

    #[test]
    fn get_expired_key_increments_get_expired_and_get_misses() {
        let store = store_with(
            "foo",
            mock_item(Bytes::copy_from_slice(b"hello"), 42, -1, 1),
        );
        let cmd = Command::Get {
            keys: vec!["foo".into()],
            with_cas: false,
        };

        execute(cmd, &store);

        assert_eq!(store.stats.get_expired.load(Relaxed), 1);
        assert_eq!(store.stats.get_misses.load(Relaxed), 1);
        assert_eq!(store.stats.get_hits.load(Relaxed), 0);
    }

    #[test]
    fn multi_key_get_counts_cmd_get_per_key_not_per_call() {
        let store = store_with("foo", mock_item(Bytes::copy_from_slice(b"hello"), 42, 0, 1));
        let cmd = Command::Get {
            keys: vec!["foo".into(), "bar".into(), "baz".into()],
            with_cas: false,
        };

        execute(cmd, &store);

        assert_eq!(store.stats.cmd_get.load(Relaxed), 3);
        assert_eq!(store.stats.get_hits.load(Relaxed), 1);
        assert_eq!(store.stats.get_misses.load(Relaxed), 2);
    }

    #[test]
    fn version_returns_cargo_package_version() {
        let cmd = Command::Version;

        let resp = execute(cmd, &empty_store());

        match resp {
            Response::Version(version) => assert_eq!(version, env!("CARGO_PKG_VERSION")),
            other => panic!("expected Version response, got {:?}", other),
        }
    }
}
