use std::{collections::hash_map::Entry, io, time::SystemTime};

use bytes::BytesMut;
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
    line: String,
}

const MAX_ITEM_SIZE: usize = 1024 * 1024; // 1 MiB

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Connection<R, W> {
    fn new(reader: R, writer: W) -> Self {
        Connection {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            line: String::new(),
        }
    }

    async fn read_command(&mut self) -> io::Result<Option<Command>> {
        loop {
            self.line.clear();
            let bytes_read = self.reader.read_line(&mut self.line).await?;
            if bytes_read == 0 {
                return Ok(None);
            }

            let parts: Vec<&str> = self.line.split_whitespace().collect();
            match parts.as_slice() {
                ["get", keys @ ..] if !keys.is_empty() => {
                    return Ok(Some(Command::Get {
                        keys: keys.iter().map(|s| s.to_string()).collect(),
                    }));
                }
                [
                    op @ ("add" | "set" | "replace" | "append" | "prepend"),
                    key,
                    flags,
                    exptime,
                    bytes_len,
                    rest @ ..,
                ] => {
                    let store_op = match *op {
                        "add" => StoreOp::Add,
                        "replace" => StoreOp::Replace,
                        "set" => StoreOp::Set,
                        _ => unreachable!(),
                    };

                    let Ok(bytes_len) = bytes_len.parse::<usize>() else {
                        self.write_response(&Response::Error).await?;
                        return Ok(None);
                    };

                    let (Ok(flags), Ok(exptime)) = (flags.parse(), exptime.parse()) else {
                        self.discard_exact(bytes_len + 2).await?;
                        self.write_response(&Response::Error).await?;
                        continue;
                    };

                    if bytes_len > MAX_ITEM_SIZE {
                        self.discard_exact(bytes_len + 2).await?;
                        self.write_response(&Response::Error).await?;
                        continue;
                    }

                    let mut data = BytesMut::zeroed(bytes_len);
                    self.reader.read_exact(&mut data).await?;
                    let data = data.freeze();

                    let mut crlf = [0u8; 2];
                    self.reader.read_exact(&mut crlf).await?;
                    if crlf != *b"\r\n" {
                        self.write_response(&Response::Error).await?;
                        continue;
                    }

                    let noreply = match rest {
                        [] => false,
                        ["noreply"] => true,
                        _ => {
                            // Note: If parsing fails before payload bytes are read, respond with error and continue.
                            self.write_response(&Response::Error).await?;
                            continue;
                        }
                    };

                    return Ok(Some(Command::Store(
                        store_op,
                        StoreArgs {
                            key: key.to_string(),
                            flags,
                            exptime,
                            data,
                            noreply,
                        },
                    )));
                }
                ["delete", key, rest @ ..] => {
                    let noreply = match rest {
                        [] => false,
                        ["noreply"] => true,
                        _ => {
                            self.write_response(&Response::Error).await?;
                            continue;
                        }
                    };

                    return Ok(Some(Command::Delete {
                        key: key.to_string(),
                        noreply,
                    }));
                }
                _ => {
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
            Response::Error => self.writer.write_all(b"ERROR\r\n").await?,
            Response::Values(values) => {
                for (key, flags, data) in values {
                    let header = format!("VALUE {key} {flags} {}\r\n", data.len());
                    self.writer.write_all(header.as_bytes()).await?;
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
}

pub(crate) fn execute(cmd: Command, store: &Store) -> Response {
    match cmd {
        Command::Get { keys } => {
            let now = SystemTime::now();
            let mut expired_keys = Vec::new();
            let mut values = Vec::new();

            {
                let store = store.read().unwrap();
                for key in keys {
                    match store.get(&key) {
                        Some(item) if item.is_expired(now) => expired_keys.push(key),
                        Some(item) => values.push((key, item.flags(), item.data().clone())),
                        _ => {}
                    }
                }
            }

            if !expired_keys.is_empty() {
                let mut store = store.write().unwrap();
                for key in expired_keys {
                    if let Entry::Occupied(entry) = store.entry(key)
                        && entry.get().is_expired(now)
                    {
                        entry.remove();
                    }
                }
            }

            Response::Values(values)
        }
        Command::Store(op, args) => {
            let now = SystemTime::now();
            let mut store = store.write().unwrap();

            match op {
                StoreOp::Add => match store.entry(args.key) {
                    Entry::Occupied(entry) if !entry.get().is_expired(now) => Response::NotStored,
                    Entry::Occupied(mut entry) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime));
                        Response::Stored
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime));
                        Response::Stored
                    }
                },
                StoreOp::Set => {
                    let item = Item::new(args.data, args.flags, args.exptime);
                    store.insert(args.key, item);
                    Response::Stored
                }
                StoreOp::Replace => match store.entry(args.key) {
                    Entry::Occupied(mut entry) if !entry.get().is_expired(now) => {
                        entry.insert(Item::new(args.data, args.flags, args.exptime));
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
                StoreOp::Append | StoreOp::Prepend => match store.entry(args.key) {
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

                        let item =
                            Item::with_parts(new_data, old_item.flags(), old_item.expires_at());
                        entry.insert(item);
                        Response::Stored
                    }
                    _ => Response::NotStored,
                },
            }
        }
        Command::Delete { key, noreply: _ } => {
            let mut store = store.write().unwrap();
            match store.remove(&key) {
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
    use core::panic;
    use std::vec;
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf, duplex, split};

    fn setup(
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
    async fn get_single_key() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"get foo\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["foo"]),
            other => panic!("expected Get, got {:?}", other.is_some()),
        }
    }

    #[tokio::test]
    async fn get_multiple_keys() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"get foo bar baz\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["foo", "bar", "baz"]),
            _ => panic!("expected Get"),
        }
    }

    #[tokio::test]
    async fn get_with_no_keys_errors_and_resyncs() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"get\r\n").await.unwrap();
        client.write_all(b"get foo\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["foo"]),
            other => panic!(
                "expected the second line to parse as Get, got {:?}",
                other.is_some()
            ),
        }

        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ERROR\r\n");
    }

    #[tokio::test]
    async fn set_valid_command() {
        let (mut conn, mut client) = setup(1024);
        client
            .write_all(b"set foo 42 3600 5\r\nhello\r\n")
            .await
            .unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Store(StoreOp::Set, args)) => {
                assert_eq!(args.key, "foo");
                assert_eq!(args.flags, 42);
                assert_eq!(args.exptime, 3600);
                assert_eq!(&args.data[..], b"hello");
                assert!(!args.noreply);
            }
            _ => panic!("Expected Store(Set)"),
        }
    }

    #[tokio::test]
    async fn set_with_noreply() {
        let (mut conn, mut client) = setup(1024);
        client
            .write_all(b"set foo 0 0 5 noreply\r\nhello\r\n")
            .await
            .unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Store(StoreOp::Set, args)) => assert!(args.noreply),
            _ => panic!("Expected Set with noreply"),
        }
    }

    #[tokio::test]
    async fn set_invalid_length_closes_connection() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"set foo 0 0 bad\r\n").await.unwrap();

        let cmd = conn.read_command().await.unwrap();
        assert!(cmd.is_none(), "Connection should close on desync");

        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ERROR\r\n");
    }

    #[tokio::test]
    async fn set_invalid_flags_discards_data_and_resyncs() {
        let (mut conn, mut client) = setup(1024);

        client
            .write_all(b"set foo bad 0 5\r\nhello\r\n")
            .await
            .unwrap();
        client.write_all(b"get foo\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["foo"]),
            _ => panic!("Expected Get command after resync"),
        }

        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ERROR\r\n");
    }

    #[tokio::test]
    async fn set_missing_trailing_crlf() {
        let (mut conn, mut client) = setup(1024);

        client
            .write_all(b"set foo 0 0 5\r\nhello\n\n")
            .await
            .unwrap();
        client.write_all(b"get foo\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { .. }) => {}
            _ => panic!("Expected Get command after CRLF error recovery"),
        }
    }

    #[tokio::test]
    async fn set_trailing_garbage_errors_and_resyncs() {
        let (mut conn, mut client) = setup(1024);
        client
            .write_all(b"set foo 0 0 5 garbage\r\nhello\r\n")
            .await
            .unwrap();
        client.write_all(b"get bar\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["bar"]),
            other => panic!("stream desynced, got {:?}", other.is_some()),
        }

        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ERROR\r\n");
    }

    #[tokio::test]
    async fn set_exceeds_max_item_size_discards_and_resyncs() {
        let (mut conn, mut client) = setup(1024);
        let payload_size = 1024 * 1024 + 1; // 1 MiB + 1 byte

        let client_taks = tokio::spawn(async move {
            let header = format!("set foo 0 0 {}\r\n", payload_size);
            client.write_all(header.as_bytes()).await.unwrap();
            client.write_all(&vec![b'A'; payload_size]).await.unwrap();
            client.write_all(b"\r\n").await.unwrap();

            client.write_all(b"get bar\r\n").await.unwrap();

            let mut buf = [0u8; 32];
            let n = client.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"ERROR\r\n");
        });

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["bar"]),
            _ => panic!("Expected Get"),
        }

        client_taks.await.unwrap(); // propagate panic
    }

    #[tokio::test]
    async fn delete_valid_command() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"delete foo\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Delete { key, .. }) => assert_eq!(key, "foo"),
            other => panic!("expected Delete, got {:?}", other.is_some()),
        }
    }

    #[tokio::test]
    async fn delete_with_noreply() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"delete foo noreply\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Delete { noreply, .. }) => assert!(noreply),
            _ => panic!("Expected Delete with noreply"),
        }
    }

    #[tokio::test]
    async fn delete_trailing_garbage_errors_and_resyncs() {
        let (mut conn, mut client) = setup(1024);
        client.write_all(b"delete foo garbage\r\n").await.unwrap();
        client.write_all(b"get bar\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["bar"]),
            other => panic!("stream desynced, got {:?}", other.is_some()),
        }

        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ERROR\r\n");
    }

    #[tokio::test]
    async fn unknown_command_resyncs() {
        let (mut conn, mut client) = setup(1024);

        client.write_all(b"bar\r\n").await.unwrap();
        client.write_all(b"get foo\r\n").await.unwrap();

        match conn.read_command().await.unwrap() {
            Some(Command::Get { keys }) => assert_eq!(keys, vec!["foo"]),
            _ => panic!("Expected Get"),
        }

        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ERROR\r\n");
    }

    #[tokio::test]
    async fn clean_client_disconnect() {
        let (mut conn, client) = setup(1024);
        drop(client);

        let cmd = conn.read_command().await.unwrap();
        assert!(cmd.is_none(), "Should gracefully return None on EOF");
    }
}
