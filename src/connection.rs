use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use crate::{
    commands::{Command, Response},
    output::{MAX_BUFFERED, Output},
    parser::{CommandHeader, parse_command_line},
    stats,
};

const MAX_LINE_LEN: u64 = 8 * 1024; // bytes
const MAX_MULTIGET_LINE_LEN: u64 = 4 * 1024 * 1024; // bytes

/// `LineTooLong` and `BadChunk` are client errors, the connection should survive.
#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    LineTooLong,
    BadChunk,
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

pub struct Connection<R, W> {
    reader: R,
    writer: W,
    buffer: BytesMut,
    /// Responses not sent yet.
    output: Output,
    bytes_read: u64,
    bytes_written: u64,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Connection<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Connection {
            reader,
            writer,
            buffer: BytesMut::new(),
            output: Output::default(),
            bytes_read: 0,
            bytes_written: 0,
        }
    }

    /// Bytes read from the client on the current connection, added to `stats::BYTES_READ` too.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Bytes written to the client on the current connection, added to `stats::BYTES_WRITTEN` too.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn output(&mut self) -> &mut Output {
        &mut self.output
    }

    fn count_read(&mut self, n: usize) {
        self.bytes_read += n as u64;
        stats::BYTES_READ.add(n);
    }

    pub async fn read_line(&mut self) -> Result<Option<Bytes>, ReadError> {
        // Bytes already searched for `\n`, so we don't need to rescan here.
        let mut searched = 0;

        loop {
            if let Some(pos) = memchr::memchr(b'\n', &self.buffer[searched..]) {
                let mut line = self.buffer.split_to(searched + pos + 1).freeze();

                while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
                    line.truncate(line.len() - 1);
                }

                return Ok(Some(line));
            }
            searched = self.buffer.len();

            let len = self.buffer.len() as u64;
            if len >= MAX_LINE_LEN && (len >= MAX_MULTIGET_LINE_LEN || !is_multiget(&self.buffer)) {
                return Err(ReadError::LineTooLong);
            }

            if self.fill_buffer().await? == 0 {
                return Ok(None);
            }
        }
    }

    pub async fn read_payload(&mut self, len: usize) -> Result<Bytes, ReadError> {
        let mut data = BytesMut::with_capacity(len);

        let from_buffer = self.buffer.len().min(len);
        data.extend_from_slice(&self.buffer[..from_buffer]);
        self.buffer.advance(from_buffer);

        while data.len() < len {
            self.flush().await?;
            let n = self.reader.read_buf(&mut data).await?;
            self.count_read(n);
            if n == 0 {
                return Err(ReadError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-payload",
                )));
            }
        }

        while self.buffer.len() < 2 {
            if self.fill_buffer().await? == 0 {
                return Err(ReadError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-payload",
                )));
            }
        }
        let crlf_ok = &self.buffer[..2] == b"\r\n";
        self.buffer.advance(2);

        if !crlf_ok {
            return Err(ReadError::BadChunk);
        }

        Ok(data.freeze())
    }

    pub async fn read_command(&mut self) -> io::Result<Option<Command>> {
        loop {
            if self.output.len() >= MAX_BUFFERED {
                Box::pin(self.flush()).await?;
            }

            let line = match self.read_line().await {
                Ok(Some(line)) => line,
                Ok(None) => return Ok(None),
                Err(e) => {
                    self.recover(e).await?;
                    continue;
                }
            };

            let header = match parse_command_line(&line) {
                Ok(header) => header,
                Err(err) => {
                    if let Some(n) = err.discard() {
                        self.discard_exact(n).await?;
                    }

                    self.output.response(&err.response());
                    continue;
                }
            };

            let command = match header {
                CommandHeader::Immediate(cmd) => cmd,
                CommandHeader::Store(pending) => match self.read_payload(pending.len).await {
                    Ok(data) => pending.into_command(data),
                    Err(e) => {
                        self.recover(e).await?;
                        continue;
                    }
                },
                CommandHeader::TooLarge(pending) => {
                    self.discard_exact(pending.len + 2).await?;
                    pending.into_too_large()
                }
                CommandHeader::Quit => {
                    tracing::debug!("quit");
                    // Quit never reaches fill_buffer so the response is flushed here
                    self.flush().await?;
                    return Ok(None);
                }
            };

            return Ok(Some(command));
        }
    }

    async fn recover(&mut self, err: ReadError) -> io::Result<()> {
        let msg = match err {
            ReadError::Io(e) => return Err(e),
            ReadError::LineTooLong => {
                self.discard_until_newline().await?;
                "line too long"
            }
            ReadError::BadChunk => "bad data chunk",
        };
        self.output.response(&Response::ClientError(msg));
        Ok(())
    }

    async fn discard_exact(&mut self, mut n: usize) -> io::Result<()> {
        let from_buffer = self.buffer.len().min(n);
        self.buffer.advance(from_buffer);
        n -= from_buffer;

        if n > 0 {
            self.flush().await?;
            let mut limited = (&mut self.reader).take(n as u64);
            let n = tokio::io::copy(&mut limited, &mut tokio::io::sink()).await?;
            self.count_read(n as usize);
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
            let n = self.reader.read_buf(&mut self.buffer).await?;
            self.count_read(n);
            if n == 0 {
                return Ok(()); // EOF
            }
        }
    }

    async fn fill_buffer(&mut self) -> io::Result<usize> {
        self.flush().await?;
        self.buffer.reserve(4096);
        let n = self.reader.read_buf(&mut self.buffer).await?;
        self.count_read(n);
        Ok(n)
    }

    /// Sends the queued responses.
    async fn flush(&mut self) -> io::Result<()> {
        let n = self.output.write_to(&mut self.writer).await?;
        self.bytes_written += n as u64;
        stats::BYTES_WRITTEN.add(n);
        Ok(())
    }
}

/// Whether a partial line is a `get`/`gets`, only lines allowed to surpass `MAX_LINE_LEN`.
fn is_multiget(line: &[u8]) -> bool {
    let start = line.iter().position(|&b| b != b' ').unwrap_or(line.len());
    let line = &line[start..];
    line.starts_with(b"get ") || line.starts_with(b"gets ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Value;
    use contatori::counters::Observable;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn quit_flushes_responses_to_pipelined_commands() {
        let (mut client, server) = tokio::io::duplex(64);
        let (r, w) = tokio::io::split(server);
        let mut conn = Connection::new(r, w);

        client.write_all(b"version\r\nquit\r\n").await.unwrap();
        let cmd = conn.read_command().await.unwrap();
        assert!(matches!(cmd, Some(Command::Version)));
        conn.output().response(&Response::Version("1.0"));
        assert!(conn.read_command().await.unwrap().is_none());
        drop(conn);

        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"VERSION 1.0\r\n");
    }

    #[tokio::test]
    async fn counts_bytes_read_and_written() {
        let (mut client, server) = tokio::io::duplex(64);
        let (r, w) = tokio::io::split(server);
        let mut conn = Connection::new(r, w);
        let read_before = stats::BYTES_READ.value().as_u64();
        let written_before = stats::BYTES_WRITTEN.value().as_u64();

        client.write_all(b"version\r\n").await.unwrap();
        let cmd = conn.read_command().await.unwrap();
        assert!(matches!(cmd, Some(Command::Version)));
        conn.output().response(&Response::Version("1.0"));
        conn.flush().await.unwrap();

        assert!(stats::BYTES_READ.value().as_u64() - read_before >= 9);
        assert!(stats::BYTES_WRITTEN.value().as_u64() - written_before >= 13);

        // Per-connection counters
        assert_eq!(conn.bytes_read(), 9);
        assert_eq!(conn.bytes_written(), b"VERSION 1.0\r\n".len() as u64);
    }

    #[tokio::test]
    async fn too_large_store_discards_its_data_block() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let (r, w) = tokio::io::split(server);
        let mut conn = Connection::new(r, w);
        let len = crate::parser::MAX_ITEM_SIZE + 1;

        let writer = tokio::spawn(async move {
            let mut input = format!("set foo 0 0 {len} noreply\r\n").into_bytes();
            input.extend(std::iter::repeat_n(b'x', len));
            input.extend_from_slice(b"\r\nversion\r\n");
            client.write_all(&input).await.unwrap();
            client
        });

        match conn.read_command().await.unwrap() {
            Some(Command::StoreTooLarge { op, key, noreply }) => {
                assert_eq!(op, crate::commands::StoreOp::Set);
                assert_eq!(key, Bytes::from_static(b"foo"));
                assert!(noreply);
            }
            other => panic!("expected StoreTooLarge, got {other:?}"),
        }
        assert!(matches!(
            conn.read_command().await.unwrap(),
            Some(Command::Version)
        ));
        writer.await.unwrap();
    }

    fn connection_with_input(
        input: Vec<u8>,
    ) -> Connection<
        tokio::io::ReadHalf<tokio::io::DuplexStream>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
    > {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            client.write_all(&input).await.unwrap();
            // Keep the client open until the server side is dropped
            let mut sink = Vec::new();
            let _ = client.read_to_end(&mut sink).await;
        });
        let (r, w) = tokio::io::split(server);
        Connection::new(r, w)
    }

    #[tokio::test]
    async fn read_command_flushes_once_max_buffered_is_queued_without_blocking() {
        let mut conn = connection_with_input(b"version\r\nversion\r\n".to_vec());
        assert!(matches!(
            conn.read_command().await.unwrap(),
            Some(Command::Version)
        ));
        assert_eq!(conn.bytes_written(), 0);

        let data = Bytes::from(vec![b'x'; 16 * 1024]);
        let out = conn.output();
        for _ in 0..MAX_BUFFERED / data.len() + 1 {
            out.value(Value {
                key: b"k",
                flags: 0,
                data: &data,
                cas: None,
            });
        }
        assert!(out.len() >= MAX_BUFFERED);

        // The next line is already buffered
        assert!(matches!(
            conn.read_command().await.unwrap(),
            Some(Command::Version)
        ));
        assert!(conn.bytes_written() >= MAX_BUFFERED as u64);
        assert!(conn.output().is_empty());
    }

    fn multiget_line(cmd: &str, keys: usize) -> Vec<u8> {
        let mut line = cmd.to_string();
        for i in 0..keys {
            line.push_str(&format!(" key{i:07}")); // 11 bytes per key
        }
        line.push_str("\r\nversion\r\n");
        line.into_bytes()
    }

    #[tokio::test]
    async fn get_and_gets_lines_longer_than_max_line_len_are_accepted() {
        for (cmd, cas) in [("get", false), ("gets", true)] {
            let input = multiget_line(cmd, 3000);
            assert!(input.len() as u64 > MAX_LINE_LEN);
            let mut conn = connection_with_input(input);

            match conn.read_command().await.unwrap() {
                Some(Command::Get { keys, with_cas }) => {
                    assert_eq!(keys.len(), 3000);
                    assert_eq!(keys[2999], Bytes::from_static(b"key0002999"));
                    assert_eq!(with_cas, cas);
                }
                other => panic!("expected Get, got {other:?}"),
            }
            assert!(matches!(
                conn.read_command().await.unwrap(),
                Some(Command::Version)
            ));
        }
    }

    #[tokio::test]
    async fn other_lines_longer_than_max_line_len_are_rejected() {
        let mut conn = connection_with_input(multiget_line("gat 0", 3000));

        assert!(matches!(
            conn.read_command().await.unwrap(),
            Some(Command::Version)
        ));
        conn.flush().await.unwrap();
        assert_eq!(
            conn.bytes_written(),
            b"CLIENT_ERROR line too long\r\n".len() as u64
        );
    }

    #[tokio::test]
    async fn get_lines_longer_than_max_multiget_line_len_are_rejected() {
        let keys = MAX_MULTIGET_LINE_LEN as usize / 11 + 1;
        let mut conn = connection_with_input(multiget_line("get", keys));

        assert!(matches!(
            conn.read_command().await.unwrap(),
            Some(Command::Version)
        ));
        conn.flush().await.unwrap();
        assert_eq!(
            conn.bytes_written(),
            b"CLIENT_ERROR line too long\r\n".len() as u64
        );
    }
}
