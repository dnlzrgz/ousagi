use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::{
    commands::{Command, Response},
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
    writer: BufWriter<W>,
    buffer: BytesMut,
    bytes_read: u64,
    bytes_written: u64,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Connection<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Connection {
            reader,
            writer: BufWriter::new(writer),
            buffer: BytesMut::new(),
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
            self.writer.flush().await?;
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

                    self.write_response(&err.response()).await?;
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
                    self.writer.flush().await?;
                    return Ok(None);
                }
            };

            return Ok(Some(command));
        }
    }

    pub async fn write_response(&mut self, resp: &Response) -> io::Result<()> {
        match resp {
            Response::Stored => self.write(b"STORED\r\n").await?,
            Response::NotStored => self.write(b"NOT_STORED\r\n").await?,
            Response::Deleted => self.write(b"DELETED\r\n").await?,
            Response::NotFound => self.write(b"NOT_FOUND\r\n").await?,
            Response::Exists => self.write(b"EXISTS\r\n").await?,
            Response::Number(val) => {
                let mut buf = itoa::Buffer::new();
                self.write(buf.format(*val).as_bytes()).await?;
                self.write(b"\r\n").await?;
            }
            Response::Error => self.write(b"ERROR\r\n").await?,
            Response::Ok => self.write(b"OK\r\n").await?,
            Response::Version(version) => {
                self.write(b"VERSION ").await?;
                self.write(version.as_bytes()).await?;
                self.write(b"\r\n").await?;
            }
            Response::ClientError(msg) => {
                self.write(b"CLIENT_ERROR ").await?;
                self.write(msg.as_bytes()).await?;
                self.write(b"\r\n").await?;
            }
            Response::ServerError(msg) => {
                self.write(b"SERVER_ERROR ").await?;
                self.write(msg.as_bytes()).await?;
                self.write(b"\r\n").await?;
            }
            Response::Touched => self.write(b"TOUCHED\r\n").await?,
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

                    self.write(&header_buf).await?;
                    self.write(data).await?;
                    self.write(b"\r\n").await?;

                    header_buf.clear();
                }

                self.write(b"END\r\n").await?;
            }
            Response::Stats(entries) => {
                for (name, value) in entries {
                    self.write(b"STAT ").await?;
                    self.write(name.as_bytes()).await?;
                    self.write(b" ").await?;
                    self.write(value.as_bytes()).await?;
                    self.write(b"\r\n").await?;
                }
                self.write(b"END\r\n").await?;
            }
        }

        Ok(())
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
        self.write_response(&Response::ClientError(msg)).await
    }

    async fn discard_exact(&mut self, mut n: usize) -> io::Result<()> {
        let from_buffer = self.buffer.len().min(n);
        self.buffer.advance(from_buffer);
        n -= from_buffer;

        if n > 0 {
            self.writer.flush().await?;
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
        self.writer.flush().await?;
        self.buffer.reserve(4096);
        let n = self.reader.read_buf(&mut self.buffer).await?;
        self.count_read(n);
        Ok(n)
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        self.bytes_written += buf.len() as u64;
        stats::BYTES_WRITTEN.add(buf.len());
        self.writer.write_all(buf).await
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
    use contatori::counters::Observable;

    #[tokio::test]
    async fn quit_flushes_responses_to_pipelined_commands() {
        let (mut client, server) = tokio::io::duplex(64);
        let (r, w) = tokio::io::split(server);
        let mut conn = Connection::new(r, w);

        client.write_all(b"version\r\nquit\r\n").await.unwrap();
        let cmd = conn.read_command().await.unwrap();
        assert!(matches!(cmd, Some(Command::Version)));
        conn.write_response(&Response::Version("1.0"))
            .await
            .unwrap();
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
        conn.write_response(&Response::Version("1.0"))
            .await
            .unwrap();

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
        assert_eq!(
            conn.bytes_written(),
            b"CLIENT_ERROR line too long\r\n".len() as u64
        );
    }
}
