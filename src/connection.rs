use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::{
    commands::{Command, Response},
    parser::{CommandHeader, parse_command_line},
    stats,
};

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
            let n = self.reader.read_buf(&mut data).await?;
            stats::BYTES_READ.add(n);
            if n == 0 {
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
                    // Quit never reaches fill_buffe so the response is flushed here
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

    async fn discard_exact(&mut self, mut n: usize) -> io::Result<()> {
        let from_buffer = self.buffer.len().min(n);
        self.buffer.advance(from_buffer);
        n -= from_buffer;

        if n > 0 {
            self.writer.flush().await?;
            let mut limited = (&mut self.reader).take(n as u64);
            let n = tokio::io::copy(&mut limited, &mut tokio::io::sink()).await?;
            stats::BYTES_READ.add(n as usize);
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
            stats::BYTES_READ.add(n);
            if n == 0 {
                return Ok(()); // EOF
            }
        }
    }

    async fn fill_buffer(&mut self) -> io::Result<usize> {
        self.writer.flush().await?;
        self.buffer.reserve(4096);
        let n = self.reader.read_buf(&mut self.buffer).await?;
        stats::BYTES_READ.add(n);
        Ok(n)
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        stats::BYTES_WRITTEN.add(buf.len());
        self.writer.write_all(buf).await
    }
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

        assert_eq!(stats::BYTES_READ.value().as_u64() - read_before, 9);
        assert_eq!(
            stats::BYTES_WRITTEN.value().as_u64() - written_before,
            b"VERSION 1.0\r\n".len() as u64
        );
    }
}
