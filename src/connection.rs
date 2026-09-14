use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::{
    commands::{Command, Response},
    parser::{CommandHeader, parse_command_line},
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
