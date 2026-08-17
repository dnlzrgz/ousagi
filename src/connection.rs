use std::io;

use bytes::BytesMut;
use tokio::{
    io::{
        AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
    },
    net::TcpStream,
};

use crate::{
    commands::{Command, Response},
    store::{Item, Store},
};

pub(crate) struct Connection<R, W> {
    reader: BufReader<R>,
    writer: BufWriter<W>,
    line: String,
}

/// Default max item size.
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
                ["set", key, flags, exptime, bytes_len, rest @ ..] => {
                    let Ok(bytes_len) = bytes_len.parse::<usize>() else {
                        self.write_response(&Response::Error).await?;
                        return Ok(None); // desync, must close
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

                    let noreply = matches!(rest, ["noreply"]);

                    let mut data = BytesMut::zeroed(bytes_len);
                    self.reader.read_exact(&mut data).await?;
                    let data = data.freeze();

                    let mut crlf = [0u8; 2];
                    self.reader.read_exact(&mut crlf).await?;
                    if crlf != *b"\r\n" {
                        self.write_response(&Response::Error).await?;
                        continue;
                    }

                    return Ok(Some(Command::Set {
                        key: key.to_string(),
                        flags,
                        exptime,
                        data,
                        noreply,
                    }));
                }
                ["delete", key, rest @ ..] => {
                    let noreply = matches!(rest, ["noreply"]);
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
        Command::Set {
            key,
            flags,
            exptime: _, // TODO:
            data,
            noreply: _, // TODO:
        } => {
            let mut store = store.lock().unwrap();
            let item = Item::new(data, flags);
            store.insert(key, item);
            Response::Stored
        }
        Command::Get { keys } => {
            let store = store.lock().unwrap();
            let values = keys
                .into_iter()
                .filter_map(|key| {
                    store
                        .get(&key)
                        .map(|item| (key, item.flags(), item.data().clone()))
                })
                .collect();

            Response::Values(values)
        }
        Command::Delete { key, noreply: _ } => {
            let mut store = store.lock().unwrap();
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
