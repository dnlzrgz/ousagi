use std::io;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::{
    commands::{Command, Response},
    store::Store,
};

pub(crate) struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    line: String,
}

impl Connection {
    fn new(socket: TcpStream) -> Self {
        let (r, w) = socket.into_split();
        Connection {
            reader: BufReader::new(r),
            writer: BufWriter::new(w),
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
}

pub(crate) fn execute(cmd: Command, store: &Store) -> Response {
    match cmd {
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
        _ => unimplemented!(),
    }
}

pub async fn process(socket: TcpStream, store: Store) -> io::Result<()> {
    let mut conn = Connection::new(socket);

    while let Some(cmd) = conn.read_command().await? {
        let noreply = cmd.noreply();
        let resp = execute(cmd, &store);
        if !noreply {
            conn.write_response(&resp).await?;
        }
    }

    Ok(())
}
