use std::io;

use tokio::{
    io::{BufReader, BufWriter},
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
        unimplemented!()
    }

    async fn write_response(&mut self, response: &Response) -> io::Result<()> {
        unimplemented!()
    }
}

pub(crate) fn execute(cmd: Command, store: &Store) -> Response {
    unimplemented!()
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
