use std::io;

use tokio::net::TcpStream;

use crate::{connection::Connection, handler, store::Store};

pub async fn process(mut socket: TcpStream, store: Store) -> io::Result<()> {
    let (r, w) = socket.split();
    let mut conn = Connection::new(r, w);

    while let Some(cmd) = conn.read_command().await? {
        handler::handle(cmd, &store, conn.output());
    }

    Ok(())
}
