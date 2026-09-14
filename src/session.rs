use std::io;

use tokio::net::TcpStream;

use crate::{connection::Connection, handler, store::Store};

pub async fn process(mut socket: TcpStream, store: Store) -> io::Result<()> {
    let (r, w) = socket.split();
    let mut conn = Connection::new(r, w);

    while let Some(cmd) = conn.read_command().await? {
        let noreply = cmd.noreply();
        let resp = handler::handle(cmd, &store);
        if !noreply {
            conn.write_response(&resp).await?;
        }
    }

    Ok(())
}
