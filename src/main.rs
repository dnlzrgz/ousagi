use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:11211").await.unwrap();
    println!("listening on 127.0.0.1:11211");

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("accepted connection from {addr}");

        tokio::spawn(async move {
            process(socket).await.unwrap();
        });
    }
}

async fn process(socket: TcpStream) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            // Client closed the connection.
            break;
        }

        writer.write_all(line.as_bytes()).await?;
    }

    Ok(())
}
