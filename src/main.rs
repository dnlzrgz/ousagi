use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

/// Item-size limit.
const MAX_ITEM_SIZE: usize = 1024 * 1024; // 1 Mb

/// A value stored in the cache.
///
/// `data` is kept as raw bytes rather than a `String` because memcached values are arbitrary byte
/// sequences. Therefore the server should not assume that cached data is UTF-8.
///
/// `flags` is a opaque metadata field supplied by the client. Its stored and returned unchanged.
struct Item {
    data: Vec<u8>,
    flags: u32,
}

/// Shared handled to the cache.
type Store = Arc<Mutex<HashMap<String, Item>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:11211").await.unwrap();
    println!("listening on 127.0.0.1:11211");

    let store: Store = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("accepted connection from {addr}");

        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = process(socket, store).await {
                eprint!("connection {addr} err: {e}");
            } else {
                println!("connection {addr} closed");
            }
        });
    }
}

/// Process a request.
async fn process(socket: TcpStream, store: Store) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();

        match parts.as_slice() {
            ["set", key, flags, _exptime, bytes_len] => {
                let flags: u32 = match flags.parse() {
                    Ok(flags) => flags,
                    Err(_) => {
                        writer
                            .write_all(b"CLIENT_ERROR bad command line format\r\n")
                            .await?;
                        continue;
                    }
                };

                let len: usize = match bytes_len.parse() {
                    Ok(n) => n,
                    Err(_) => {
                        writer
                            .write_all(b"CLIENT_ERROR bad command line format\r\n")
                            .await?;
                        continue;
                    }
                };

                if len > MAX_ITEM_SIZE {
                    writer
                        .write_all(b"SERVER_ERROR object too large for cache\r\n")
                        .await?;
                    continue;
                }

                let mut data = vec![0u8; len];
                reader.read_exact(&mut data).await?;

                // `read_exact` only consume the value itself, so we need to consume the reamining bytes.
                let mut delimiter = [0u8; 2];
                reader.read_exact(&mut delimiter).await?;
                if delimiter != *b"\r\n" {
                    writer.write_all(b"CLIENT_ERROR bad data chunk\r\n").await?;
                    continue;
                }

                store
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), Item { data, flags });

                writer.write_all(b"STORED\r\n").await?;
            }
            ["get", key] => {
                let found = store
                    .lock()
                    .unwrap()
                    .get(*key)
                    .map(|item| (item.data.clone(), item.flags));

                if let Some((data, flags)) = found {
                    let header = format!("VALUE {key} {flags} {}\r\n", data.len());
                    writer.write_all(header.as_bytes()).await?;
                    writer.write_all(&data).await?;
                    writer.write_all(b"\r\n").await?;
                }
                writer.write_all(b"END\r\n").await?;
            }
            _ => {
                writer.write_all(b"ERROR\r\n").await?;
            }
        }
    }

    Ok(())
}
