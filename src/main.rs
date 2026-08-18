use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use ousagi::{connection, store::Store};
use tokio::net::TcpListener;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:11211").await.unwrap();
    println!("listening on 127.0.0.1:11211");

    let store: Store = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("accepted connection from {addr}");

        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = connection::process(socket, store).await {
                eprint!("connection {addr} err: {e}");
            } else {
                println!("connection {addr} closed");
            }
        });
    }
}
