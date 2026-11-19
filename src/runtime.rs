use std::io;

use tokio::runtime::{Builder, Runtime};

pub fn build(threads: usize) -> io::Result<Runtime> {
    if threads > 1 {
        Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
    } else {
        Builder::new_current_thread().enable_all().build()
    }
}
