use std::{
    collections::VecDeque,
    io::{self, IoSlice},
};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    commands::{Response, Value},
    stats::StatValue,
};

/// Queued bytes past which values are queued by reference instead of copied,
/// and `Connection` sends the queue before reading the next command.
pub(crate) const MAX_BUFFERED: usize = 64 * 1024;

/// Responses encoded but not yet sent to the client.
///
/// Encoding is synchronous; `Connection` sends everything with `write_to`
/// before it blocks on a read. Small values are copied so a batch of responses
/// usually goes out in one write. A value that doesn't fit in `MAX_BUFFERED`
/// is queued as its own `Bytes` chunk instead, so memory doesn't grow with the
/// number of keys times their size.
#[derive(Default)]
pub struct Output {
    /// Sent before `buf`, in order: frozen parts of `buf` and values queued by reference.
    chunks: Vec<Bytes>,
    chunks_len: usize,
    buf: BytesMut,
    /// Bytes appended to `buf` since the last `write_to` that were already frozen
    /// into `chunks`. With `buf.len()` this bounds how much is copied per send.
    copied: usize,
}

impl Output {
    /// Bytes queued.
    pub fn len(&self) -> usize {
        self.chunks_len + self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn response(&mut self, resp: &Response) {
        match resp {
            Response::Stored => self.buf.extend_from_slice(b"STORED\r\n"),
            Response::NotStored => self.buf.extend_from_slice(b"NOT_STORED\r\n"),
            Response::Deleted => self.buf.extend_from_slice(b"DELETED\r\n"),
            Response::NotFound => self.buf.extend_from_slice(b"NOT_FOUND\r\n"),
            Response::Exists => self.buf.extend_from_slice(b"EXISTS\r\n"),
            Response::Touched => self.buf.extend_from_slice(b"TOUCHED\r\n"),
            Response::End => self.buf.extend_from_slice(b"END\r\n"),
            Response::Number(val) => {
                self.number(*val);
                self.buf.extend_from_slice(b"\r\n");
            }
            Response::Ok => self.buf.extend_from_slice(b"OK\r\n"),
            Response::Version(version) => self.line(b"VERSION ", version),
            Response::Error => self.buf.extend_from_slice(b"ERROR\r\n"),
            Response::ClientError(msg) => self.line(b"CLIENT_ERROR ", msg),
            Response::ServerError(msg) => self.line(b"SERVER_ERROR ", msg),
        }
    }

    /// `VALUE <key> <flags> <bytes> [<cas>]` and the data block. Callers end the list with `Response::End`.
    pub fn value(&mut self, value: Value<'_>) {
        self.buf.extend_from_slice(b"VALUE ");
        self.buf.extend_from_slice(value.key);
        self.buf.extend_from_slice(b" ");
        self.number(value.flags.into());
        self.buf.extend_from_slice(b" ");
        self.number(value.data.len() as u64);
        if let Some(cas) = value.cas {
            self.buf.extend_from_slice(b" ");
            self.number(cas);
        }
        self.buf.extend_from_slice(b"\r\n");

        // Bound what's copied per send, not what's queued: after a large value
        // goes by reference, small ones are copied again.
        if self.copied + self.buf.len() + value.data.len() <= MAX_BUFFERED {
            self.buf.extend_from_slice(value.data);
        } else {
            let frozen = self.buf.split().freeze();
            self.copied += frozen.len();
            self.push_chunk(frozen);
            self.push_chunk(value.data.clone());
        }
        self.buf.extend_from_slice(b"\r\n");
    }

    /// `STAT <name> <value>`. Callers end the list with `Response::End`.
    pub fn stat(&mut self, name: &str, value: StatValue) {
        self.buf.extend_from_slice(b"STAT ");
        self.buf.extend_from_slice(name.as_bytes());
        self.buf.extend_from_slice(b" ");
        match value {
            StatValue::Number(n) => self.number(n),
            StatValue::Text(s) => self.buf.extend_from_slice(s.as_bytes()),
        }
        self.buf.extend_from_slice(b"\r\n");
    }

    /// Writes and flushes everything queued, returning the bytes written.
    ///
    /// Not cancel-safe: the queue is consumed while writing, so dropping the
    /// future midway loses the unsent bytes. Sessions never `select!` on it.
    pub async fn write_to<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> io::Result<usize> {
        let n = self.len();
        if n == 0 {
            return Ok(0);
        }

        self.copied = 0;
        if self.chunks.is_empty() {
            writer.write_all(&self.buf).await?;
        } else {
            let mut chunks = self.chunks.drain(..).collect::<VecDeque<_>>();
            self.chunks_len = 0;
            let rest = self.buf.split().freeze();
            if !rest.is_empty() {
                chunks.push_back(rest);
            }
            writer
                .write_all_buf(&mut ChunkQueue {
                    chunks,
                    remaining: n,
                })
                .await?;
        }
        writer.flush().await?;

        self.buf.clear();
        // Don't keep a large response's buffer around for the connection's lifetime
        if self.buf.capacity() > 2 * MAX_BUFFERED {
            self.buf = BytesMut::new();
        }

        Ok(n)
    }

    fn push_chunk(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.chunks_len += chunk.len();
            self.chunks.push(chunk);
        }
    }

    fn number(&mut self, n: u64) {
        self.buf
            .extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
    }

    fn line(&mut self, prefix: &[u8], msg: &str) {
        self.buf.extend_from_slice(prefix);
        self.buf.extend_from_slice(msg.as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }
}

/// The chunks being sent, as a `Buf` so tokio can `writev` them. Holds no empty chunks.
struct ChunkQueue {
    chunks: VecDeque<Bytes>,
    remaining: usize,
}

impl Buf for ChunkQueue {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.chunks.front().map_or(&[], Bytes::as_ref)
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let Some(front) = self.chunks.front_mut() else {
                break;
            };
            let step = cnt.min(front.len());
            if step == front.len() {
                self.chunks.pop_front();
            } else {
                front.advance(step);
            }
            self.remaining -= step;
            cnt -= step;
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut n = 0;
        for (slot, chunk) in dst.iter_mut().zip(&self.chunks) {
            *slot = IoSlice::new(chunk);
            n += 1;
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use super::*;

    fn value<'a>(key: &'a [u8], data: &'a Bytes, cas: Option<u64>) -> Value<'a> {
        Value {
            key,
            flags: 5,
            data,
            cas,
        }
    }

    async fn sent(out: &mut Output) -> Vec<u8> {
        let mut written = Vec::new();
        let n = out.write_to(&mut written).await.unwrap();
        assert_eq!(n, written.len());
        assert!(out.is_empty());
        written
    }

    #[tokio::test]
    async fn encodes_simple_responses() {
        let mut out = Output::default();
        out.response(&Response::Stored);
        out.response(&Response::Number(42));
        out.response(&Response::Version("1.2.3"));
        out.response(&Response::ClientError("bad data chunk"));
        out.response(&Response::ServerError("out of memory storing object"));

        assert_eq!(
            sent(&mut out).await,
            b"STORED\r\n42\r\nVERSION 1.2.3\r\nCLIENT_ERROR bad data chunk\r\n\
              SERVER_ERROR out of memory storing object\r\n"
        );
    }

    #[tokio::test]
    async fn encodes_values_with_and_without_cas() {
        let mut out = Output::default();
        let data = Bytes::from_static(b"hello");
        out.value(value(b"foo", &data, None));
        out.value(value(b"bar", &data, Some(7)));
        out.response(&Response::End);

        assert_eq!(
            sent(&mut out).await,
            b"VALUE foo 5 5\r\nhello\r\nVALUE bar 5 5 7\r\nhello\r\nEND\r\n"
        );
    }

    #[tokio::test]
    async fn encodes_stats() {
        let mut out = Output::default();
        out.stat("pid", StatValue::Number(12));
        out.stat("version", StatValue::Text("1.0"));
        out.response(&Response::End);

        assert_eq!(
            sent(&mut out).await,
            b"STAT pid 12\r\nSTAT version 1.0\r\nEND\r\n"
        );
    }

    #[tokio::test]
    async fn large_values_are_queued_by_reference_in_order() {
        let mut out = Output::default();
        let small = Bytes::from_static(b"small");
        let large = Bytes::from(vec![b'x'; MAX_BUFFERED]);

        out.value(value(b"a", &small, None));
        out.value(value(b"b", &large, None));
        out.value(value(b"c", &small, None));
        out.response(&Response::End);

        // The large value isn't copied: the queued chunk shares its allocation
        assert!(out.chunks.iter().any(|c| c.as_ptr() == large.as_ptr()));

        let mut expected = b"VALUE a 5 5\r\nsmall\r\n".to_vec();
        expected.extend_from_slice(format!("VALUE b 5 {MAX_BUFFERED}\r\n").as_bytes());
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"\r\nVALUE c 5 5\r\nsmall\r\nEND\r\n");
        assert_eq!(sent(&mut out).await, expected);
    }

    #[tokio::test]
    async fn copies_stop_once_max_buffered_is_queued() {
        let mut out = Output::default();
        let data = Bytes::from(vec![b'x'; 1024]);
        for _ in 0..2 * MAX_BUFFERED / 1024 {
            out.value(value(b"k", &data, None));
        }

        assert!(out.buf.len() <= MAX_BUFFERED);
        assert!(!out.chunks.is_empty());
        let per_value = b"VALUE k 5 1024\r\n".len() + 1024 + 2;
        assert_eq!(
            sent(&mut out).await.len(),
            2 * MAX_BUFFERED / 1024 * per_value
        );
    }

    #[tokio::test]
    async fn small_values_after_a_large_one_are_copied() {
        let mut out = Output::default();
        let small = Bytes::from_static(b"small");
        let large = Bytes::from(vec![b'x'; MAX_BUFFERED]);

        out.value(value(b"big", &large, None));
        for _ in 0..1000 {
            out.value(value(b"s", &small, None));
        }
        out.response(&Response::End);

        // header (copied, frozen) + large value only
        assert_eq!(out.chunks.len(), 2);

        let mut expected = format!("VALUE big 5 {MAX_BUFFERED}\r\n").into_bytes();
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"\r\n");
        for _ in 0..1000 {
            expected.extend_from_slice(b"VALUE s 5 5\r\nsmall\r\n");
        }
        expected.extend_from_slice(b"END\r\n");
        assert_eq!(sent(&mut out).await, expected);
    }

    #[tokio::test]
    async fn copied_resets_after_each_send() {
        let mut out = Output::default();
        let data = Bytes::from(vec![b'x'; MAX_BUFFERED - 100]);
        out.value(value(b"a", &data, None));
        assert!(out.chunks.is_empty());
        sent(&mut out).await;
        out.value(value(b"a", &data, None));
        assert!(out.chunks.is_empty());
    }

    fn queue(parts: &[&'static [u8]]) -> ChunkQueue {
        ChunkQueue {
            chunks: parts.iter().map(|p| Bytes::from_static(p)).collect(),
            remaining: parts.iter().map(|p| p.len()).sum(),
        }
    }

    #[test]
    fn chunk_queue_advances_across_chunk_boundaries() {
        let mut q = queue(&[b"abc", b"de", b"fghi"]);
        assert_eq!(q.remaining(), 9);
        assert_eq!(q.chunk(), b"abc");

        q.advance(2);
        assert_eq!((q.remaining(), q.chunk()), (7, &b"c"[..]));

        q.advance(1);
        assert_eq!((q.remaining(), q.chunk()), (6, &b"de"[..]));

        q.advance(3); // all of "de" and one byte of "fghi"
        assert_eq!((q.remaining(), q.chunk()), (3, &b"ghi"[..]));

        q.advance(0);
        assert_eq!(q.remaining(), 3);

        q.advance(100); // past the end just stops
        assert_eq!(q.remaining(), 0);
        assert!(!q.has_remaining());
        assert_eq!(q.chunk(), b"");
    }

    #[test]
    fn chunk_queue_fills_vectored_slices() {
        let mut q = queue(&[b"abc", b"de", b"fghi"]);
        q.advance(1);

        let mut slices = [IoSlice::new(&[]); 8];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, 3);
        assert_eq!(&*slices[0], b"bc");
        assert_eq!(&*slices[1], b"de");
        assert_eq!(&*slices[2], b"fghi");

        // Fewer slices than chunks
        let mut slices = [IoSlice::new(&[]); 2];
        assert_eq!(q.chunks_vectored(&mut slices), 2);
        assert_eq!(&*slices[1], b"de");

        assert_eq!(queue(&[]).chunks_vectored(&mut slices), 0);
    }

    /// Accepts at most `max` bytes per write; vectored writes only take the
    /// first `max` bytes spread over the slices when `vectored` is set.
    struct Trickle {
        written: Vec<u8>,
        max: usize,
        vectored: bool,
        vectored_calls: usize,
    }

    impl Trickle {
        fn new(max: usize, vectored: bool) -> Self {
            Self {
                written: Vec::new(),
                max,
                vectored,
                vectored_calls: 0,
            }
        }
    }

    impl AsyncWrite for Trickle {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let n = buf.len().min(self.max);
            self.written.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.vectored_calls += 1;
            let mut left = self.max;
            for b in bufs {
                let n = b.len().min(left);
                self.written.extend_from_slice(&b[..n]);
                left -= n;
                if left == 0 {
                    break;
                }
            }
            Poll::Ready(Ok(self.max - left))
        }

        fn is_write_vectored(&self) -> bool {
            self.vectored
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn mixed_output() -> (Output, Vec<u8>) {
        let mut out = Output::default();
        let small = Bytes::from_static(b"small");
        let large = Bytes::from((0..MAX_BUFFERED).map(|i| i as u8).collect::<Vec<_>>());
        out.value(value(b"a", &small, None));
        out.value(value(b"b", &large, Some(9)));
        out.value(value(b"c", &small, None));
        out.response(&Response::End);
        assert!(!out.chunks.is_empty());

        let mut expected = b"VALUE a 5 5\r\nsmall\r\n".to_vec();
        expected.extend_from_slice(format!("VALUE b 5 {MAX_BUFFERED} 9\r\n").as_bytes());
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"\r\nVALUE c 5 5\r\nsmall\r\nEND\r\n");
        (out, expected)
    }

    #[tokio::test]
    async fn partial_writes_still_deliver_everything_in_order() {
        let (mut out, expected) = mixed_output();
        let mut writer = Trickle::new(3, false);
        let n = out.write_to(&mut writer).await.unwrap();
        assert_eq!(n, expected.len());
        assert_eq!(writer.written, expected);
        assert_eq!(writer.vectored_calls, 0);
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn partial_vectored_writes_still_deliver_everything_in_order() {
        let (mut out, expected) = mixed_output();
        let mut writer = Trickle::new(7, true);
        let n = out.write_to(&mut writer).await.unwrap();
        assert_eq!(n, expected.len());
        assert_eq!(writer.written, expected);
        assert!(writer.vectored_calls > 0);
        assert!(out.is_empty());

        // The output is reusable afterwards
        out.response(&Response::Stored);
        let mut writer = Trickle::new(7, true);
        out.write_to(&mut writer).await.unwrap();
        assert_eq!(writer.written, b"STORED\r\n");
    }

    #[tokio::test]
    async fn writing_nothing_is_a_no_op() {
        let mut out = Output::default();
        assert_eq!(sent(&mut out).await, b"");
    }
}
