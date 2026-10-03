use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::rt;
use hyper_util::client::legacy::connect::{Connected as HyperConnected, Connection};

pub(in crate::transport) trait Io:
    rt::Read + rt::Write + Send + 'static
{
}

impl<T> Io for T where T: rt::Read + rt::Write + Send + 'static {}

pub(crate) struct BoxedIo(Pin<Box<dyn Io>>);

impl BoxedIo {
    pub(in crate::transport) fn new<I: Io>(io: I) -> Self {
        BoxedIo(Box::pin(io))
    }
}

impl Connection for BoxedIo {
    fn connected(&self) -> HyperConnected {
        HyperConnected::new()
    }
}

impl rt::Read for BoxedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl rt::Write for BoxedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
}

/// Inline original capability outside every box owned by the final connector
/// output. No extraction API lets an IO box escape without its capability.
pub(crate) struct OwnedConnectionIo<I> {
    io: Option<I>,
    owner: Option<bytes::Bytes>,
}

impl<I> OwnedConnectionIo<I> {
    pub(in crate::transport) fn new(io: I, owner: Option<bytes::Bytes>) -> Self {
        Self {
            io: Some(io),
            owner,
        }
    }
}

impl<I> Drop for OwnedConnectionIo<I> {
    fn drop(&mut self) {
        // Declare the owner first so unwinding from IO destruction still
        // deallocates every owned box before dropping this final capability.
        let owner = self.owner.take();
        let io = self.io.take();
        drop(io);
        drop(owner);
    }
}

impl<I: rt::Read + Unpin> rt::Read for OwnedConnectionIo<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(
            self.get_mut()
                .io
                .as_mut()
                .expect("live owned connection IO"),
        )
        .poll_read(cx, buf)
    }
}

impl<I: rt::Write + Unpin> rt::Write for OwnedConnectionIo<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(
            self.get_mut()
                .io
                .as_mut()
                .expect("live owned connection IO"),
        )
        .poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.get_mut()
                .io
                .as_mut()
                .expect("live owned connection IO"),
        )
        .poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.get_mut()
                .io
                .as_mut()
                .expect("live owned connection IO"),
        )
        .poll_shutdown(cx)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(
            self.get_mut()
                .io
                .as_mut()
                .expect("live owned connection IO"),
        )
        .poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.io
            .as_ref()
            .expect("live owned connection IO")
            .is_write_vectored()
    }
}
