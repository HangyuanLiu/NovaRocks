//! Fixed raw frame input backing for one opt-in HTTP/2 connection.

use bytes::{Bytes, BytesMut};
use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

struct Core {
    buffer: UnsafeCell<Vec<u8>>,
    bound: AtomicBool,
    max_payload: usize,
    // Core's Arc allocation and Vec exit before this original owner.
    _ownership: Bytes,
}
// SAFETY: bind mints only one non-cloneable mutable lease for this Core.
// Public handles expose no access to its buffer. Only that lease's &mut self
// lends mutable storage to the single parser; final Core Drop requires all
// strong handles (including the lease) to have exited.
unsafe impl Sync for Core {}

/// Fixed raw input storage whose original funding lasts through its physical
/// allocation exit. One buffer binds once to one connection, before I/O.
///
/// This covers the raw input Vec and Core allocation only. The emitted
/// BytesMut frame copy, HPACK/headers/continuations, retained DATA, socket,
/// task backing and caller ownership-carrier metadata are separate.
/// Defaults do not install this buffer.
pub struct ReceiveFrameBuffer {
    core: Option<Arc<Core>>,
}

impl ReceiveFrameBuffer {
    /// Conservative Rust-requested allocation bound for Core/Arc and the full
    /// fixed input Vec, including its nine-byte frame header. Caller ownership
    /// carrier metadata and allocator caches/RSS are excluded.
    pub fn allocation_capacity_bound(max_payload: usize) -> io::Result<usize> {
        validate(max_payload)?;
        std::mem::size_of::<Core>()
            .checked_add(3 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::align_of::<Core>()))
            .and_then(|n| n.checked_add(max_payload))
            .and_then(|n| n.checked_add(9))
            .ok_or_else(|| invalid("raw receive allocation size overflow"))
    }

    /// Construct only after the caller obtains the complete bound and supplies
    /// its original physical-exit ownership carrier. No read grows the Vec.
    pub fn new(max_payload: usize, ownership: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound(max_payload)?;
        let capacity = max_payload + 9;
        let buffer = vec![0; capacity];
        assert_eq!(buffer.capacity(), capacity);
        Ok(Self {
            core: Some(Arc::new(Core {
                buffer: UnsafeCell::new(buffer),
                bound: AtomicBool::new(false),
                max_payload,
                _ownership: ownership,
            })),
        })
    }

    /// Fixed payload capacity, excluding the nine-byte header.
    pub fn max_payload_bytes(&self) -> usize {
        self.core().max_payload
    }

    pub(crate) fn bind(&self, max_payload: usize) -> io::Result<BoundFrameBuffer> {
        if max_payload > self.max_payload_bytes() {
            return Err(invalid("local frame maximum exceeds raw receive buffer"));
        }
        self.core()
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("raw receive buffer already bound to a connection"))?;
        Ok(BoundFrameBuffer {
            buffer: self.clone(),
        })
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live raw receive buffer")
    }
}
impl Clone for ReceiveFrameBuffer {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(
                self.core.as_ref().expect("live raw receive buffer"),
            )),
        }
    }
}
impl Drop for ReceiveFrameBuffer {
    fn drop(&mut self) {
        // No Weak/raw Arc escapes. Free the final Arc allocation before Core,
        // its complete Vec and finally the original ownership carrier exit.
        drop(Arc::into_inner(
            self.core.take().expect("live raw receive buffer"),
        ));
    }
}
impl fmt::Debug for ReceiveFrameBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveFrameBuffer")
            .field("max_payload_bytes", &self.max_payload_bytes())
            .finish_non_exhaustive()
    }
}

pub(crate) struct BoundFrameBuffer {
    buffer: ReceiveFrameBuffer,
}
impl BoundFrameBuffer {
    fn storage(&mut self) -> &mut [u8] {
        // SAFETY: this non-cloneable lease is minted by the sole successful
        // bind CAS. Only its exclusive borrower can access the Vec.
        unsafe { &mut *self.buffer.core().buffer.get() }
    }
}

pub(crate) struct FixedFrameRead<T> {
    io: T,
    filled: usize,
    target: usize,
    max_payload: usize,
    failed: bool,
    buffer: BoundFrameBuffer,
}
impl<T> FixedFrameRead<T> {
    pub(crate) fn new(io: T, buffer: BoundFrameBuffer, max_payload: usize) -> Self {
        Self {
            io,
            filled: 0,
            target: 9,
            max_payload,
            failed: false,
            buffer,
        }
    }
    pub(crate) fn get_ref(&self) -> &T {
        &self.io
    }
    pub(crate) fn get_mut(&mut self) -> &mut T {
        &mut self.io
    }
    pub(crate) fn max_payload(&self) -> usize {
        self.max_payload
    }
    pub(crate) fn set_max_payload(&mut self, max: usize) {
        assert!(max <= self.buffer.buffer.max_payload_bytes());
        self.max_payload = max;
    }
}
impl<T: AsyncRead + Unpin> FixedFrameRead<T> {
    pub(crate) fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<BytesMut>>> {
        if self.failed {
            return Poll::Ready(None);
        }
        loop {
            while self.filled < self.target {
                let storage = self.buffer.storage();
                let mut read = ReadBuf::new(&mut storage[self.filled..self.target]);
                match Pin::new(&mut self.io).poll_read(cx, &mut read) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        self.failed = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(Ok(())) => {
                        let read = read.filled().len();
                        if read == 0 {
                            self.failed = true;
                            return if self.filled == 0 {
                                Poll::Ready(None)
                            } else {
                                Poll::Ready(Some(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "truncated HTTP/2 frame",
                                ))))
                            };
                        }
                        self.filled += read;
                    }
                }
            }
            if self.target == 9 {
                let bytes = self.buffer.storage();
                let payload = (usize::from(bytes[0]) << 16)
                    | (usize::from(bytes[1]) << 8)
                    | usize::from(bytes[2]);
                if payload > self.max_payload {
                    self.failed = true;
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        FrameSizeExceeded,
                    ))));
                }
                self.target = 9 + payload;
                if self.filled < self.target {
                    continue;
                }
            }
            // This separate exact frame copy is bounded before allocation, but
            // is not part of the fixed input Vec's ownership/allocation claim.
            let frame = BytesMut::from(&self.buffer.storage()[..self.target]);
            self.filled = 0;
            self.target = 9;
            return Poll::Ready(Some(Ok(frame)));
        }
    }
}

#[derive(Debug)]
pub(crate) struct FrameSizeExceeded;
impl fmt::Display for FrameSizeExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP/2 frame exceeds local receive maximum")
    }
}
impl std::error::Error for FrameSizeExceeded {}
fn validate(max: usize) -> io::Result<()> {
    if !(16384..=16777215).contains(&max) {
        return Err(invalid("invalid fixed raw receive geometry"));
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
