//! Original fixed send-buffer funding for one opt-in HTTP/2 connection.

use bytes::Bytes;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct Core {
    bound: AtomicBool,
    capacity: usize,
    max_payload: usize,
    // The final Core Arc allocation exits before this original carrier.
    _ownership: Bytes,
}

/// Original ownership for one fixed send buffer and its Core allocation.
///
/// Obtain the complete allocation bound before construction. Construction
/// allocates only Core; the bound writer constructs the actual fixed BytesMut
/// backing after acquiring its unique lease. The writer must free that complete
/// backing before dropping the lease, and must not grow or let backing aliases
/// outlive it. Additional public clones conservatively retain original funding.
///
/// This covers the buffer and Core only. HPACK, queued HeaderMaps, stream queues,
/// socket/task allocations and ownership-carrier metadata are separate. This
/// does not claim allocator caches or whole-process RSS. Defaults do not install
/// this carrier.
pub struct SendFrameBuffer {
    core: Option<Arc<Core>>,
}

impl SendFrameBuffer {
    /// Conservative Rust-requested bound for the complete fixed buffer capacity
    /// and Core/Arc allocation. Caller ownership-carrier metadata is excluded.
    pub fn allocation_capacity_bound(
        capacity_bytes: usize,
        max_payload: usize,
    ) -> io::Result<usize> {
        validate(capacity_bytes, max_payload)?;
        std::mem::size_of::<Core>()
            .checked_add(3 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::align_of::<Core>()))
            .and_then(|n| n.checked_add(capacity_bytes))
            .ok_or_else(|| invalid("send frame allocation size overflow"))
    }

    /// Construct under the supplied original physical-exit ownership carrier.
    /// The actual buffer is allocated by the writer after binding, not here.
    pub fn new(capacity_bytes: usize, max_payload: usize, ownership: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound(capacity_bytes, max_payload)?;
        Ok(Self {
            core: Some(Arc::new(Core {
                bound: AtomicBool::new(false),
                capacity: capacity_bytes,
                max_payload,
                _ownership: ownership,
            })),
        })
    }

    /// Complete fixed send-buffer capacity, including frame headers.
    pub fn capacity_bytes(&self) -> usize {
        self.core().capacity
    }

    /// Maximum frame payload, excluding its nine-byte header.
    pub fn max_payload_bytes(&self) -> usize {
        self.core().max_payload
    }

    pub(crate) fn bind(&self) -> io::Result<BoundSendFrameBuffer> {
        self.core()
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("send frame buffer already bound to a connection"))?;
        Ok(BoundSendFrameBuffer {
            buffer: self.clone(),
        })
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live send frame buffer")
    }
}

impl Clone for SendFrameBuffer {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(
                self.core.as_ref().expect("live send frame buffer"),
            )),
        }
    }
}

impl Drop for SendFrameBuffer {
    fn drop(&mut self) {
        // No Weak/raw Arc escapes. Free the final Arc allocation before Core
        // and then its original ownership carrier exit. The writer separately
        // guarantees its actual buffer has already physically exited.
        drop(Arc::into_inner(
            self.core.take().expect("live send frame buffer"),
        ));
    }
}

impl fmt::Debug for SendFrameBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendFrameBuffer")
            .field("capacity_bytes", &self.capacity_bytes())
            .field("max_payload_bytes", &self.max_payload_bytes())
            .finish_non_exhaustive()
    }
}

/// Non-cloneable lease for the sole fixed writer backing.
#[derive(Debug)]
pub(crate) struct BoundSendFrameBuffer {
    buffer: SendFrameBuffer,
}

impl BoundSendFrameBuffer {
    pub(crate) fn capacity_bytes(&self) -> usize {
        self.buffer.capacity_bytes()
    }

    pub(crate) fn max_payload_bytes(&self) -> usize {
        self.buffer.max_payload_bytes()
    }
}

fn validate(capacity: usize, max_payload: usize) -> io::Result<()> {
    if !(16384..=16777215).contains(&max_payload) {
        return Err(invalid("invalid fixed send frame geometry"));
    }
    let minimum = max_payload
        .checked_add(9)
        .ok_or_else(|| invalid("send frame geometry size overflow"))?;
    if capacity < minimum {
        return Err(invalid("fixed send capacity cannot hold a complete frame"));
    }
    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
