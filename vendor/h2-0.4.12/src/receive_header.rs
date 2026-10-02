//! Fixed encoded header input backing for one opt-in HTTP/2 connection.

use bytes::Bytes;
use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct Core {
    buffer: UnsafeCell<Vec<u8>>,
    bound: AtomicBool,
    max_encoded: usize,
    // Core's Arc allocation and Vec exit before this original owner.
    _ownership: Bytes,
}
// SAFETY: bind mints only one non-cloneable mutable lease for this Core.
// Public handles expose no access to its buffer. Only that lease's &mut self
// lends mutable storage to the single parser; final Core Drop requires all
// strong handles (including the lease) to have exited.
unsafe impl Sync for Core {}

/// Original fixed encoded HEADERS/CONTINUATION input capacity for one connection.
///
/// Obtain the complete Vec/Core/Arc bound before construction. A unique once-
/// bound lease appends and incrementally decodes the entire bounded block.
/// Encoded input never escapes this workspace: decoded strings get independent
/// owners. Public clones conservatively retain original funding until final
/// physical exit. HeaderMap, decoded fields/Huffman/table, socket/task/carrier
/// metadata and allocator caches are separate. Defaults do not install it.
pub struct ReceiveHeaderBlockBuffer {
    core: Option<Arc<Core>>,
}

impl ReceiveHeaderBlockBuffer {
    /// Conservative Rust-requested allocation bound for Core/Arc and the full
    /// fixed input Vec, excluding wire frame headers. Caller ownership
    /// carrier metadata and allocator caches/RSS are excluded.
    pub fn allocation_capacity_bound(max_encoded: usize) -> io::Result<usize> {
        validate(max_encoded)?;
        std::mem::size_of::<Core>()
            .checked_add(3 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::align_of::<Core>()))
            .and_then(|n| n.checked_add(max_encoded))
            .ok_or_else(|| invalid("encoded header allocation size overflow"))
    }

    /// Construct only after the caller obtains the complete bound and supplies
    /// its original physical-exit ownership carrier. No read grows the Vec.
    pub fn new(max_encoded: usize, ownership: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound(max_encoded)?;
        let capacity = max_encoded;
        let buffer = vec![0; capacity];
        assert_eq!(buffer.capacity(), capacity);
        Ok(Self {
            core: Some(Arc::new(Core {
                buffer: UnsafeCell::new(buffer),
                bound: AtomicBool::new(false),
                max_encoded,
                _ownership: ownership,
            })),
        })
    }

    /// Complete encoded block capacity, excluding wire frame headers.
    pub fn max_encoded_bytes(&self) -> usize {
        self.core().max_encoded
    }

    pub(crate) fn bind(&self, max_encoded: usize) -> io::Result<BoundHeaderBlockBuffer> {
        validate(max_encoded)?;
        if max_encoded > self.max_encoded_bytes() {
            return Err(invalid("local encoded block maximum exceeds header buffer"));
        }
        self.core()
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("encoded header buffer already bound to a connection"))?;
        Ok(BoundHeaderBlockBuffer {
            buffer: self.clone(),
            filled: 0,
            committed: 0,
            limit: max_encoded,
        })
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live encoded header buffer")
    }
}
impl Clone for ReceiveHeaderBlockBuffer {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(
                self.core.as_ref().expect("live encoded header buffer"),
            )),
        }
    }
}
impl Drop for ReceiveHeaderBlockBuffer {
    fn drop(&mut self) {
        // No Weak/raw Arc escapes. Free the final Arc allocation before Core,
        // its complete Vec and finally the original ownership carrier exit.
        drop(Arc::into_inner(
            self.core.take().expect("live encoded header buffer"),
        ));
    }
}
impl fmt::Debug for ReceiveHeaderBlockBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveHeaderBlockBuffer")
            .field("max_encoded_bytes", &self.max_encoded_bytes())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct BoundHeaderBlockBuffer {
    buffer: ReceiveHeaderBlockBuffer,
    filled: usize,
    committed: usize,
    limit: usize,
}
impl BoundHeaderBlockBuffer {
    pub(crate) fn reset(&mut self) {
        self.filled = 0;
        self.committed = 0;
    }
    pub(crate) fn append(&mut self, input: &[u8]) -> bool {
        let Some(end) = self
            .filled
            .checked_add(input.len())
            .filter(|n| *n <= self.limit)
        else {
            return false;
        };
        let start = self.filled;
        self.storage()[start..end].copy_from_slice(input);
        self.filled = end;
        true
    }
    pub(crate) fn decode<R>(&mut self, f: impl FnOnce(&[u8], &mut usize) -> R) -> R {
        // SAFETY: only this non-cloneable mutable lease accesses Core storage.
        // The callback cannot return a borrow of this private input.
        let storage = unsafe { &*self.buffer.core().buffer.get() };
        f(&storage[..self.filled], &mut self.committed)
    }
    fn storage(&mut self) -> &mut [u8] {
        // SAFETY: this non-cloneable lease is minted by the sole successful
        // bind CAS. Only its exclusive borrower can access the Vec.
        unsafe { &mut *self.buffer.core().buffer.get() }
    }
}

fn validate(max: usize) -> io::Result<()> {
    if max == 0 || max > u32::MAX as usize {
        return Err(invalid("invalid fixed encoded header geometry"));
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
