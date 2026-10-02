//! Original typed backing for the inbound HPACK dynamic table.

use crate::hpack::{DecoderError, Header};
use bytes::Bytes;
use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct Core {
    // Empty before binding, None afterwards. A single successful CAS grants
    // exclusive access; no other operation reads or writes this cell.
    slots: UnsafeCell<Option<Vec<Option<Header>>>>,
    bound: AtomicBool,
    max_bytes: usize,
    // Original typed backing and the final Arc allocation exit first.
    _ownership: Bytes,
}
// SAFETY: only the winner of the once-only bind CAS accesses slots. The
// noncloneable lease owns the extracted Vec; public aliases access metadata.
unsafe impl Send for Core {}
unsafe impl Sync for Core {}

/// Fixed typed slots for one connection's inbound HPACK dynamic table.
///
/// HPACK's 32-byte entry overhead is a protocol quantity, not the actual Rust
/// allocation size. Obtain the complete typed-slot/Core/Arc bound before
/// construction. The buffer must cover the initial 4096-byte protocol table
/// and the advertised incoming ceiling. Regular field payloads retain their
/// independent original field pool; pseudo-header conversion, field bytes and
/// HeaderMaps are outside this typed-backing bound.
pub struct ReceiveHeaderTableBuffer {
    core: Option<Arc<Core>>,
}

impl ReceiveHeaderTableBuffer {
    /// Complete Rust-requested typed-slot/Core/Arc allocation bound.
    /// Caller carrier metadata and allocator caches/RSS are separate.
    pub fn allocation_capacity_bound(max_table_bytes: usize) -> io::Result<usize> {
        let layout = validate(max_table_bytes)?;
        layout
            .size()
            .checked_add(std::mem::size_of::<Core>())
            .and_then(|n| n.checked_add(3 * std::mem::size_of::<usize>()))
            .and_then(|n| n.checked_add(std::mem::align_of::<Core>()))
            .ok_or_else(|| invalid("header table allocation size overflow"))
    }

    /// Construct only after obtaining the complete original capacity bound.
    pub fn new(max_table_bytes: usize, ownership: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound(max_table_bytes)?;
        let count = max_table_bytes / 32;
        let slots = (0..count).map(|_| None).collect::<Vec<Option<Header>>>();
        assert_eq!(slots.capacity(), count);
        Ok(Self {
            core: Some(Arc::new(Core {
                slots: UnsafeCell::new(Some(slots)),
                bound: AtomicBool::new(false),
                max_bytes: max_table_bytes,
                _ownership: ownership,
            })),
        })
    }

    /// Maximum protocol table size covered by the original typed allocation.
    pub fn max_table_bytes(&self) -> usize {
        self.core().max_bytes
    }

    pub(crate) fn bind(&self) -> io::Result<BoundHeaderTableBuffer> {
        self.core()
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("header table buffer already bound to a connection"))?;
        // SAFETY: the successful once-only CAS grants exclusive access. No
        // public alias reads this cell and no later bind can reach it.
        let slots =
            unsafe { (&mut *self.core().slots.get()).take() }.expect("unbound header table slots");
        Ok(BoundHeaderTableBuffer {
            slots,
            _owner: self.clone(),
            head: 0,
            len: 0,
        })
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live header table buffer")
    }
}

impl Clone for ReceiveHeaderTableBuffer {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core.as_ref().unwrap())),
        }
    }
}
impl Drop for ReceiveHeaderTableBuffer {
    fn drop(&mut self) {
        drop(Arc::into_inner(self.core.take().unwrap()));
    }
}
impl fmt::Debug for ReceiveHeaderTableBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveHeaderTableBuffer")
            .field("max_table_bytes", &self.max_table_bytes())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct BoundHeaderTableBuffer {
    // Field order is deliberate: Vec drop glue clears every entry and frees
    // the original typed allocation before the owner, including during unwind.
    slots: Vec<Option<Header>>,
    _owner: ReceiveHeaderTableBuffer,
    head: usize,
    len: usize,
}

impl BoundHeaderTableBuffer {
    pub(crate) fn get(&self, index: usize) -> Option<&Header> {
        if index >= self.len {
            return None;
        }
        self.slots[(self.head + index) % self.slots.len()].as_ref()
    }
    pub(crate) fn back(&self) -> Option<&Header> {
        self.len.checked_sub(1).and_then(|last| self.get(last))
    }
    pub(crate) fn pop_back(&mut self) -> Option<Header> {
        let last = self.len.checked_sub(1)?;
        let index = (self.head + last) % self.slots.len();
        self.len = last;
        self.slots[index].take()
    }
    pub(crate) fn push_front(&mut self, header: Header) -> Result<(), DecoderError> {
        if self.len == self.slots.len() {
            return Err(DecoderError::HeaderTableBufferExhausted);
        }
        self.head = (self.head + self.slots.len() - 1) % self.slots.len();
        debug_assert!(self.slots[self.head].is_none());
        self.slots[self.head] = Some(header);
        self.len += 1;
        Ok(())
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn validate(max: usize) -> io::Result<std::alloc::Layout> {
    if !(4096..=u32::MAX as usize).contains(&max) {
        return Err(invalid(
            "header table capacity must cover 4096 bytes and fit u32",
        ));
    }
    std::alloc::Layout::array::<Option<Header>>(max / 32)
        .map_err(|_| invalid("header table typed layout exceeds addressable allocation"))
}
