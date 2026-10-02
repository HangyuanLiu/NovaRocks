//! Original fixed backing for one whole outbound HPACK block.

use crate::ReceiveBufferPool;
use bytes::Bytes;
use std::io;

/// One fixed encoded-header slot, retained through the last escaping alias.
///
/// Obtain the complete allocation bound before construction and supply the
/// original physical-exit carrier. This funds the block, pool Core, slot storage
/// and payload wrapper only. HeaderMap storage, HPACK tables, writer buffers,
/// socket/task allocations and ownership-carrier metadata are separate.
///
/// The connection builder must explicitly disable its outbound dynamic table
/// with `max_send_header_table_size(0)`. With that policy, two table-size
/// updates require at most twenty bytes, and literal names/values fit within
/// four times the decoded header-list size. This type does not change the
/// builder's table policy or validate headers on its behalf.
#[derive(Clone, Debug)]
pub struct SendHeaderBlockPool {
    pool: ReceiveBufferPool,
    max_header_list_size: usize,
}

impl SendHeaderBlockPool {
    /// Conservative Rust-requested bound for the fixed block and pool backing.
    /// Caller ownership-carrier metadata and other connection objects are not
    /// included. No allocator-cache or whole-process RSS claim is made.
    pub fn allocation_capacity_bound(max_header_list_size: usize) -> io::Result<usize> {
        ReceiveBufferPool::allocation_capacity_bound_for_payload(
            1,
            block_capacity(max_header_list_size)?,
        )
    }

    /// Construct the sole block under its pregranted original ownership.
    pub fn new(max_header_list_size: usize, ownership: Bytes) -> io::Result<Self> {
        let capacity = block_capacity(max_header_list_size)?;
        Ok(Self {
            pool: ReceiveBufferPool::new_for_payload(1, capacity, ownership)?,
            max_header_list_size,
        })
    }

    /// Maximum decoded header-list size, including each field's 32-byte cost.
    pub fn max_header_list_size(&self) -> usize {
        self.max_header_list_size
    }

    /// Complete encoded-block backing capacity, independent of visible length.
    pub fn buffer_capacity_bytes(&self) -> usize {
        self.pool.buffer_capacity_bytes()
    }

    /// Bind once across every public clone to one outbound connection encoder.
    pub(crate) fn bind(&self) -> io::Result<Self> {
        Ok(Self {
            pool: self.pool.bind(self.buffer_capacity_bytes())?,
            max_header_list_size: self.max_header_list_size,
        })
    }

    /// Acquire the slot before encoding. A busy slot does not invoke `encode`.
    /// The encoder must remain within the complete preflighted block capacity;
    /// pool checkout restores the slot if the callback unwinds.
    pub(crate) fn try_encode<F>(&self, encode: F) -> Option<Bytes>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        self.pool.try_fill_payload(encode)
    }
}

fn block_capacity(max_header_list_size: usize) -> io::Result<usize> {
    if max_header_list_size == 0 || u32::try_from(max_header_list_size).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid fixed send header-list maximum",
        ));
    }
    max_header_list_size
        .checked_mul(4)
        .and_then(|n| n.checked_add(20))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "send header block capacity overflow",
            )
        })
}
