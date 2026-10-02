//! Original aggregate backing for independently retained decoded fields.

use crate::hpack::DecoderError;
use bytes::Bytes;
use http::header::{HeaderFieldAllocationPool, HeaderFieldFillError};
use std::{fmt, io};

/// Fixed aggregate bytes and a finite number of independently retained fields.
///
/// The neutral HTTP arena owns all physical storage and final wrapper retirement.
/// Obtain its complete bound before construction; exhaustion does not wait or
/// allocate a fallback. HeaderMap/table/pseudo-header metadata remain separate.
#[derive(Clone)]
pub struct ReceiveHeaderFieldPool {
    allocation: HeaderFieldAllocationPool,
}

impl ReceiveHeaderFieldPool {
    /// Complete arena/bitmap/Core/Arc and maximum live wrapper allocation bound.
    /// Caller carrier metadata and allocator caches/RSS remain separate.
    pub fn allocation_capacity_bound(
        capacity: usize,
        positions: usize,
        max_field: usize,
    ) -> io::Result<usize> {
        HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, max_field)
    }

    /// Construct after obtaining the complete original capacity bound.
    pub fn new(
        capacity: usize,
        positions: usize,
        max_field: usize,
        ownership: Bytes,
    ) -> io::Result<Self> {
        HeaderFieldAllocationPool::new(capacity, positions, max_field, ownership)
            .map(|allocation| Self { allocation })
    }

    /// Actual fixed arena capacity, including extent rounding and fragmentation.
    pub fn capacity_bytes(&self) -> usize {
        self.allocation.capacity_bytes()
    }
    /// Maximum simultaneous prepared/live/retiring owner wrappers.
    pub fn field_positions(&self) -> usize {
        self.allocation.field_positions()
    }
    /// Maximum decoded length of one field name or value.
    pub fn max_field_bytes(&self) -> usize {
        self.allocation.max_field_bytes()
    }
    /// Positions free after previous wrappers physically exited.
    pub fn available_positions(&self) -> usize {
        self.allocation.available_positions()
    }
    /// Borrow the exact original arena capability without a new allocation.
    pub fn allocation_pool(&self) -> &HeaderFieldAllocationPool {
        &self.allocation
    }

    pub(crate) fn bind(&self) -> io::Result<Self> {
        self.allocation.try_bind_once()?;
        Ok(self.clone())
    }

    pub(crate) fn try_fill(
        &self,
        len: usize,
        fill: impl FnOnce(&mut [u8]) -> Result<(), DecoderError>,
    ) -> Result<Bytes, DecoderError> {
        self.allocation
            .try_fill(len, fill)
            .map_err(|error| match error {
                HeaderFieldFillError::TooLarge => DecoderError::HeaderFieldTooLarge,
                HeaderFieldFillError::Exhausted => DecoderError::HeaderFieldPoolExhausted,
                HeaderFieldFillError::Fill(error) => error,
            })
    }
}

impl fmt::Debug for ReceiveHeaderFieldPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveHeaderFieldPool")
            .field("capacity_bytes", &self.capacity_bytes())
            .field("field_positions", &self.field_positions())
            .field("max_field_bytes", &self.max_field_bytes())
            .finish_non_exhaustive()
    }
}
