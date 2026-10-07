// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Owned segmented local-result storage. Only a tail block can grow; a
//! replacement copies at most 64 KiB, never the accumulated column. Buffer
//! capacities are explicit and checked before reserve/copy, and Arrow receives
//! compact buffers after the source conversion workspace has retired.

const BLOCK_BYTES: usize = 64 * 1024;

pub(super) struct LocalBuffer<T> {
    blocks: Vec<Vec<T>>,
    len: usize,
    capacity: usize,
    leading_metadata_bytes: usize,
}

impl<T: Copy> LocalBuffer<T> {
    pub(super) fn new(leading_metadata_bytes: usize) -> Self {
        Self {
            blocks: Vec::new(),
            len: 0,
            capacity: 0,
            leading_metadata_bytes,
        }
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn capacity_bytes(&self) -> usize {
        self.capacity * size_of::<T>()
    }
    fn tail_metadata_bytes(&self) -> usize {
        if self.blocks.len() == 1 {
            self.leading_metadata_bytes
        } else {
            0
        }
    }
    pub(super) fn maximum_tail_bytes(&self) -> usize {
        self.blocks
            .last()
            .map_or(0, |block| block.capacity() * size_of::<T>())
            .saturating_sub(self.tail_metadata_bytes())
    }
    pub(super) fn missing_bytes(&self, additional: usize) -> usize {
        let free = self
            .blocks
            .last()
            .map_or(0, |block| block.capacity() - block.len());
        additional.saturating_sub(free) * size_of::<T>()
    }

    /// Source plus the copied live tail fits the workspace: tail length is a
    /// subset of the previously admitted cells, and incoming cells were
    /// checked against the same whole logical bound before this call.
    pub(super) fn compact_tail(&mut self, growth: &mut Growth) -> Result<(), String> {
        let metadata = self.tail_metadata_bytes();
        if let Some(tail) = self.blocks.last_mut() {
            if tail.capacity() > tail.len() {
                let old = tail.capacity();
                growth.preflight_copy(tail.len() * size_of::<T>(), metadata)?;
                let mut compact = Vec::new();
                compact
                    .try_reserve_exact(tail.len())
                    .map_err(|_| allocation_error())?;
                compact.extend_from_slice(tail);
                *tail = compact;
                self.capacity -= old - tail.capacity();
            }
        }
        Ok(())
    }

    pub(super) fn append(&mut self, mut values: &[T], growth: &mut Growth) -> Result<(), String> {
        let maximum = BLOCK_BYTES / size_of::<T>();
        while !values.is_empty() {
            if self
                .blocks
                .last()
                .is_none_or(|block| block.len() == maximum)
            {
                // Every completed block is full. At most one tail per column
                // buffer has unused capacity; metadata therefore has a fixed
                // bound from columns plus collector_bytes / BLOCK_BYTES.
                if self.blocks.len() == self.blocks.capacity() {
                    self.blocks
                        .try_reserve_exact(1)
                        .map_err(|_| allocation_error())?;
                }
                self.blocks.push(Vec::new());
            }
            let metadata = self.tail_metadata_bytes();
            let tail = self.blocks.last_mut().expect("owned local buffer tail");
            let append = values.len().min(maximum - tail.len());
            let required = tail.len() + append;
            if required > tail.capacity() {
                let old = tail.capacity();
                let desired = old.saturating_mul(2).max(8).max(required).min(maximum);
                let capacity = growth.choose(old, required, desired, size_of::<T>());
                growth.preflight_copy(old * size_of::<T>(), metadata)?;
                // The old tail remains workspace until actual reserve exits.
                // It is at most BLOCK_BYTES; its peak was admitted by the row.
                tail.try_reserve_exact(capacity - tail.len())
                    .map_err(|_| allocation_error())?;
                self.capacity += tail.capacity() - old;
                if tail.capacity() != capacity {
                    return Err(
                        "local result buffer allocator returned an unexpected capacity".into(),
                    );
                }
            }
            tail.extend_from_slice(&values[..append]);
            self.len += append;
            values = &values[append..];
        }
        Ok(())
    }

    pub(super) fn last_mut(&mut self) -> Option<&mut T> {
        self.blocks.last_mut()?.last_mut()
    }

    pub(super) fn into_compact(self) -> Result<Vec<T>, String> {
        if self.blocks.len() == 1 {
            let mut block = self.blocks.into_iter().next().expect("one buffer block");
            // Moving the tail preserves capacity within the collector bound;
            // compacting is unnecessary when this is already one Arrow buffer.
            block.truncate(self.len);
            return Ok(block);
        }
        let mut compact = Vec::new();
        compact
            .try_reserve_exact(self.len)
            .map_err(|_| allocation_error())?;
        for block in self.blocks {
            compact.extend_from_slice(&block);
        }
        Ok(compact)
    }
}

/// All mandatory growth for the whole row is pre-admitted. Amortized slack
/// may consume only the remaining collector bytes, without stealing another
/// column's mandatory growth. This owner never obtains capacity mid-stream.
pub(super) struct Growth {
    extra_bytes: usize,
    incoming_bytes: usize,
    workspace_bytes: usize,
    #[cfg(test)]
    pub(super) maximum_copy_bytes: usize,
}

impl Growth {
    pub(super) fn new(extra_bytes: usize, incoming_bytes: usize, workspace_bytes: usize) -> Self {
        Self {
            extra_bytes,
            incoming_bytes,
            workspace_bytes,
            #[cfg(test)]
            maximum_copy_bytes: 0,
        }
    }
    pub(super) fn grant_extra(&mut self, extra_bytes: usize) {
        self.extra_bytes = extra_bytes;
    }
    fn preflight_copy(&mut self, bytes: usize, fixed_metadata: usize) -> Result<(), String> {
        // The leading offset/header bytes live in the root metadata share.
        let payload = bytes.saturating_sub(fixed_metadata);
        if payload
            .checked_add(self.incoming_bytes)
            .is_none_or(|n| n > self.workspace_bytes)
        {
            return Err("local result growth exceeds its conversion workspace bound".into());
        }
        #[cfg(test)]
        {
            self.maximum_copy_bytes = self.maximum_copy_bytes.max(payload);
        }
        Ok(())
    }
    fn choose(&mut self, old: usize, required: usize, desired: usize, unit: usize) -> usize {
        let extra = (desired - required).min(self.extra_bytes / unit);
        self.extra_bytes -= extra * unit;
        debug_assert!(required > old);
        required + extra
    }
}

fn allocation_error() -> String {
    "local result buffer allocation failed".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_denied_growth_preserves_the_live_buffer_before_reserve_or_copy() {
        let mut buffer = LocalBuffer::<u8>::new(0);
        buffer.append(b"abc", &mut Growth::new(5, 3, 32)).unwrap();
        assert_eq!(buffer.capacity_bytes(), 8);
        let mut denied = Growth::new(8, 32, 32);
        assert!(
            buffer
                .append(b"012345678", &mut denied)
                .unwrap_err()
                .contains("workspace")
        );
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity_bytes(), 8);
        assert_eq!(buffer.into_compact().unwrap(), b"abc");
    }

    #[test]
    fn a_single_buffer_moves_to_arrow_without_allocating_another_payload() {
        let mut buffer = LocalBuffer::<u8>::new(0);
        buffer
            .append(b"owned payload", &mut Growth::new(0, 13, 64))
            .unwrap();
        let original = buffer.blocks[0].as_ptr();
        let bytes = buffer.into_compact().unwrap();
        assert_eq!(bytes.as_ptr(), original);
        let arrow = arrow::buffer::Buffer::from(bytes);
        assert_eq!(arrow.as_ptr(), original);
        assert_eq!(arrow.as_slice(), b"owned payload");
    }
}
