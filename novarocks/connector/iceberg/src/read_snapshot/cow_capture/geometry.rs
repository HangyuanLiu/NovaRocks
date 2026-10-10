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

// Private read_snapshot child. Requested-layout upper, not SDK input backing.
use std::mem::{align_of, size_of};

pub(crate) enum Failure<E> {
    Original(E),
    Overflow,
    ReceiptExceeded,
}
impl<E> std::fmt::Debug for Failure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Original(_) => "CowCapture::Original",
            Self::Overflow => "CowCapture::Overflow",
            Self::ReceiptExceeded => "CowCapture::ReceiptExceeded",
        })
    }
}
impl<E> std::fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl<E: std::error::Error + 'static> std::error::Error for Failure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Self::Original(e) = self {
            Some(e)
        } else {
            None
        }
    }
}
pub(crate) fn add<E>(a: u64, b: u64) -> Result<u64, Failure<E>> {
    a.checked_add(b).ok_or(Failure::Overflow)
}
pub(crate) fn mul<E>(a: u64, b: u64) -> Result<u64, Failure<E>> {
    a.checked_mul(b).ok_or(Failure::Overflow)
}
pub(crate) fn slots<E, T>(n: usize) -> Result<u64, Failure<E>> {
    mul(n as u64, size_of::<T>() as u64)
}
pub(crate) fn arc_slice<E, T>(n: usize) -> Result<u64, Failure<E>> {
    let a = align_of::<T>().max(align_of::<usize>()) as u64;
    let h = add(2 * size_of::<usize>() as u64, a - 1)? / a * a;
    let z = add(h, slots::<E, T>(n)?)?;
    Ok(add(z, a - 1)? / a * a)
}
pub(crate) fn arc_bytes<E>(n: usize) -> Result<u64, Failure<E>> {
    arc_slice::<E, u8>(n)
}
// All pinned hashbrown 0.17.1 SIMD/generic groups on the audited targets are
// at most 16 bytes. This is layout geometry, not a runtime admission cap.
const GROUP_MAX: usize = 16;
fn buckets_for<E, T>(n: usize) -> Result<usize, Failure<E>> {
    if n == 0 {
        return Ok(0);
    }
    if n < 15 {
        let min = match size_of::<T>() {
            0..=1 => 14,
            2..=3 => 7,
            _ => 3,
        };
        return Ok(match n.max(min) {
            0..=3 => 4,
            4..=7 => 8,
            _ => 16,
        });
    }
    let adjusted = n.checked_mul(8).ok_or(Failure::Overflow)? / 7;
    adjusted
        .checked_next_power_of_two()
        .ok_or(Failure::Overflow)
}
fn layout<E, T>(b: usize) -> Result<u64, Failure<E>> {
    if b == 0 {
        return Ok(0);
    }
    let a = align_of::<T>().max(GROUP_MAX) as u64;
    let raw = slots::<E, T>(b)?;
    let ctrl = add(raw, a - 1)? / a * a;
    let len = add(ctrl, add(b as u64, GROUP_MAX as u64)?)?;
    if len > isize::MAX as u64 - (a - 1) {
        return Err(Failure::Overflow);
    }
    Ok(len)
}
pub(crate) fn hash_max<E, T>(hint: usize) -> Result<u64, Failure<E>> {
    layout::<E, T>(buckets_for::<E, T>(hint)?)
}
pub(crate) fn hash_actual<E, T>(public_capacity: usize) -> Result<u64, Failure<E>> {
    let b = if public_capacity == 0 {
        0
    } else if public_capacity < 8 {
        if public_capacity != 3 && public_capacity != 7 {
            return Err(Failure::ReceiptExceeded);
        }
        public_capacity.checked_add(1).ok_or(Failure::Overflow)?
    } else {
        if public_capacity % 7 != 0 {
            return Err(Failure::ReceiptExceeded);
        }
        public_capacity
            .checked_div(7)
            .and_then(|n| n.checked_mul(8))
            .ok_or(Failure::Overflow)?
    };
    if b != 0 && !b.is_power_of_two() {
        return Err(Failure::ReceiptExceeded);
    }
    layout::<E, T>(b)
}
// Pinned std B=6: parent + two u16 + keys[11] + vals[11]; internal
// edges[12]. Summed per-field padding safely covers unspecified field order.
pub(crate) fn btree_node<E, K, V>() -> Result<u64, Failure<E>> {
    let a = align_of::<K>()
        .max(align_of::<V>())
        .max(align_of::<usize>()) as u64;
    let mut n = add(size_of::<usize>() as u64, 4)?;
    n = add(n, slots::<E, K>(11)?)?;
    n = add(n, slots::<E, V>(11)?)?;
    n = add(n, slots::<E, usize>(12)?)?;
    add(n, mul(9, a - 1)?)
}
pub(crate) fn btree_construct<E, K, V>(items: usize) -> Result<u64, Failure<E>> {
    if items == 0 {
        Ok(0)
    } else {
        mul(add(mul(2, items as u64)?, 1)?, btree_node::<E, K, V>()?)
    }
}
pub(crate) fn btree_retained<E, K, V>(items: usize) -> Result<u64, Failure<E>> {
    mul(items as u64, btree_node::<E, K, V>()?)
}
// format! starts with estimated literal capacity and grows geometrically.
// All audited formatters here have no owned internal scratch. Original String
// output is retained; this bounds its requested capacity rather than its len.
pub(crate) fn format_capacity<E>(length_upper: u64) -> Result<u64, Failure<E>> {
    Ok(mul(2, length_upper)?.max(8))
}

// Pinned driftsort: <=20 uses insertion sort; otherwise max(n,48) covers
// max(n-n/2,min(n,8MB/sizeofT),SMALL_SORT_GENERAL_SCRATCH_LEN=48).
// This safely charges heap even when the pinned 4KiB stack buffer is sufficient.
pub(crate) fn stable_sort_scratch<E, T>(n: usize) -> Result<u64, Failure<E>> {
    if n <= 20 {
        Ok(0)
    } else {
        slots::<E, T>(n.max(48))
    }
}
