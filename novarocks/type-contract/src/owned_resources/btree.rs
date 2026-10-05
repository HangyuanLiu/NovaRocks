// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One locked Rust BTree node request bound for the actual key/value layouts.
//! Private Rust field order is not mirrored. Insertion-only cumulative bounds
//! cover opaque library movement, not measured retained capacity, cooperative
//! internal callbacks or a host allocation grant.

use super::profile::LOCKED_TOOLCHAIN;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BTreeResourceError {
    SourceModel(&'static str),
    Arithmetic(&'static str),
}
impl BTreeResourceError {
    pub const fn message(self) -> &'static str {
        match self {
            Self::SourceModel(message) | Self::Arithmetic(message) => message,
        }
    }
}

/// Conservative lookup work for the locked B=6 source. Even binary fanout
/// bounds visited levels by bit-length+1; sixteen units cover each node's
/// keys, edges and header. Allocation/insert movement is accounted separately.
pub fn lookup_work(entries: usize) -> Result<usize, &'static str> {
    lookup_work_typed(entries).map_err(BTreeResourceError::message)
}

pub fn lookup_work_typed(entries: usize) -> Result<usize, BTreeResourceError> {
    let levels = usize::try_from(usize::BITS - entries.leading_zeros()).map_err(|_| {
        BTreeResourceError::Arithmetic("expression lookup depth is unrepresentable")
    })?;
    levels
        .checked_add(1)
        .and_then(|n| n.checked_mul(16))
        .ok_or(BTreeResourceError::Arithmetic(
            "expression resource product overflow",
        ))
}

pub fn node_layout<K, V>() -> Result<Layout, &'static str> {
    node_layout_typed::<K, V>().map_err(BTreeResourceError::message)
}

pub fn node_layout_typed<K, V>() -> Result<Layout, BTreeResourceError> {
    if !LOCKED_TOOLCHAIN {
        return Err(BTreeResourceError::SourceModel(
            "BTree allocation source model drift",
        ));
    }
    let key = Layout::new::<K>();
    let value = Layout::new::<V>();
    let pointer = Layout::new::<Option<std::ptr::NonNull<()>>>();
    // Both existing consumers used the target's usize layout. Require that
    // it is the actual thin parent/edge layout before retaining their bound.
    if pointer != Layout::new::<usize>() || pointer.align() < Layout::new::<u16>().align() {
        return Err(BTreeResourceError::SourceModel(
            "BTree node target layout source model drift",
        ));
    }
    let align = key.align().max(value.align()).max(pointer.align());
    let add = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(BTreeResourceError::Arithmetic("BTree node size overflow"))
    };
    let mul = |a: usize, b: usize| {
        a.checked_mul(b)
            .ok_or(BTreeResourceError::Arithmetic("BTree node size overflow"))
    };
    // Rust 1.92 alloc/btree/node.rs: B=6, eleven keys/values, twelve
    // edges. LeafNode contains five members: parent pointer, two u16,
    // key array and value array. Each member can add at most align-1
    // padding for any private field order. InternalNode appends edges.
    let members = add(
        add(pointer.size(), 2 * std::mem::size_of::<u16>())?,
        mul(11, add(key.size(), value.size())?)?,
    )?;
    let leaf = add(members, mul(5, align - 1)?)?;
    let internal = add(add(leaf, mul(12, pointer.size())?)?, align - 1)?;
    Layout::from_size_align(internal, align)
        .map(|layout| layout.pad_to_align())
        .map_err(|_| BTreeResourceError::Arithmetic("BTree node layout is unrepresentable"))
}

/// The insertion-only source never removes or rebuilds nodes: at most one
/// retained node request per entry. These are cumulative request/work bounds,
/// not a measurement or necessary floor of the final private tree backing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InsertionOnlyFacts {
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

pub fn insertion_only<K, V>(entries: usize) -> Result<InsertionOnlyFacts, BTreeResourceError> {
    let node = node_layout_typed::<K, V>()?;
    let lookup = lookup_work_typed(entries)?;
    let request_bytes_upper_bound =
        entries
            .checked_mul(node.size())
            .ok_or(BTreeResourceError::Arithmetic(
                "BTree insertion product overflow",
            ))?;
    let cumulative_work_upper_bound = traversal_work(entries, node, lookup)?;
    Ok(InsertionOnlyFacts {
        allocation_requests_upper_bound: entries,
        request_bytes_upper_bound,
        cumulative_work_upper_bound,
    })
}

/// The locked retain/extract traversal visits every original entry once. Each
/// removal may repair at most the original height's nodes and parent links;
/// eight node extents per level cover rotations, merging and root repair.
/// This is work only: retaining an owned tree requests no new tree backing.
pub fn retain_work<K, V>(entries: usize) -> Result<usize, BTreeResourceError> {
    traversal_work(
        entries,
        node_layout_typed::<K, V>()?,
        lookup_work_typed(entries)?,
    )
}

fn traversal_work(
    entries: usize,
    node: Layout,
    lookup: usize,
) -> Result<usize, BTreeResourceError> {
    let mul = |a: usize, b: usize| {
        a.checked_mul(b).ok_or(BTreeResourceError::Arithmetic(
            "BTree insertion product overflow",
        ))
    };
    // At every visited B=6 level, eight node-layout byte units cover moves,
    // split/root initialization and parent-link repairs. Search is charged
    // separately for every possible level, not only actual split locations.
    let movement = mul(mul(entries, lookup / 16)?, mul(node.size(), 8)?)?;
    let search = mul(entries, mul(lookup, 4)?)?;
    search
        .checked_add(movement)
        .ok_or(BTreeResourceError::Arithmetic(
            "BTree insertion sum overflow",
        ))
}
