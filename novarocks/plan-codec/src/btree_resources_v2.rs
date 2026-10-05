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

use crate::resource_source_model::LOCKED_TOOLCHAIN;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BTreeResourceError {
    SourceModel(&'static str),
    Arithmetic(&'static str),
}
impl BTreeResourceError {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::SourceModel(message) | Self::Arithmetic(message) => message,
        }
    }
}

/// Conservative lookup work for the locked B=6 source. Even binary fanout
/// bounds visited levels by bit-length+1; sixteen units cover each node's
/// keys, edges and header. Allocation/insert movement is accounted separately.
pub(crate) fn lookup_work(entries: usize) -> Result<usize, &'static str> {
    lookup_work_typed(entries).map_err(BTreeResourceError::message)
}

pub(crate) fn lookup_work_typed(entries: usize) -> Result<usize, BTreeResourceError> {
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

pub(crate) fn node_layout<K, V>() -> Result<Layout, &'static str> {
    node_layout_typed::<K, V>().map_err(BTreeResourceError::message)
}

pub(crate) fn node_layout_typed<K, V>() -> Result<Layout, BTreeResourceError> {
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
pub(crate) struct InsertionOnlyFacts {
    pub(crate) allocation_requests_upper_bound: usize,
    pub(crate) request_bytes_upper_bound: usize,
    pub(crate) cumulative_work_upper_bound: usize,
}

pub(crate) fn insertion_only<K, V>(
    entries: usize,
) -> Result<InsertionOnlyFacts, BTreeResourceError> {
    let node = node_layout_typed::<K, V>()?;
    let lookup = lookup_work_typed(entries)?;
    let mul = |a: usize, b: usize| {
        a.checked_mul(b).ok_or(BTreeResourceError::Arithmetic(
            "BTree insertion product overflow",
        ))
    };
    let request_bytes_upper_bound = mul(entries, node.size())?;
    // At every visited B=6 level, eight node-layout byte units cover moves,
    // split/root initialization and parent-link repairs. Search is charged
    // separately for every possible level, not only actual split locations.
    let movement = mul(mul(entries, lookup / 16)?, mul(node.size(), 8)?)?;
    let search = mul(entries, mul(lookup, 4)?)?;
    let cumulative_work_upper_bound =
        search
            .checked_add(movement)
            .ok_or(BTreeResourceError::Arithmetic(
                "BTree insertion sum overflow",
            ))?;
    Ok(InsertionOnlyFacts {
        allocation_requests_upper_bound: entries,
        request_bytes_upper_bound,
        cumulative_work_upper_bound,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(align(64))]
    struct WideKey([u8; 65]);
    #[repr(align(128))]
    struct AlignedZero;

    #[test]
    fn insertion_only_bounds_charge_all_requests_and_opaque_levels_before_allocation() {
        // Independent small target-layout invoice, including worst-case
        // padding for every private field order rather than a packed leaf.
        let pointer = std::mem::size_of::<usize>();
        let align = std::mem::align_of::<u64>().max(std::mem::align_of::<usize>());
        let unpadded = pointer + 4 + 11 * (4 + 8) + 5 * (align - 1) + 12 * pointer + (align - 1);
        let node = unpadded.div_ceil(align) * align;
        let four = insertion_only::<u32, u64>(4).unwrap();
        assert_eq!(four.allocation_requests_upper_bound, 4);
        assert_eq!(four.request_bytes_upper_bound, 4 * node);
        // Four possible levels, 64 lookup units per search. Charge eight
        // node extents at every level of every insertion, not split count.
        assert_eq!(
            four.cumulative_work_upper_bound,
            4 * 64 * 4 + 4 * 4 * 8 * node
        );
        let empty = insertion_only::<u32, u64>(0).unwrap();
        assert_eq!(
            empty,
            InsertionOnlyFacts {
                allocation_requests_upper_bound: 0,
                request_bytes_upper_bound: 0,
                cumulative_work_upper_bound: 0
            }
        );
        // Existing ordinary-string consumers keep the same original owner.
        assert_eq!(
            node_layout::<u32, u64>().unwrap(),
            node_layout_typed::<u32, u64>().unwrap()
        );
        assert_eq!(lookup_work(4).unwrap(), lookup_work_typed(4).unwrap());
    }

    #[test]
    fn insertion_only_arithmetic_refusals_have_typed_origin() {
        assert!(matches!(
            insertion_only::<u32, u64>(usize::MAX),
            Err(BTreeResourceError::Arithmetic(_))
        ));
        assert!(matches!(
            insertion_only::<u32, u64>(usize::MAX / 288),
            Err(BTreeResourceError::Arithmetic(_))
        ));
    }

    fn all_private_orders<K, V>() {
        let members = [
            Layout::new::<usize>(),
            Layout::new::<u16>(),
            Layout::new::<u16>(),
            Layout::array::<K>(11).unwrap(),
            Layout::array::<V>(11).unwrap(),
        ];
        let bound = node_layout::<K, V>().unwrap();
        fn visit(members: &[Layout; 5], chosen: u8, leaf: Layout, bound: Layout) {
            if chosen == 31 {
                let leaf = leaf.pad_to_align();
                let edges = Layout::array::<usize>(12).unwrap();
                let (internal, _) = leaf.extend(edges).unwrap();
                let internal = internal.pad_to_align();
                assert!(internal.size() <= bound.size());
                assert!(internal.align() <= bound.align());
                return;
            }
            for (at, member) in members.iter().enumerate() {
                if chosen & (1 << at) == 0 {
                    visit(
                        members,
                        chosen | (1 << at),
                        leaf.extend(*member).unwrap().0,
                        bound,
                    );
                }
            }
        }
        // Exhaust all 120 possible private LeafNode member orders with an
        // independent Layout::extend oracle, including final leaf padding.
        visit(&members, 0, Layout::from_size_align(0, 1).unwrap(), bound);
    }

    #[test]
    fn locked_node_bound_covers_all_private_orders_and_high_alignment() {
        all_private_orders::<u32, novarocks_constant_contract::ConstantPool>();
        all_private_orders::<(usize, usize), ()>();
        all_private_orders::<
            novarocks_type_contract::SemanticParameterId,
            novarocks_type_contract::SemanticParameterValue,
        >();
        all_private_orders::<WideKey, AlignedZero>();
        all_private_orders::<(), ()>();
        // Touch the representative payload so the test carrier has no
        // unused-field suppression that might conceal an accidental change.
        assert_eq!(WideKey([0; 65]).0.len(), 65);
    }
}
