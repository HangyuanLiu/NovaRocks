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

//! The shared locked collection source owner supplies the original bounds.
//! Keep existing callers on the same numerical author and error provenance.

pub(crate) use novarocks_type_contract::owned_resources::btree::*;
#[cfg(test)]
use std::alloc::Layout;

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
