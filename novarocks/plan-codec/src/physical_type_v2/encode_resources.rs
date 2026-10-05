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

//! Numerical contributions of the original package Type encoder. The caller
//! owns the meter, source union invoice, actual grammar and allocation scope.
//! This model neither observes synthetic work nor grants host memory.

use super::{PackageTypeProjectionFacts, PackageTypeProjectionLimits, TypeCodecError};
use crate::{
    btree_resources_v2 as btree,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{
    CompileControlError, MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_METADATA_ENTRIES,
    MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, MAX_VALUE_TYPE_NODES,
    owned_resources::hashmap,
};
use std::{alloc::Layout, sync::Arc};

type E = TypeCodecError;
fn add(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_mul(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn array<T>(n: usize) -> Result<Layout, E> {
    Layout::array::<T>(n).map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn tree_error(error: btree::BTreeResourceError) -> E {
    match error {
        btree::BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
        btree::BTreeResourceError::SourceModel(message) => E::ResourceSource(message),
    }
}
fn hash_error(error: hashmap::HashMapResourceError) -> E {
    match error {
        hashmap::HashMapResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
        hashmap::HashMapResourceError::SourceModel(message) => E::ResourceSource(message),
    }
}

pub(super) struct Model {
    source: usize,
    definitions: usize,
    expanded: usize,
    strings: usize,
    requests: usize,
    bytes: usize,
    work: usize,
}
impl Model {
    fn request(&mut self, layout: Layout, count: usize) -> Result<(), E> {
        if layout.size() == 0 || count == 0 {
            return Ok(());
        }
        let bytes = mul(layout.size(), count)?;
        self.requests = add(self.requests, count)?;
        self.bytes = add(self.bytes, bytes)?;
        // Initialization/copy/movement and abort cleanup of real requested
        // buffers. Opaque library internals do not acquire callbacks here.
        self.work = add(self.work, add(mul(bytes, 4)?, mul(count, 128)?)?)?;
        Ok(())
    }
    fn tree(&mut self, count: usize) -> Result<(), E> {
        let facts = btree::insertion_only::<u32, ()>(count).map_err(tree_error)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.bytes = add(self.bytes, facts.request_bytes_upper_bound)?;
        self.work = add(
            self.work,
            add(
                facts.cumulative_work_upper_bound,
                mul(facts.request_bytes_upper_bound, 4)?,
            )?,
        )?;
        Ok(())
    }
    /// Before the first source walk: the two actual ID sets, every strict
    /// validator's original heap stack, and count/emission bookkeeping are
    /// admitted. Writer sources must be the caller's immutable checked Drafts;
    /// root counts confer no Writer law or source proof of their own.
    pub(super) fn new(
        source: usize,
        value_roots: usize,
        strict_field_roots: usize,
        writer_sources: usize,
        writer_fields: usize,
        limits: PackageTypeProjectionLimits,
    ) -> Result<Self, E> {
        if !LOCKED_TOOLCHAIN || !LOCKED_FAMILY {
            return Err(E::ResourceSource("type encoder library source model drift"));
        }
        // Borrowed root slices may alias/share allocation, so their necessary
        // payload floors are combined by max, never summed into a fake union.
        let floor = array::<(u32, novarocks_type_contract::FunctionValueType)>(value_roots)?
            .size()
            .max(array::<(u32, Arc<Field>)>(strict_field_roots)?.size())
            .max(array::<super::WriterTypeSource<'_>>(writer_sources)?.size())
            .max(array::<u32>(writer_fields)?.size());
        if source < floor {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let field_roots = add(strict_field_roots, writer_fields)?;
        let strict_roots = add(value_roots, strict_field_roots)?;
        let roots = add(value_roots, field_roots)?;
        if roots > limits.max_definitions {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let mut model = Self {
            source,
            definitions: 0,
            expanded: 0,
            strings: 0,
            requests: 0,
            bytes: 0,
            work: 256,
        };
        model.tree(value_roots)?;
        model.tree(field_roots)?;
        // Rust1.92 Vec starts with the original singleton and grows at least
        // four/doubling. Per strict root there are <=N requests and <=4N
        // cumulative tuple payload bytes. No second datatype walk is made.
        let stack_nodes = mul(strict_roots, MAX_VALUE_TYPE_NODES)?;
        let stack = array::<(&DataType, usize)>(mul(MAX_VALUE_TYPE_NODES, 4)?)?;
        let stack_bytes = mul(stack.size(), strict_roots)?;
        model.requests = add(model.requests, stack_nodes)?;
        model.bytes = add(model.bytes, stack_bytes)?;
        model.work = add(
            model.work,
            add(mul(stack_bytes, 4)?, mul(stack_nodes, 128)?)?,
        )?;
        // Strict validation precedes count_field: its Field metadata source
        // iteration and bounded byte walks must already be admitted too.
        let iteration =
            hashmap::source_iterator_work_upper_bound(source, MAX_ARROW_FIELD_METADATA_ENTRIES)
                .map_err(hash_error)?;
        let strict_bytes = add(
            add(MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_NAME_BYTES)?,
            MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES,
        )?;
        model.work = add(
            model.work,
            mul(
                stack_nodes,
                add(iteration, add(1024, mul(strict_bytes, 8)?)?)?,
            )?,
        )?;
        // Existing count expansion terminates at E. Every emitted C/F/V has
        // at least one expanded node. FieldIds' monotone cursor tests at most
        // root Field IDs plus automatic Field occurrences, never max(ID).
        let lookup = btree::lookup_work_typed(add(field_roots, limits.max_expanded_nodes)?)
            .map_err(tree_error)?;
        model.work = add(
            model.work,
            mul(limits.max_expanded_nodes, add(512, mul(lookup, 8)?)?)?,
        )?;
        model.work = add(
            model.work,
            add(
                mul(roots, add(256, mul(lookup, 4)?)?)?,
                mul(writer_sources, 256)?,
            )?,
        )?;
        // One terminal parameter diagnostic uses bounded decimal/enum text;
        // locked String growth <=128 requests and <=4*128 payload bytes.
        model.requests = add(model.requests, 128)?;
        model.bytes = add(model.bytes, 512)?;
        model.work = add(model.work, 4 * 512 + 128 * 128)?;
        model.facts(limits)?;
        Ok(model)
    }
    /// Header-only numerical contribution, before any child traversal.
    pub(super) fn carrier(&mut self, ty: &DataType) -> Result<(), E> {
        match ty {
            DataType::Struct(fields) => self.request(array::<u32>(fields.len())?, 1)?,
            DataType::Union(fields, _) => {
                self.request(array::<wire::UnionField>(fields.len())?, 1)?
            }
            DataType::Timestamp(_, Some(zone)) => self.string(zone.len())?,
            _ => {}
        }
        Ok(())
    }
    /// No metadata entry is read here. Admit both real insertion-sort and DTO
    /// buffers, source deleted-bucket iteration, and the worst-case byte
    /// comparisons before the first metadata iterator operation.
    pub(super) fn field(&mut self, field: &Field) -> Result<(), E> {
        let entries = field.metadata().len();
        // Only occupied String handles plus this actual owned Field/name.
        // Deleted buckets and private table capacity remain the caller's B.
        let floor = add(
            add(std::mem::size_of::<Field>(), field.name().len())?,
            array::<(String, String)>(entries)?.size(),
        )?;
        if self.source < floor {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        self.string(field.name().len())?;
        self.request(array::<(&str, &str)>(entries)?, 1)?;
        self.request(array::<plan::ArrowFieldMetadataEntry>(entries)?, 1)?;
        // The original Count walk and encode_metadata each iterate the
        // same source map once. Strict pre-validation is admitted in new().
        self.work = add(
            self.work,
            mul(
                hashmap::source_iterator_work_upper_bound(self.source, entries)
                    .map_err(hash_error)?,
                2,
            )?,
        )?;
        let pairs = mul(entries, entries.saturating_sub(1))? / 2;
        // At most B equal-prefix bytes, one length/order comparison and one
        // swap per pair. B is the truthful union, not len()/capacity().
        self.work = add(self.work, mul(pairs, add(self.source, 2)?)?)?;
        self.work = add(self.work, mul(entries, 128)?)?;
        Ok(())
    }
    /// Exactly one actual emitted String request. Metadata callers use this
    /// once for key and once for value; name and zone are already included.
    pub(super) fn string(&mut self, length: usize) -> Result<(), E> {
        if length > self.source {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        self.strings = add(self.strings, length)?;
        self.request(array::<u8>(length)?, 1)?;
        self.work = add(self.work, add(64, mul(length, 8)?)?)?;
        Ok(())
    }
    pub(super) fn definition(&mut self, count: usize) -> Result<(), CompileControlError> {
        self.definitions = add(self.definitions, count)?;
        Ok(())
    }
    pub(super) fn expansion(&mut self, count: usize) -> Result<(), CompileControlError> {
        self.expanded = add(self.expanded, count)?;
        Ok(())
    }
    /// The actual three DTO Vec requests, after complete count preflight and
    /// before the first output allocation. Nested buffers were counted above.
    pub(super) fn output(
        &mut self,
        carriers: usize,
        fields: usize,
        values: usize,
    ) -> Result<(), E> {
        self.request(array::<wire::CarrierTypeDefinition>(carriers)?, 1)?;
        self.request(array::<wire::FieldDefinition>(fields)?, 1)?;
        self.request(array::<wire::ValueTypeDefinition>(values)?, 1)?;
        Ok(())
    }
    pub(super) fn facts(
        &self,
        limits: PackageTypeProjectionLimits,
    ) -> Result<PackageTypeProjectionFacts, CompileControlError> {
        let facts = PackageTypeProjectionFacts {
            definition_count: self.definitions,
            expanded_node_count: self.expanded,
            string_bytes: self.strings,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: add(self.source, self.bytes)?,
            cumulative_work_upper_bound: self.work,
        };
        if facts.definition_count > limits.max_definitions
            || facts.expanded_node_count > limits.max_expanded_nodes
            || facts.string_bytes > limits.max_string_bytes
            || facts.allocation_requests_upper_bound > limits.max_allocation_requests
            || facts.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes
            || facts.cumulative_work_upper_bound > limits.max_work
        {
            return Err(CompileControlError::ResourceExhausted);
        }
        Ok(facts)
    }
    pub(super) fn gate(
        &self,
        limits: PackageTypeProjectionLimits,
        admit: &mut (
                 impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError> + ?Sized
             ),
    ) -> Result<(), E> {
        let facts = self.facts(limits)?;
        admit(&facts)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    fn limits() -> PackageTypeProjectionLimits {
        PackageTypeProjectionLimits {
            max_definitions: 100_000,
            max_expanded_nodes: 1_000_000,
            max_string_bytes: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        }
    }
    fn empty() -> Model {
        Model::new(65_536, 0, 0, 0, 0, limits()).unwrap()
    }
    fn facts(model: &Model) -> PackageTypeProjectionFacts {
        model.facts(limits()).unwrap()
    }
    fn tree_bytes() -> usize {
        // Independent Rust1.92 B=6 layout bound, including private-order
        // padding. This is a request upper, not retained Set capacity.
        let pointer = std::mem::size_of::<usize>();
        let align = std::mem::align_of::<usize>().max(std::mem::align_of::<u32>());
        let raw = pointer + 4 + 11 * 4 + 5 * (align - 1) + 12 * pointer + align - 1;
        raw.div_ceil(align) * align
    }
    #[test]
    fn initial_id_sets_and_strict_original_stack_have_independent_request_layouts() {
        let model = Model::new(4096, 2, 3, 2, 4, limits()).unwrap();
        let actual = facts(&model);
        let stack = 5 * 4 * MAX_VALUE_TYPE_NODES * std::mem::size_of::<(&DataType, usize)>();
        assert_eq!(actual.definition_count, 0);
        assert_eq!(actual.expanded_node_count, 0);
        assert_eq!(
            actual.allocation_requests_upper_bound,
            2 + 7 + 5 * MAX_VALUE_TYPE_NODES + 128
        );
        assert_eq!(
            actual.allocation_request_bytes_upper_bound,
            9 * tree_bytes() + stack + 512
        );
        assert_eq!(
            actual.coexisting_source_and_request_bytes_upper_bound,
            4096 + actual.allocation_request_bytes_upper_bound
        );
        // Writer-only roots request no strict Value-validator heap stack.
        let writer = facts(&Model::new(4096, 0, 0, 2, 7, limits()).unwrap());
        assert_eq!(writer.allocation_requests_upper_bound, 7 + 128);
        assert_eq!(
            writer.allocation_request_bytes_upper_bound,
            7 * tree_bytes() + 512
        );
    }
    #[test]
    fn actual_carrier_field_metadata_and_three_dto_vecs_have_independent_golden() {
        let mut model = empty();
        let base = facts(&model);
        let fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, true)),
        ];
        model.carrier(&DataType::Struct(fields.into())).unwrap();
        let union = arrow::datatypes::UnionFields::try_new(
            [0, 7],
            [
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int64, true),
            ],
        )
        .unwrap();
        model
            .carrier(&DataType::Union(union, arrow::datatypes::UnionMode::Dense))
            .unwrap();
        model
            .carrier(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some(Arc::from("中国/é")),
            ))
            .unwrap();
        let field = Field::new("名\0", DataType::Int64, true).with_metadata(HashMap::from([
            ("é".into(), "中国\0".into()),
            ("z".into(), "".into()),
        ]));
        model.field(&field).unwrap();
        for (key, value) in field.metadata() {
            model.string(key.len()).unwrap();
            model.string(value.len()).unwrap();
        }
        model.definition(6).unwrap();
        model.expansion(9).unwrap();
        model.output(3, 2, 1).unwrap();
        let actual = facts(&model);
        let strings = "中国/é".len() + "名\0".len() + "é".len() + "中国\0".len() + 1;
        let payload = 2 * std::mem::size_of::<u32>()
            + 2 * std::mem::size_of::<wire::UnionField>()
            + 2 * std::mem::size_of::<(&str, &str)>()
            + 2 * std::mem::size_of::<plan::ArrowFieldMetadataEntry>()
            + strings
            + 3 * std::mem::size_of::<wire::CarrierTypeDefinition>()
            + 2 * std::mem::size_of::<wire::FieldDefinition>()
            + std::mem::size_of::<wire::ValueTypeDefinition>();
        assert_eq!(actual.definition_count, 6);
        assert_eq!(actual.expanded_node_count, 9);
        assert_eq!(actual.string_bytes, strings);
        assert_eq!(
            actual.allocation_requests_upper_bound - base.allocation_requests_upper_bound,
            12
        );
        assert_eq!(
            actual.allocation_request_bytes_upper_bound - base.allocation_request_bytes_upper_bound,
            payload
        );
        assert_eq!(
            actual.coexisting_source_and_request_bytes_upper_bound,
            65_536 + actual.allocation_request_bytes_upper_bound
        );
    }
    #[test]
    fn empty_payloads_have_no_nonzero_request_and_counts_never_reset_early_work() {
        let mut model = empty();
        let baseline = facts(&model);
        model
            .carrier(&DataType::Struct(Vec::<Arc<Field>>::new().into()))
            .unwrap();
        model
            .field(&Field::new("", DataType::Int32, false))
            .unwrap();
        model.string(0).unwrap();
        model.output(0, 0, 0).unwrap();
        let actual = facts(&model);
        assert_eq!(
            actual.allocation_requests_upper_bound,
            baseline.allocation_requests_upper_bound
        );
        assert_eq!(
            actual.allocation_request_bytes_upper_bound,
            baseline.allocation_request_bytes_upper_bound
        );
        assert_eq!(actual.string_bytes, 0);
        assert!(actual.cumulative_work_upper_bound >= baseline.cumulative_work_upper_bound);
        model.definition(2).unwrap();
        model.expansion(3).unwrap();
        assert_eq!(
            facts(&model).cumulative_work_upper_bound,
            actual.cumulative_work_upper_bound
        );
    }
    #[test]
    fn all_seven_known_axes_refuse_before_caller_admission_callback_and_replay_exactly() {
        let mut model = Model::new(128, 0, 0, 0, 1, limits()).unwrap();
        model.definition(2).unwrap();
        model.expansion(3).unwrap();
        model.string(7).unwrap();
        model.output(1, 1, 0).unwrap();
        let expected = facts(&model);
        let exact = PackageTypeProjectionLimits {
            max_definitions: expected.definition_count,
            max_expanded_nodes: expected.expanded_node_count,
            max_string_bytes: expected.string_bytes,
            max_allocation_requests: expected.allocation_requests_upper_bound,
            max_allocation_request_bytes: expected.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: expected
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: expected.cumulative_work_upper_bound,
        };
        let mut calls = 0;
        model
            .gate(exact, &mut |f| {
                calls += 1;
                assert_eq!(*f, expected);
                Ok(())
            })
            .unwrap();
        assert_eq!(calls, 1);
        for axis in 0..7 {
            let mut under = exact;
            match axis {
                0 => under.max_definitions -= 1,
                1 => under.max_expanded_nodes -= 1,
                2 => under.max_string_bytes -= 1,
                3 => under.max_allocation_requests -= 1,
                4 => under.max_allocation_request_bytes -= 1,
                5 => under.max_coexisting_source_and_request_bytes -= 1,
                _ => under.max_work -= 1,
            }
            assert!(matches!(
                model.gate(under, &mut |_| {
                    calls += 1;
                    Ok(())
                }),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
        }
        assert_eq!(calls, 1);
    }
    struct Late {
        cause: CompileControlError,
        trace: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for Late {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            if trace.len() == 2 {
                Err(self.cause)
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn known_numeric_refusal_wins_real_pending255_before_every_late_primary_control() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Late {
                cause,
                trace: Mutex::new(Vec::new()),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            // Real caller work, before this pure numerical component. This
            // seam is not a complete source encoder/path assertion.
            let mut destination = [0u8; 255];
            for (i, byte) in destination.iter_mut().enumerate() {
                *byte = i as u8;
                work.step().unwrap();
            }
            assert_eq!(destination[254], 254);
            let mut bound = limits();
            bound.max_allocation_requests = 127;
            assert!(matches!(
                Model::new(0, 0, 0, 0, 0, bound),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*control.trace.lock().unwrap(), vec![0]);
        }
    }
    #[test]
    fn source_deleted_bucket_and_equal_prefix_work_is_admitted_without_metadata_scan() {
        let field = Field::new("", DataType::Int32, false).with_metadata(HashMap::from([
            ("same-prefix-a".into(), "".into()),
            ("same-prefix-b".into(), "".into()),
            ("same-prefix-c".into(), "".into()),
        ]));
        let mut low = Model::new(1024, 0, 0, 0, 0, limits()).unwrap();
        let mut high = Model::new(4096, 0, 0, 0, 0, limits()).unwrap();
        low.field(&field).unwrap();
        high.field(&field).unwrap();
        // Three insertion-sort pairs + two locked iterators'32 units per source
        // byte. Input/source invoice is not a map private-capacity estimate.
        assert_eq!(
            facts(&high).cumulative_work_upper_bound - facts(&low).cumulative_work_upper_bound,
            (4096 - 1024) * (32 * 2 + 3)
        );
        assert_eq!(
            facts(&high).allocation_request_bytes_upper_bound,
            facts(&low).allocation_request_bytes_upper_bound
        );
        assert_eq!(facts(&high).string_bytes, 0);
    }
    #[test]
    fn checked_products_and_known_root_counts_fail_as_numeric_controls() {
        let mut small = limits();
        small.max_definitions = 1;
        assert!(matches!(
            Model::new(0, 1, 0, 0, 1, small),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            Model::new(0, usize::MAX, 1, 0, 0, limits()),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let mut model = empty();
        assert!(matches!(
            model.string(usize::MAX),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
    #[test]
    fn necessary_source_floors_use_shared_root_max_and_real_occupied_field_handles() {
        let root_floor = std::mem::size_of::<(u32, novarocks_type_contract::FunctionValueType)>()
            .max(2 * std::mem::size_of::<(u32, Arc<Field>)>())
            .max(std::mem::size_of::<super::super::WriterTypeSource<'_>>())
            .max(3 * std::mem::size_of::<u32>());
        assert!(Model::new(root_floor, 1, 2, 1, 3, limits()).is_ok());
        assert!(matches!(
            Model::new(root_floor - 1, 1, 2, 1, 3, limits()),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let field = Field::new("名", DataType::Int32, false)
            .with_metadata(HashMap::from([("k".into(), "v".into())]));
        let occupied_floor =
            std::mem::size_of::<Field>() + "名".len() + std::mem::size_of::<(String, String)>();
        let mut exact = Model::new(occupied_floor, 0, 0, 0, 0, limits()).unwrap();
        exact.field(&field).unwrap();
        let mut under = Model::new(occupied_floor - 1, 0, 0, 0, 0, limits()).unwrap();
        let before = facts(&under);
        assert!(matches!(
            under.field(&field),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(facts(&under), before);
        assert!(matches!(
            under.string(occupied_floor),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            under.carrier(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Second,
                Some(Arc::from("x".repeat(occupied_floor)))
            )),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
}
