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

use super::{
    ConstantNamespaceProjectionLimits, ConstantWriteProjectionLimits,
    PhysicalConstantCodecError as Error, PreparedConstantRecordWrite,
    binding_resources::preflight_constant_binding_resources, finish, prepare_constant_record_write,
};
use crate::physical_type_v2::EncodedTypeTable;
use arrow::datatypes::Field;
use novarocks_physical_plan::{ConstantPoolId, ConstantPools};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
    arrow_data_types_exact_borrowed_observed, arrow_fields_exact_borrowed_observed,
};
use std::{alloc::Layout, sync::Arc};

/// The sole type-table author binds one pair of IDs per pool in the original
/// ConstantPools canonical iteration order. These are separate namespaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConstantRecordTypeIds {
    pub pool: ConstantPoolId,
    pub value_type_id: u32,
    pub field_id: u32,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantNamespaceWriteFacts {
    pub record_count: usize,
    pub source_retained_bytes: usize,
    pub prepared_storage_request_bytes: usize,
    pub record_storage_request_bytes: usize,
    pub writer_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub binding_work_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}
/// Retaining the immutable table loan prevents changing its DTO between the
/// exact source binding and emission. Source roots and pools also stay borrowed.
pub struct PreparedConstantNamespaceWrite<'pool, 'table, 'control> {
    records: Vec<PreparedConstantRecordWrite<'pool, 'control>>,
    facts: ConstantNamespaceWriteFacts,
    _types: &'table EncodedTypeTable<'table>,
    control: &'control dyn PureCompileControl,
}
impl PreparedConstantNamespaceWrite<'_, '_, '_> {
    pub const fn facts(&self) -> &ConstantNamespaceWriteFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<Vec<wire::IpcConstantPool>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = (|| {
            work.flush()?;
            let mut output = Vec::new();
            let reserved = output.try_reserve_exact(self.records.len());
            crate::allocation_exit_v2::reserve_exit::<Error>(reserved, &mut work)?;
            for record in self.records {
                work.step()?;
                work.flush()?;
                let encoded = record.emit()?;
                work.flush()?;
                output.push(encoded);
                work.step()?;
            }
            Ok(output)
        })();
        finish(work, result)
    }
}
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_add(right)
        .ok_or_else(|| shape("constant namespace writer resource sum overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_mul(right)
        .ok_or_else(|| shape("constant namespace writer resource product overflow"))
}
fn array_bytes<T>(count: usize) -> Result<usize, Error> {
    Layout::array::<T>(count)
        .map(|value| value.size())
        .map_err(|_| shape("constant namespace writer source layout is unrepresentable"))
}
fn cap(
    actual: usize,
    limit: usize,
    message: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    work.step()?;
    if actual > limit {
        return Err(shape(message));
    }
    Ok(())
}
fn check(
    facts: &ConstantNamespaceWriteFacts,
    limits: ConstantNamespaceProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    cap(
        facts.prepared_storage_request_bytes,
        limits.max_preparation_request_bytes,
        "constant namespace writer preparation envelope exceeded",
        work,
    )?;
    cap(
        facts.new_allocation_request_bytes_upper_bound,
        limits.max_new_allocation_request_bytes,
        "constant namespace writer request envelope exceeded",
        work,
    )?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        "constant namespace writer coexistence envelope exceeded",
        work,
    )?;
    cap(
        facts.cumulative_library_work_upper_bound,
        limits.max_cumulative_library_work,
        "constant namespace writer work envelope exceeded",
        work,
    )
}

/// All retained type DTO/root/pool/binding backing belongs in the mandatory
/// source invoice. Existing type/schema scratch stages retain their own prior
/// gates; these aggregate output request facts do not retroactively grant them.
pub fn prepare_constant_namespace_write<'pool, 'table, 'control>(
    pools: &'pool ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &'table EncodedTypeTable<'table>,
    source_retained_bytes: usize,
    record_limits: ConstantWriteProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedConstantNamespaceWrite<'pool, 'table, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let count = pools.entries().len();
        cap(
            count,
            limits.max_records,
            "constant namespace writer count envelope exceeded",
            &mut work,
        )?;
        let lengths_match = bindings.len() == count;
        work.step()?;
        if !lengths_match {
            return Err(shape(
                "constant namespace writer requires one binding per original pool",
            ));
        }
        let storage = Layout::array::<PreparedConstantRecordWrite<'_, '_>>(count)
            .map_err(|_| shape("constant namespace writer preparation layout is unrepresentable"))?
            .size();
        let output = Layout::array::<wire::IpcConstantPool>(count)
            .map_err(|_| shape("constant namespace writer record layout is unrepresentable"))?
            .size();
        let (values, fields) = types.source_counts();
        let lookups = mul(count, add(values, fields)?)?;
        let own_work = add(add(storage, output)?, add(lookups, mul(count, 32)?)?)?;
        let requests = add(storage, output)?;
        let mut facts = ConstantNamespaceWriteFacts {
            record_count: count,
            source_retained_bytes,
            prepared_storage_request_bytes: storage,
            record_storage_request_bytes: output,
            allocation_request_count_upper_bound: if count == 0 { 0 } else { 2 },
            new_allocation_request_bytes_upper_bound: requests,
            coexisting_source_and_request_bytes_upper_bound: add(source_retained_bytes, requests)?,
            cumulative_library_work_upper_bound: own_work,
            ..Default::default()
        };
        check(&facts, limits, &mut work)?;
        // Known retained source lower bounds must use the ORIGINAL invoice.
        // Adding our new prepared storage would mask a missing input charge.
        // This top-level floor is not a complete invoice for nested strings,
        // source spare capacity or deleted metadata buckets; the host owns it.
        let table = types.as_wire();
        let mut source_floor = array_bytes::<ConstantRecordTypeIds>(count)?;
        source_floor = add(
            source_floor,
            array_bytes::<(u32, FunctionValueType)>(values)?,
        )?;
        source_floor = add(source_floor, array_bytes::<(u32, Arc<Field>)>(fields)?)?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>(
                table.carriers.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::physical_type_v2::FieldDefinition>(
                table.fields.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>(
                table.value_types.capacity(),
            )?,
        )?;
        cap(
            source_floor,
            source_retained_bytes,
            "constant namespace writer source invoice excludes type and binding storage",
            &mut work,
        )?;
        for pool in pools.entries().values() {
            let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
                .map_err(|_| {
                    shape("constant namespace writer original retained extent is unrepresentable")
                });
            work.step()?;
            cap(
                retained?,
                source_retained_bytes,
                "constant namespace writer source invoice excludes original pool backing",
                &mut work,
            )?;
        }
        work.flush()?;
        let mut prepared = Vec::new();
        let reserved = prepared.try_reserve_exact(count);
        crate::allocation_exit_v2::reserve_exit::<Error>(reserved, &mut work)?;
        for ((id, pool), binding) in pools.entries().iter().zip(bindings) {
            let id_matches = *id == binding.pool;
            work.step()?;
            if !id_matches {
                return Err(shape(
                    "constant namespace writer binding order differs from original pools",
                ));
            }
            let ty = types
                .value_type_observed(binding.value_type_id, &mut work)?
                .ok_or_else(|| {
                    shape("constant namespace writer references an unknown source value type")
                })?;
            let field = types
                .field_observed(binding.field_id, &mut work)?
                .ok_or_else(|| {
                    shape("constant namespace writer references an unknown source Field")
                })?;
            let flags_match = ty.nullable == pool.value_type().nullable
                && ty.logical_type == pool.value_type().logical_type;
            work.step()?;
            if !flags_match {
                return Err(shape(
                    "constant namespace writer source value type differs from original pool",
                ));
            }
            let live_source = add(source_retained_bytes, storage)?;
            let binding_facts = preflight_constant_binding_resources(
                pool,
                field,
                live_source,
                limits.max_cumulative_library_work - facts.cumulative_library_work_upper_bound,
                &mut work,
            )?;
            facts.binding_work_upper_bound = add(
                facts.binding_work_upper_bound,
                binding_facts.work_upper_bound(),
            )?;
            facts.cumulative_library_work_upper_bound = add(
                facts.cumulative_library_work_upper_bound,
                binding_facts.work_upper_bound(),
            )?;
            check(&facts, limits, &mut work)?;
            work.flush()?;
            let types_match = arrow_data_types_exact_borrowed_observed(
                &pool.value_type().data_type,
                &ty.data_type,
                || work.step().map_err(Error::from),
            )?;
            work.step()?;
            if !types_match {
                return Err(shape(
                    "constant namespace writer source value type differs from original pool",
                ));
            }
            let fields_match = if binding_facts.compare_full_field {
                arrow_fields_exact_borrowed_observed(pool.field(), field, || {
                    work.step().map_err(Error::from)
                })?
            } else {
                true
            };
            work.step()?;
            if !fields_match {
                return Err(shape(
                    "constant namespace writer source Field differs from original pool",
                ));
            }
            work.flush()?;
            let writer = prepare_constant_record_write(
                *id,
                binding.value_type_id,
                binding.field_id,
                pool,
                live_source,
                record_limits,
                work.control(),
            )?;
            work.flush()?;
            let writer_facts = writer.facts();
            facts.writer_request_bytes_upper_bound = add(
                facts.writer_request_bytes_upper_bound,
                writer_facts.new_allocation_request_bytes_upper_bound,
            )?;
            facts.allocation_request_count_upper_bound = add(
                facts.allocation_request_count_upper_bound,
                writer_facts.allocation_request_count_upper_bound,
            )?;
            facts.cumulative_library_work_upper_bound = add(
                facts.cumulative_library_work_upper_bound,
                writer_facts.cumulative_library_work_upper_bound,
            )?;
            facts.new_allocation_request_bytes_upper_bound =
                add(requests, facts.writer_request_bytes_upper_bound)?;
            facts.coexisting_source_and_request_bytes_upper_bound = add(
                source_retained_bytes,
                facts.new_allocation_request_bytes_upper_bound,
            )?;
            check(&facts, limits, &mut work)?;
            prepared.push(writer);
            work.step()?;
        }
        Ok(PreparedConstantNamespaceWrite {
            records: prepared,
            facts,
            _types: types,
            control,
        })
    })();
    finish(work, result)
}

pub fn encode_constant_namespace(
    pools: &ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &EncodedTypeTable<'_>,
    source_retained_bytes: usize,
    record_limits: ConstantWriteProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<wire::IpcConstantPool>, Error> {
    prepare_constant_namespace_write(
        pools,
        bindings,
        types,
        source_retained_bytes,
        record_limits,
        limits,
        control,
    )?
    .emit()
}
