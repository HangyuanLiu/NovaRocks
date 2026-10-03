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

//! Accumulate the actual receiving records before any pool materialization.
//! Stream/schema/scratch preparation retains its preceding owner gates. These
//! request facts are neither allocator usable sizes nor formal MEM grants.

use super::{
    ConstantDecodeProjectionLimits, PhysicalConstantCodecError as Error, finish, record_sources,
    recursive,
};
use crate::{
    ipc_flat_stream_v2::{PreparedFlatReader, preflight_flat_constant_stream},
    ipc_recursive_stream_v2::{PreparedRecursiveReader, preflight_recursive_constant_stream},
    physical_type_v2::DecodedTypeTable,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantPool};
use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReferenceError};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::{alloc::Layout, mem, sync::Arc};

/// The caller allocates a share of its whole-fragment envelope to this stage.
/// There is no independent default pool-count policy or implicit wallet.
#[derive(Clone, Copy, Debug)]
pub struct ConstantNamespaceProjectionLimits {
    pub max_records: usize,
    pub max_preparation_request_bytes: usize,
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantNamespaceResourceFacts {
    pub record_count: usize,
    pub source_retained_bytes: usize,
    pub prepared_storage_request_bytes: usize,
    pub geometry_scratch_request_bytes: usize,
    pub preparation_request_bytes: usize,
    pub pool_table_request_bytes_upper_bound: usize,
    pub reader_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}

enum PreparedReader<'raw, 'table> {
    Flat(PreparedFlatReader<'raw, 'table, 'table>),
    Recursive(PreparedRecursiveReader<'raw, 'table, 'table>),
}
impl PreparedReader<'_, '_> {
    fn geometry_requests(&self) -> (usize, usize) {
        match self {
            Self::Flat(value) => (
                value.geometry_scratch_request_bytes(),
                value.geometry_scratch_request_count(),
            ),
            Self::Recursive(value) => (
                value.geometry_scratch_request_bytes(),
                value.geometry_scratch_request_count(),
            ),
        }
    }
    fn reader_requests(&self) -> (usize, usize, usize) {
        match self {
            Self::Flat(value) => {
                let facts = value.facts();
                (
                    facts.new_allocation_request_bytes_upper_bound,
                    facts.allocation_request_count_upper_bound,
                    facts.cumulative_library_work_upper_bound,
                )
            }
            Self::Recursive(value) => {
                let facts = value.facts();
                (
                    facts.new_allocation_request_bytes_upper_bound,
                    facts.allocation_request_count_upper_bound,
                    facts.cumulative_library_work_upper_bound,
                )
            }
        }
    }
    fn materialize(self, control: &dyn PureCompileControl) -> Result<ConstantPool, Error> {
        Ok(match self {
            Self::Flat(value) => value.materialize(control)?,
            Self::Recursive(value) => value.materialize(control)?,
        })
    }
}
struct PreparedRecord<'raw, 'table> {
    id: ConstantPoolId,
    reader: PreparedReader<'raw, 'table>,
}

/// Private construction freezes the same checked stream, type, policy and
/// numeric model. The original control is retained through consumption.
pub struct PreparedConstantNamespace<'raw, 'table, 'control> {
    records: Vec<PreparedRecord<'raw, 'table>>,
    facts: ConstantNamespaceResourceFacts,
    control: &'control dyn PureCompileControl,
}
impl PreparedConstantNamespace<'_, '_, '_> {
    pub const fn facts(&self) -> &ConstantNamespaceResourceFacts {
        &self.facts
    }

    /// Reuses the original ConstantPools duplicate author. No second pool map
    /// or byte-based backing deduplication exists. Package closure is later.
    pub fn materialize(self) -> Result<ConstantPools, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = (|| {
            let mut pools = ConstantPools::empty();
            for record in self.records {
                work.step()?;
                work.flush()?;
                let pool = record.reader.materialize(work.control())?;
                work.flush()?;
                let inserted = pools.insert(record.id, pool);
                work.step()?;
                inserted?;
            }
            Ok(pools)
        })();
        finish(work, result)
    }
}

impl From<ConstantReferenceError> for Error {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(error)
            | ConstantReferenceError::Constant(ConstantError::Control(error)) => {
                Self::Control(error)
            }
            error => Self::Reference(error),
        }
    }
}
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| shape("constant namespace resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| shape("constant namespace resource product overflow"))
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

/// Rust 1.92 alloc/btree/node.rs: B=6, eleven keys/values, twelve edges.
/// LeafNode has private Rust field order: use the actual member Layout sizes
/// and maximum padding per member, rather than assert an identical mirror.
fn table_node_layout() -> Result<Layout, Error> {
    let key = Layout::new::<ConstantPoolId>();
    let value = Layout::new::<ConstantPool>();
    let pointer = Layout::new::<usize>();
    let align = key.align().max(value.align()).max(pointer.align());
    let members = add(
        add(pointer.size(), 2 * mem::size_of::<u16>())?,
        mul(11, add(key.size(), value.size())?)?,
    )?;
    let leaf = add(members, mul(5, align - 1)?)?;
    let internal = add(add(leaf, mul(12, pointer.size())?)?, align - 1)?;
    Layout::from_size_align(internal, align)
        .map(|value| value.pad_to_align())
        .map_err(|_| shape("constant namespace table node layout is unrepresentable"))
}

fn initial_facts(
    count: usize,
    source: usize,
    limits: ConstantNamespaceProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantNamespaceResourceFacts, Error> {
    cap(
        count,
        limits.max_records,
        "constant namespace count envelope exceeded",
        work,
    )?;
    let model_locked = LOCKED_FAMILY && LOCKED_TOOLCHAIN;
    work.step()?;
    if !model_locked {
        return Err(shape("constant namespace source model drift"));
    }
    let storage = Layout::array::<PreparedRecord<'_, '_>>(count)
        .map_err(|_| shape("constant namespace prepared storage layout is unrepresentable"))?
        .size();
    // Only insertion occurs. Every retained node has at least one entry; no
    // node is removed or reallocated. Cumulative node requests <= input count.
    let table = mul(table_node_layout()?.size(), count)?;
    let requests = add(storage, table)?;
    let levels = if count == 0 {
        0
    } else {
        (usize::BITS - count.leading_zeros()) as usize + 1
    };
    let per_level = add(
        mul(
            11,
            add(
                mem::size_of::<ConstantPoolId>(),
                mem::size_of::<ConstantPool>(),
            )?,
        )?,
        12 * mem::size_of::<usize>() + 11,
    )?;
    let table_work = mul(count, mul(levels, per_level)?)?;
    let bookkeeping = add(requests, add(table_work, mul(count, 32)?)?)?;
    cap(
        storage,
        limits.max_preparation_request_bytes,
        "constant namespace preparation envelope exceeded",
        work,
    )?;
    cap(
        requests,
        limits.max_new_allocation_request_bytes,
        "constant namespace request envelope exceeded",
        work,
    )?;
    cap(
        add(source, requests)?,
        limits.max_coexisting_source_and_request_bytes,
        "constant namespace coexistence envelope exceeded",
        work,
    )?;
    cap(
        bookkeeping,
        limits.max_cumulative_library_work,
        "constant namespace work envelope exceeded",
        work,
    )?;
    Ok(ConstantNamespaceResourceFacts {
        record_count: count,
        source_retained_bytes: source,
        prepared_storage_request_bytes: storage,
        preparation_request_bytes: storage,
        pool_table_request_bytes_upper_bound: table,
        allocation_request_count_upper_bound: add(count, usize::from(count != 0))?,
        new_allocation_request_bytes_upper_bound: requests,
        coexisting_source_and_request_bytes_upper_bound: add(source, requests)?,
        cumulative_library_work_upper_bound: bookkeeping,
        ..Default::default()
    })
}

/// Raw DTO/type-table backing, source capacity and earlier schema/framing
/// stages remain the whole-package host's obligation. This model admits
/// prepared storage before reserve, aggregate live geometry before each
/// geometry allocation, and all receiving pools/table requests before the
/// first materialization. Each record's official metadata and reader model are
/// consumed once; inactive profiles never supply a fallback.
#[expect(
    clippy::too_many_arguments,
    reason = "Explicit original source, policy, profiles, verifier and control are independently authored"
)]
pub fn prepare_constant_namespace<'raw, 'table, 'control>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let mut facts = initial_facts(records.len(), source_retained_bytes, limits, &mut work)?;
        let mut visible_source = Layout::array::<wire::IpcConstantPool>(records.len())
            .map_err(|_| shape("constant namespace source layout is unrepresentable"))?
            .size();
        for record in records {
            visible_source = add(visible_source, record.arrow_ipc.capacity())?;
            work.step()?;
        }
        cap(
            visible_source,
            source_retained_bytes,
            "constant namespace source invoice excludes retained record storage",
            &mut work,
        )?;
        work.flush()?;
        let mut prepared = Vec::new();
        let reserved = prepared.try_reserve_exact(records.len());
        work.flush()?;
        reserved.map_err(|_| shape("constant namespace prepared storage allocation failed"))?;
        for record in records {
            let (value_type, field) =
                record_sources(record, types, source_retained_bytes, &mut work)?;
            let live_source = add(source_retained_bytes, facts.preparation_request_bytes)?;
            work.flush()?;
            let reader = if recursive(field.data_type()) {
                let mut stream_limits = record_limits.recursive_stream;
                // Intersect the original explicit record envelope with the
                // caller's remaining namespace preparation share. The geometry
                // owner checks this before its actual Vec reserve.
                stream_limits.batch.max_geometry_request_bytes = stream_limits
                    .batch
                    .max_geometry_request_bytes
                    .min(limits.max_preparation_request_bytes - facts.preparation_request_bytes)
                    .min(
                        limits.max_new_allocation_request_bytes
                            - facts.new_allocation_request_bytes_upper_bound,
                    )
                    .min(
                        limits.max_coexisting_source_and_request_bytes
                            - facts.coexisting_source_and_request_bytes_upper_bound,
                    );
                let stream = preflight_recursive_constant_stream(
                    &record.arrow_ipc,
                    field,
                    stream_limits,
                    verifier,
                    work.control(),
                )?;
                work.flush()?;
                PreparedReader::Recursive(stream.prepare_pool_borrowed(
                    Arc::clone(field),
                    value_type,
                    live_source,
                    policy,
                    record_limits.recursive_reader,
                    work.control(),
                )?)
            } else {
                let stream = preflight_flat_constant_stream(
                    &record.arrow_ipc,
                    field,
                    record_limits.flat_stream,
                    verifier,
                    work.control(),
                )?;
                work.flush()?;
                PreparedReader::Flat(stream.prepare_pool_borrowed(
                    Arc::clone(field),
                    value_type,
                    live_source,
                    policy,
                    record_limits.flat_reader,
                    work.control(),
                )?)
            };
            work.flush()?;
            let (geometry_bytes, _) = reader.geometry_requests();
            let (reader_bytes, reader_count, reader_work) = reader.reader_requests();
            facts.geometry_scratch_request_bytes =
                add(facts.geometry_scratch_request_bytes, geometry_bytes)?;
            facts.preparation_request_bytes = add(
                facts.prepared_storage_request_bytes,
                facts.geometry_scratch_request_bytes,
            )?;
            facts.reader_request_bytes_upper_bound =
                add(facts.reader_request_bytes_upper_bound, reader_bytes)?;
            facts.allocation_request_count_upper_bound =
                add(facts.allocation_request_count_upper_bound, reader_count)?;
            facts.new_allocation_request_bytes_upper_bound = add(
                add(
                    facts.prepared_storage_request_bytes,
                    facts.pool_table_request_bytes_upper_bound,
                )?,
                facts.reader_request_bytes_upper_bound,
            )?;
            facts.coexisting_source_and_request_bytes_upper_bound = add(
                source_retained_bytes,
                facts.new_allocation_request_bytes_upper_bound,
            )?;
            facts.cumulative_library_work_upper_bound =
                add(facts.cumulative_library_work_upper_bound, reader_work)?;
            cap(
                facts.preparation_request_bytes,
                limits.max_preparation_request_bytes,
                "constant namespace preparation envelope exceeded",
                &mut work,
            )?;
            cap(
                facts.new_allocation_request_bytes_upper_bound,
                limits.max_new_allocation_request_bytes,
                "constant namespace request envelope exceeded",
                &mut work,
            )?;
            cap(
                facts.coexisting_source_and_request_bytes_upper_bound,
                limits.max_coexisting_source_and_request_bytes,
                "constant namespace coexistence envelope exceeded",
                &mut work,
            )?;
            cap(
                facts.cumulative_library_work_upper_bound,
                limits.max_cumulative_library_work,
                "constant namespace work envelope exceeded",
                &mut work,
            )?;
            prepared.push(PreparedRecord {
                id: ConstantPoolId::new(record.id),
                reader,
            });
            work.step()?;
        }
        Ok(PreparedConstantNamespace {
            records: prepared,
            facts,
            control,
        })
    })();
    finish(work, result)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Explicit original source, policy, profiles, verifier and control are independently authored"
)]
pub fn decode_constant_namespace(
    records: &[wire::IpcConstantPool],
    types: &DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<ConstantPools, Error> {
    prepare_constant_namespace(
        records,
        types,
        source_retained_bytes,
        policy,
        record_limits,
        limits,
        verifier,
        control,
    )?
    .materialize()
}
