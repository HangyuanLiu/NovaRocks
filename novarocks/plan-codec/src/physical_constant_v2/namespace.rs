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
    record_sources_captured, recursive,
};
use crate::{
    ipc_flat_stream_v2::{
        PreparedFlatReader, preflight_flat_constant_stream, preflight_flat_constant_stream_in,
        progress::IpcReaderProgressFacts,
    },
    ipc_recursive_stream_v2::{
        PreparedRecursiveReader, preflight_recursive_constant_stream,
        preflight_recursive_constant_stream_in,
    },
    physical_type_v2::DecodedTypeTable,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantPool};
use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReferenceError};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
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

type Admit<'a> = dyn FnMut(&ConstantNamespaceResourceFacts) -> Result<(), CompileControlError> + 'a;
fn gate_parent(
    facts: &ConstantNamespaceResourceFacts,
    limits: ConstantNamespaceProjectionLimits,
    admit: &mut Admit<'_>,
) -> Result<(), CompileControlError> {
    for (actual, maximum) in [
        (facts.record_count, limits.max_records),
        (
            facts.preparation_request_bytes,
            limits.max_preparation_request_bytes,
        ),
        (
            facts.new_allocation_request_bytes_upper_bound,
            limits.max_new_allocation_request_bytes,
        ),
        (
            facts.coexisting_source_and_request_bytes_upper_bound,
            limits.max_coexisting_source_and_request_bytes,
        ),
        (
            facts.cumulative_library_work_upper_bound,
            limits.max_cumulative_library_work,
        ),
    ] {
        if actual > maximum {
            return Err(CompileControlError::ResourceExhausted);
        }
    }
    admit(facts)
}
// One child snapshot replaces its previous contribution. Its complete reader
// requests already contain geometry; the preparation sublimit views it once.
fn with_child(
    base: ConstantNamespaceResourceFacts,
    child: &IpcReaderProgressFacts,
) -> Result<ConstantNamespaceResourceFacts, CompileControlError> {
    let sum = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(CompileControlError::ResourceExhausted)
    };
    let mut facts = base;
    facts.geometry_scratch_request_bytes = sum(
        base.geometry_scratch_request_bytes,
        child.geometry_scratch_request_bytes,
    )?;
    facts.preparation_request_bytes = sum(
        facts.prepared_storage_request_bytes,
        facts.geometry_scratch_request_bytes,
    )?;
    facts.reader_request_bytes_upper_bound = sum(
        base.reader_request_bytes_upper_bound,
        child.new_allocation_request_bytes_upper_bound,
    )?;
    facts.allocation_request_count_upper_bound = sum(
        base.allocation_request_count_upper_bound,
        child.allocation_request_count_upper_bound,
    )?;
    facts.new_allocation_request_bytes_upper_bound = sum(
        sum(
            facts.prepared_storage_request_bytes,
            facts.pool_table_request_bytes_upper_bound,
        )?,
        facts.reader_request_bytes_upper_bound,
    )?;
    facts.coexisting_source_and_request_bytes_upper_bound = sum(
        facts.source_retained_bytes,
        facts.new_allocation_request_bytes_upper_bound,
    )?;
    facts.cumulative_library_work_upper_bound = sum(
        base.cumulative_library_work_upper_bound,
        child.cumulative_library_work_upper_bound,
    )?;
    Ok(facts)
}

enum PreparedReader<'raw, 'table, 'control> {
    Flat(PreparedFlatReader<'raw, 'table, 'table, 'control>),
    Recursive(PreparedRecursiveReader<'raw, 'table, 'table, 'control>),
}
impl PreparedReader<'_, '_, '_> {
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
    fn materialize_in(
        self,
        admit: &mut dyn FnMut(&IpcReaderProgressFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, Error> {
        Ok(match self {
            Self::Flat(value) => value.materialize_in(admit, work)?,
            Self::Recursive(value) => value.materialize_in(admit, work)?,
        })
    }
}
struct PreparedRecord<'raw, 'table, 'control> {
    id: ConstantPoolId,
    reader: PreparedReader<'raw, 'table, 'control>,
}

/// Private construction freezes the same checked stream, type, policy and
/// numeric model. The original control is retained through consumption.
pub struct PreparedConstantNamespace<'raw, 'table, 'control> {
    records: Vec<PreparedRecord<'raw, 'table, 'control>>,
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
        let result = self.materialize_core(None, &mut work);
        finish(work, result)
    }
    pub(crate) fn materialize_in(
        self,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPools, Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(shape(
                "constant namespace caller work has a different original control",
            ));
        }
        admit(&self.facts)?;
        self.materialize_core(Some(admit), work)
    }
    fn materialize_core(
        self,
        mut admit: Option<&mut Admit<'_>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPools, Error> {
        (|| {
            let mut pools = ConstantPools::empty();
            for record in self.records {
                work.step()?;
                work.flush()?;
                let pool = if let Some(parent) = &mut admit {
                    let (geometry_bytes, geometry_count) = record.reader.geometry_requests();
                    let (reader_bytes, reader_count, reader_work) = record.reader.reader_requests();
                    record.reader.materialize_in(
                        &mut |progress| {
                            if progress.geometry_scratch_request_bytes > geometry_bytes
                                || progress.geometry_scratch_request_count > geometry_count
                                || progress.new_allocation_request_bytes_upper_bound > reader_bytes
                                || progress.allocation_request_count_upper_bound > reader_count
                                || progress.cumulative_library_work_upper_bound > reader_work
                            {
                                return Err(CompileControlError::ResourceExhausted);
                            }
                            parent(&self.facts)
                        },
                        work,
                    )?
                } else {
                    record.reader.materialize(work.control())?
                };
                work.flush()?;
                let inserted = pools.insert(record.id, pool);
                work.step()?;
                inserted?;
            }
            Ok(pools)
        })()
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

/// The shared locked author uses these actual key/value member layouts.
fn table_node_layout() -> Result<Layout, Error> {
    crate::btree_resources_v2::node_layout::<ConstantPoolId, ConstantPool>().map_err(shape)
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
    let (storage, table, requests, bookkeeping) = initial_parts(count)?;
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
    initial_header(count, source, storage, table, requests, bookkeeping)
}

// Pure extraction of the original numerical author. Neither the caller nor
// the old facade duplicates the table-layout or insertion-work model.
fn initial_parts(count: usize) -> Result<(usize, usize, usize, usize), Error> {
    let storage = Layout::array::<PreparedRecord<'_, '_, '_>>(count)
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
    Ok((storage, table, requests, bookkeeping))
}

fn initial_header(
    count: usize,
    source: usize,
    storage: usize,
    table: usize,
    requests: usize,
    bookkeeping: usize,
) -> Result<ConstantNamespaceResourceFacts, Error> {
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
    let result = prepare_core(
        records,
        types,
        source_retained_bytes,
        policy,
        record_limits,
        limits,
        verifier,
        None,
        &mut work,
    );
    finish(work, result)
}

pub(crate) fn prepare_constant_namespace_in<'raw, 'table, 'control>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, Error> {
    prepare_core(
        records,
        types,
        source_retained_bytes,
        policy,
        record_limits,
        limits,
        verifier,
        Some(admit),
        work,
    )
}

fn prepare_core<'raw, 'table, 'control>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, Error> {
    (|| {
        let mut facts = if let Some(parent) = &mut admit {
            if !LOCKED_FAMILY || !LOCKED_TOOLCHAIN {
                return Err(shape("constant namespace source model drift"));
            }
            let parts =
                initial_parts(records.len()).map_err(|_| CompileControlError::ResourceExhausted)?;
            let known = initial_header(
                records.len(),
                source_retained_bytes,
                parts.0,
                parts.1,
                parts.2,
                parts.3,
            )
            .map_err(|_| CompileControlError::ResourceExhausted)?;
            gate_parent(&known, limits, *parent)?;
            known
        } else {
            initial_facts(records.len(), source_retained_bytes, limits, work)?
        };
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
            work,
        )?;
        work.flush()?;
        let mut prepared = Vec::new();
        let reserved = prepared.try_reserve_exact(records.len());
        crate::allocation_exit_v2::reserve_exit::<Error>(reserved, work)?;
        for record in records {
            if let Some(parent) = &mut admit {
                let base = facts;
                let live_source = add(source_retained_bytes, facts.preparation_request_bytes)
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                let mut snapshot = IpcReaderProgressFacts::default();
                let reader = prepare_record_reader_in(
                    record,
                    types,
                    source_retained_bytes,
                    live_source,
                    policy,
                    record_limits,
                    verifier,
                    &mut |progress| {
                        snapshot.geometry_scratch_request_bytes = snapshot
                            .geometry_scratch_request_bytes
                            .max(progress.geometry_scratch_request_bytes);
                        snapshot.geometry_scratch_request_count = snapshot
                            .geometry_scratch_request_count
                            .max(progress.geometry_scratch_request_count);
                        snapshot.allocation_request_count_upper_bound = snapshot
                            .allocation_request_count_upper_bound
                            .max(progress.allocation_request_count_upper_bound);
                        snapshot.new_allocation_request_bytes_upper_bound = snapshot
                            .new_allocation_request_bytes_upper_bound
                            .max(progress.new_allocation_request_bytes_upper_bound);
                        snapshot.cumulative_library_work_upper_bound = snapshot
                            .cumulative_library_work_upper_bound
                            .max(progress.cumulative_library_work_upper_bound);
                        gate_parent(&with_child(base, &snapshot)?, limits, *parent)
                    },
                    work,
                )?;
                let (geometry_bytes, geometry_count) = reader.geometry_requests();
                let (reader_bytes, reader_count, reader_work) = reader.reader_requests();
                // The original complete author must cover every prepared prefix.
                if snapshot.geometry_scratch_request_bytes > geometry_bytes
                    || snapshot.geometry_scratch_request_count > geometry_count
                    || snapshot.new_allocation_request_bytes_upper_bound > reader_bytes
                    || snapshot.allocation_request_count_upper_bound > reader_count
                    || snapshot.cumulative_library_work_upper_bound > reader_work
                {
                    return Err(shape(
                        "constant reader complete projection omits an admitted preparation prefix",
                    ));
                }
                snapshot.geometry_scratch_request_bytes = geometry_bytes;
                snapshot.geometry_scratch_request_count = geometry_count;
                snapshot.new_allocation_request_bytes_upper_bound = reader_bytes;
                snapshot.allocation_request_count_upper_bound = reader_count;
                snapshot.cumulative_library_work_upper_bound = reader_work;
                facts = with_child(base, &snapshot)?;
                gate_parent(&facts, limits, *parent)?;
                prepared.push(PreparedRecord {
                    id: ConstantPoolId::new(record.id),
                    reader,
                });
                work.step()?;
                continue;
            }
            let (value_type, field) = record_sources(record, types, source_retained_bytes, work)?;
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
                work,
            )?;
            cap(
                facts.new_allocation_request_bytes_upper_bound,
                limits.max_new_allocation_request_bytes,
                "constant namespace request envelope exceeded",
                work,
            )?;
            cap(
                facts.coexisting_source_and_request_bytes_upper_bound,
                limits.max_coexisting_source_and_request_bytes,
                "constant namespace coexistence envelope exceeded",
                work,
            )?;
            cap(
                facts.cumulative_library_work_upper_bound,
                limits.max_cumulative_library_work,
                "constant namespace work envelope exceeded",
                work,
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
            control: work.control(),
        })
    })()
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

fn prepare_record_reader_in<'raw, 'table, 'control>(
    record: &'raw wire::IpcConstantPool,
    types: &'table DecodedTypeTable,
    original_source: usize,
    live_source: usize,
    policy: ConstantPolicy,
    limits: ConstantDecodeProjectionLimits,
    verifier: &VerifierOptions,
    admit: &mut dyn FnMut(&IpcReaderProgressFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedReader<'raw, 'table, 'control>, Error> {
    let mut reader = None;
    record_sources_captured(
        record,
        types,
        original_source,
        &mut |value_type, field, work| {
            reader = Some(if recursive(field.data_type()) {
                let stream = preflight_recursive_constant_stream_in(
                    &record.arrow_ipc,
                    field,
                    limits.recursive_stream,
                    verifier,
                    live_source,
                    admit,
                    work,
                )?;
                PreparedReader::Recursive(stream.prepare_pool_borrowed_in(
                    Arc::clone(field),
                    value_type,
                    live_source,
                    policy,
                    limits.recursive_reader,
                    admit,
                    work,
                )?)
            } else {
                let stream = preflight_flat_constant_stream_in(
                    &record.arrow_ipc,
                    field,
                    limits.flat_stream,
                    verifier,
                    live_source,
                    admit,
                    work,
                )?;
                PreparedReader::Flat(stream.prepare_pool_borrowed_in(
                    Arc::clone(field),
                    value_type,
                    live_source,
                    policy,
                    limits.flat_reader,
                    admit,
                    work,
                )?)
            });
            Ok(())
        },
        work,
    )?;
    reader
        .ok_or_else(|| shape("constant reader was not prepared from its captured original sources"))
}
