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

//! Array-output materialization after complete immutable flat stream checking.
//! The request/work projection is an input to the original preparation host;
//! it does not implement Account/grant/allocation-origin/free authorization.

use super::{
    FlatConstantStream, FlatPoolResourceError, FlatPoolResourceProjection, reader_allocations,
    reader_diagnostics, reader_work,
};
use crate::physical_type_v2::TypeCodecError;
use arrow::{
    datatypes::{Field, Schema},
    ipc::reader::read_record_batch,
};
use arrow_buffer::Buffer;
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{collections::HashMap, sync::Arc};

/// Explicit admitted host envelopes. They do not create a separate wallet.
#[derive(Clone, Copy, Debug)]
pub struct FlatReaderProjectionLimits {
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}

/// Conservative allocation requests (not allocator usable bytes/RSS) and
/// cumulative byte/element/header visits for this actual reader/pool path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatReaderResourceFacts {
    pub source_retained_bytes: usize,
    pub payload_request_bytes_upper_bound: usize,
    pub structural_request_bytes_upper_bound: usize,
    pub diagnostic_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
    pub pool: FlatPoolResourceProjection,
}

#[derive(Debug)]
pub enum FlatReaderError {
    Projection(FlatPoolResourceError),
    Arrow(String),
}
impl From<FlatPoolResourceError> for FlatReaderError {
    fn from(error: FlatPoolResourceError) -> Self {
        Self::Projection(error)
    }
}
impl From<novarocks_type_contract::CompileControlError> for FlatReaderError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Projection(FlatPoolResourceError::Control(error))
    }
}
impl std::fmt::Display for FlatReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Projection(e) => e.fmt(f),
            Self::Arrow(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for FlatReaderError {}
fn shape(message: &'static str) -> FlatPoolResourceError {
    FlatPoolResourceError::Shape(TypeCodecError::InvalidShape(message))
}
fn add(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_add(b)
        .ok_or_else(|| shape("flat reader resource sum overflow"))
}
fn cap(actual: usize, limit: usize, message: &'static str) -> Result<(), FlatPoolResourceError> {
    if actual > limit {
        Err(shape(message))
    } else {
        Ok(())
    }
}

impl FlatConstantStream<'_, '_> {
    /// The caller supplies actual retained source backing, including any larger
    /// allocation kept by a slice and retained Field/type owners, including all
    /// HashMap bucket/control backing even after removals. Visible input
    /// length is only a lower bound. Raw framing/schema verification is the
    /// preceding stage, not authorized retroactively by this projection.
    pub fn preflight_reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<FlatReaderResourceFacts, FlatPoolResourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result =
            self.reader_resources(value_type, source_retained_bytes, policy, limits, &mut work);
        if matches!(&result, Err(FlatPoolResourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FlatReaderResourceFacts, FlatPoolResourceError> {
        cap(
            self.input().len(),
            source_retained_bytes,
            "flat reader source retention is below visible input",
        )?;
        work.step()?;
        cap(
            reader_work::source_metadata_work(source_retained_bytes, work)?,
            limits.max_cumulative_library_work,
            "flat reader source metadata work envelope exceeded",
        )?;
        work.flush()?;
        let pool = self.preflight_pool_resources(value_type, policy, work.control())?;
        let structures = reader_allocations::preflight(self, &pool, work)?;
        let diagnostics = reader_diagnostics::preflight(self, work)?;
        let payload = add(
            add(
                pool.owned_body_capacity_bytes,
                pool.alignment_repair_capacity_bytes_upper_bound,
            )?,
            pool.empty_offset_capacity_bytes,
        )?;
        let requested = add(
            add(payload, structures.structural_request_bytes_upper_bound)?,
            diagnostics.request_bytes_upper_bound,
        )?;
        let coexisting = add(source_retained_bytes, requested)?;
        let library_work = reader_work::preflight(
            self,
            source_retained_bytes,
            &pool,
            &structures,
            &diagnostics,
            work,
        )?;
        cap(
            requested,
            limits.max_new_allocation_request_bytes,
            "flat reader allocation request envelope exceeded",
        )?;
        cap(
            coexisting,
            limits.max_coexisting_source_and_request_bytes,
            "flat reader source coexistence envelope exceeded",
        )?;
        cap(
            library_work,
            limits.max_cumulative_library_work,
            "flat reader cumulative library work envelope exceeded",
        )?;
        work.step()?;
        let payload_requests = usize::from(pool.owned_body_capacity_bytes > 0)
            + usize::from(pool.alignment_repair_capacity_bytes_upper_bound > 0)
            + usize::from(pool.empty_offset_capacity_bytes > 0);
        Ok(FlatReaderResourceFacts {
            source_retained_bytes,
            payload_request_bytes_upper_bound: payload,
            structural_request_bytes_upper_bound: structures.structural_request_bytes_upper_bound,
            diagnostic_request_bytes_upper_bound: diagnostics.request_bytes_upper_bound,
            allocation_request_count_upper_bound: add(
                add(
                    structures.allocation_requests_upper_bound,
                    diagnostics.allocation_requests_upper_bound,
                )?,
                payload_requests,
            )?,
            new_allocation_request_bytes_upper_bound: requested,
            coexisting_source_and_request_bytes_upper_bound: coexisting,
            cumulative_library_work_upper_bound: library_work,
            pool,
        })
    }

    /// Invokes the actual public safe reader with the same original Field Arc.
    /// The complete array-output request/work gate precedes Schema/body creation.
    /// A typed refusal is primary; no partial pool is exposed on any error.
    pub fn materialize_pool(
        &self,
        field: Arc<Field>,
        value_type: FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, FlatReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            if !std::ptr::eq(self.field(), field.as_ref()) {
                return Err(shape("flat reader requires the original source Field Arc").into());
            }
            work.step()?;
            let _facts = self.reader_resources(
                &value_type,
                source_retained_bytes,
                policy,
                limits,
                &mut work,
            )?;
            work.flush()?;
            let schema = Arc::new(Schema::new([Arc::clone(&field)]));
            work.flush()?;
            let body = Buffer::from_slice_ref(self.batch_body());
            work.flush()?;
            let read = read_record_batch(
                &body,
                self.record_batch(),
                schema,
                &HashMap::new(),
                None,
                &self.metadata_version(),
            );
            // Formatting the ordinary Arrow error is part of the already checked
            // diagnostic envelope. Observe the same original control afterwards.
            let read = read.map_err(|e| FlatReaderError::Arrow(e.to_string()));
            work.flush()?;
            let decoded = read?;
            let data = decoded.column(0).to_data();
            work.flush()?;
            let pool = ConstantPool::try_new(
                field,
                value_type,
                data,
                policy,
                CompilePhase::Decode,
                work.control(),
            )
            .map_err(FlatPoolResourceError::from)?;
            Ok(pool)
        })();
        if matches!(
            &result,
            Err(FlatReaderError::Projection(FlatPoolResourceError::Control(
                _
            )))
        ) {
            return result;
        }
        work.finish()?;
        result
    }
}
