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

//! Driver-local ordered RF state for the original compiled aggregate.
//! Group identity and expression evaluation remain with that processor. The
//! helper only copies a newly inserted group's already evaluated key
//! onto its creating host and uses the existing neutral producer lifecycle.

use arrow::array::ArrayRef;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use novarocks_local_program::{AggregateTopNFilter, LocalProgram, ProgramNodeId, ProgramNodeKind};

use super::aggregate::AGGREGATE_TOPN_PUBLISH_ROWS;
use super::aggregate::native_runtime_filter::{
    AggregateTopNProducerSession, AggregateTopNProducerSessionFactory,
};
use super::aggregate::topn_boundary::{
    AggregateTopNBoundaryError, OwnedAggregateTopNBoundaryState, borrowed_key_from_group_array,
};
use super::compiled_expression::RuntimeKernelControl;
use crate::exec::expr::agg::AggregateVec;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime_filter::{
    OrderedTopNAllocationHost, RuntimeFilterProducerFailure, RuntimeFilterSessionRef,
};

fn invalid(message: &'static str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message)).into()
}

fn specifications(
    program: &LocalProgram,
    node: ProgramNodeId,
) -> ExecutionResult<&[AggregateTopNFilter]> {
    let Some(node) = program.graph().nodes().get(node.index()) else {
        return Err(invalid("compiled aggregate TopN node is absent"));
    };
    let ProgramNodeKind::Aggregate { topn_filters, .. } = node.kind() else {
        return Err(invalid("compiled aggregate TopN node is not an Aggregate"));
    };
    if topn_filters.is_empty() {
        return Err(invalid(
            "compiled aggregate TopN has no frozen producer sites",
        ));
    }
    Ok(topn_filters.as_slice())
}

pub(crate) struct CompiledAggregateTopNFactory {
    node: ProgramNodeId,
    session: AggregateTopNProducerSessionFactory,
    host: OrderedTopNAllocationHost,
    control: RuntimeKernelControl,
}

impl CompiledAggregateTopNFactory {
    pub(crate) fn try_new(
        program: &LocalProgram,
        node: ProgramNodeId,
        session: RuntimeFilterSessionRef,
        actual_dop: i32,
        host: &OrderedTopNAllocationHost,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<Self> {
        let specs = specifications(program, node)?;
        let session = AggregateTopNProducerSessionFactory::try_from_compiled(
            specs, session, actual_dop, host, control,
        )?;
        Ok(Self {
            node,
            session,
            host: host.clone(),
            control: control.clone(),
        })
    }

    pub(crate) fn create_for_driver(
        &self,
        program: &LocalProgram,
        dop: i32,
        driver_id: i32,
    ) -> ExecutionResult<CompiledAggregateTopN> {
        let specs = specifications(program, self.node)?;
        self.control.checkpoint(0)?;
        let mut boundaries = AggregateVec::new_in(self.host.vector_allocator());
        boundaries
            .try_reserve_exact(specs.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for (index, spec) in specs.iter().enumerate() {
            self.control.checkpoint(1)?;
            let order = self.session.order_contract(index).ok_or_else(|| {
                invalid("compiled aggregate TopN producer and boundary sites differ")
            })?;
            boundaries.push(OwnedAggregateTopNBoundaryState::try_new_owned(
                spec.limit,
                order.clone(),
                &self.host,
            )?);
        }
        if self.session.order_contract(specs.len()).is_some() {
            return Err(invalid(
                "compiled aggregate TopN producer and boundary sites differ",
            ));
        }
        self.control.checkpoint(0)?;
        // Complete every boundary before constructing a session. No producer
        // is opened until the processor reaches its original runtime bind.
        let session = self
            .session
            .create_for_driver_owned(dop, driver_id, &self.control)?;
        Ok(CompiledAggregateTopN {
            node: self.node,
            host: self.host.clone(),
            control: self.control.clone(),
            session,
            boundaries,
            rows_since_publish: 0,
        })
    }
}

pub(crate) struct CompiledAggregateTopN {
    node: ProgramNodeId,
    host: OrderedTopNAllocationHost,
    control: RuntimeKernelControl,
    session: AggregateTopNProducerSession,
    boundaries: AggregateVec<OwnedAggregateTopNBoundaryState>,
    rows_since_publish: usize,
}

impl CompiledAggregateTopN {
    pub(crate) fn bind(&mut self) -> ExecutionResult<()> {
        self.session.bind_owned(&self.control)
    }

    /// The caller invokes this only for its original KeyLookup::is_new.
    pub(crate) fn observe_new_group(
        &mut self,
        program: &LocalProgram,
        keys: &[ArrayRef],
        row: usize,
        group_id: usize,
    ) -> ExecutionResult<()> {
        let specs = specifications(program, self.node)?;
        if specs.len() != self.boundaries.len() {
            return Err(invalid(
                "compiled aggregate TopN producer and boundary sites differ",
            ));
        }
        for (spec, boundary) in specs.iter().zip(self.boundaries.iter_mut()) {
            self.control.checkpoint(1)?;
            let array = keys.get(spec.group_key_ordinal).ok_or(
                AggregateTopNBoundaryError::MissingGroupKey {
                    ordinal: spec.group_key_ordinal,
                    actual: keys.len(),
                },
            )?;
            let value = borrowed_key_from_group_array(boundary.contract(), array, row)?;
            let key = self.host.copy_scalar(value, &self.control)?;
            boundary.observe_owned_group(group_id, key, &self.control)?;
        }
        Ok(())
    }

    /// Publication follows successful evaluation/update of the complete batch.
    pub(crate) fn after_successful_input(&mut self, rows: usize) -> ExecutionResult<()> {
        self.rows_since_publish = self
            .rows_since_publish
            .checked_add(rows)
            .ok_or(KernelFailure::ResourceExhausted)?;
        if self.rows_since_publish >= AGGREGATE_TOPN_PUBLISH_ROWS {
            self.session
                .submit_pending_owned(&mut self.boundaries, &self.control)?;
            self.rows_since_publish = 0;
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> ExecutionResult<()> {
        self.session
            .finish_owned(&mut self.boundaries, &self.control)
    }

    pub(crate) fn fail(&mut self, reason: RuntimeFilterProducerFailure) -> ExecutionResult<()> {
        self.session.fail_owned(reason)
    }
}
