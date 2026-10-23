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
//! Exactly addressed UNION ALL producer endpoint.
use super::union_all_shared::{UnionAllSharedState, UnionProducer};
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    operator::{EncodedValueOccurrence, InputEncodingPolicy, Operator, OperatorLifecycle},
    operator_factory::OperatorFactory,
    shape::{ControlStep, SinkKernel, SinkStep, Transform},
    step::{AdmissionNext, NextStep},
    step_context::StepContext,
};
use crate::runtime::{
    fragment::ExecutionResult,
    operator_byte_credit::{CreditAcquire, QueuePermit, credit_contract},
};
use std::sync::Mutex;

pub struct UnionAllSinkFactory {
    name: String,
    state: UnionAllSharedState,
    input: usize,
}
impl UnionAllSinkFactory {
    pub(crate) fn new(state: UnionAllSharedState, input: usize, node_id: i32) -> Self {
        Self {
            name: format!("UnionAllSink (id={node_id}, input={input})"),
            state,
            input,
        }
    }
}
impl OperatorFactory for UnionAllSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let endpoint = usize::try_from(dop)
            .ok()
            .zip(usize::try_from(driver_id).ok())
            .ok_or_else(|| credit_contract("negative UnionAll endpoint"))
            .and_then(|(dop, driver)| self.state.producer(self.input, driver, dop));
        Box::new(Transform::hydrating(UnionAllSinkOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            endpoint,
            pending: Mutex::new(None),
            finished: false,
        }))
    }
    fn is_sink(&self) -> bool {
        true
    }
}
struct UnionAllSinkOperator {
    name: String,
    state: UnionAllSharedState,
    endpoint: ExecutionResult<UnionProducer>,
    pending: Mutex<Option<QueuePermit>>,
    finished: bool,
}
impl OperatorLifecycle for UnionAllSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn cancel(&mut self) {
        self.pending
            .get_mut()
            .expect("UnionAll pending lock")
            .take();
        self.state.close();
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.pending
            .get_mut()
            .expect("UnionAll pending lock")
            .take();
        Ok(())
    }
}
// UnionAll has no scalar operation. It retains every exact encoded occurrence
// already admitted by each branch's normalizing Project, including nested ones.
impl InputEncodingPolicy for UnionAllSinkOperator {
    fn retains_encoded_column(
        &self,
        _: novarocks_types::SlotId,
        _: &arrow::datatypes::DataType,
    ) -> bool {
        true
    }
    fn retains_encoded_path(&self, _: EncodedValueOccurrence<'_>) -> bool {
        true
    }
}
impl SinkKernel for UnionAllSinkOperator {
    fn control_observable(&self) -> Option<std::sync::Arc<crate::runtime::observable::Observable>> {
        self.endpoint
            .as_ref()
            .ok()
            .and_then(|endpoint| self.state.producer_observable(*endpoint).ok())
    }
    fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        if self.state.is_closed() {
            Ok(ControlStep::Withdrawn)
        } else {
            Ok(ControlStep::Active)
        }
    }
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        input: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let endpoint = *self.endpoint.as_ref().map_err(Clone::clone)?;
        let mut pending = self.pending.lock().expect("UnionAll pending lock");
        if pending.is_some() {
            return Err(credit_contract(
                "UnionAll accepted another input before using its reservation",
            ));
        }
        if self.finished {
            return Err(credit_contract(
                "UnionAll admission follows endpoint finish",
            ));
        }
        let bytes = input.retained_geometry(self.state.input_limits(endpoint)?)?;
        match self.state.acquire(endpoint, bytes)? {
            CreditAcquire::Granted(permit) => {
                *pending = Some(permit);
                Ok(None)
            }
            CreditAcquire::Pending(wait) => Ok(Some(AdmissionNext::Wait(wait))),
            CreditAcquire::Closed => Ok(Some(AdmissionNext::ResumeExisting)),
        }
    }
    fn consume(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let endpoint = *self.endpoint.as_ref().map_err(Clone::clone)?;
        let permit = self
            .pending
            .get_mut()
            .expect("UnionAll pending lock")
            .take()
            .ok_or_else(|| credit_contract("UnionAll accepted input has no queue reservation"))?;
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len())
            .map_err(|_| credit_contract("UnionAll row count is not representable"))?;
        self.state.push_chunk(endpoint, chunk, permit)?;
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        if !self.finished {
            self.state
                .producer_finished(*self.endpoint.as_ref().map_err(Clone::clone)?)?;
            self.finished = true;
        }
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
