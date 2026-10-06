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

//! One cumulative package admission over original component snapshots.
//! Source backing and fixed inline staging are present once; component
//! prepare/emit snapshots replace their own previous contribution. This
//! numerical envelope is not an allocator-capacity or opaque-CPU grant.

use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};

#[derive(Clone, Copy, Debug)]
pub(crate) struct EncodeAdmissionLimits {
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncodeAdmissionFacts {
    pub source_retained_bytes: usize,
    pub inline_staging_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Contribution {
    pub requests: usize,
    pub bytes: usize,
    pub work: usize,
}
impl Contribution {
    pub const EMPTY: Self = Self {
        requests: 0,
        bytes: 0,
        work: 0,
    };
    pub fn add(self, other: Self) -> Result<Self, CompileControlError> {
        Ok(Self {
            requests: sum(self.requests, other.requests)?,
            bytes: sum(self.bytes, other.bytes)?,
            work: sum(self.work, other.work)?,
        })
    }
    fn includes(self, old: Self) -> bool {
        self.requests >= old.requests && self.bytes >= old.bytes && self.work >= old.work
    }
}
fn sum(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}

// Each original resource author owns exactly one slot. Node-family snapshots
// are combined with previously completed nodes before replacing the Nodes
// slot; they do not require a map indexed by sparse native NodeId.
#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum Component {
    Inputs,
    Types,
    Functions,
    Aggregates,
    Providers,
    Payloads,
    Reads,
    Relations,
    Schemas,
    Constants,
    Parameters,
    Values,
    Expressions,
    Requests,
    Envelope,
    Nodes,
    Control,
    Calls,
    Pruning,
    Cuts,
    Result,
    Scans,
    Writes,
}
const COMPONENTS: usize = Component::Writes as usize + 1;

#[derive(Debug)]
pub(super) enum AdmissionError {
    Control(CompileControlError),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for AdmissionError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}

pub(super) struct EncodeAdmission<'control, 'parent> {
    control: &'control dyn PureCompileControl,
    source: usize,
    inline: usize,
    contributions: [Contribution; COMPONENTS],
    limits: EncodeAdmissionLimits,
    admit: &'parent mut dyn FnMut(&EncodeAdmissionFacts) -> Result<(), CompileControlError>,
}
impl<'control, 'parent> EncodeAdmission<'control, 'parent> {
    // Construction has no callback. The first original component's update
    // brings its captured requests into the same numerical/parent gate before
    // any completed source observation. Known root requests may be installed
    // with seed() before that first original callback.
    pub fn new(
        source: usize,
        inline: usize,
        limits: EncodeAdmissionLimits,
        admit: &'parent mut dyn FnMut(&EncodeAdmissionFacts) -> Result<(), CompileControlError>,
        work: &CompileCheckpoints<'control>,
    ) -> Self {
        Self {
            control: work.control(),
            source,
            inline,
            contributions: [Contribution::EMPTY; COMPONENTS],
            limits,
            admit,
        }
    }
    pub fn check_in(&self, work: &CompileCheckpoints<'_>) -> Result<(), AdmissionError> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(AdmissionError::InvalidSource(
                "package admission uses another caller control",
            ));
        }
        Ok(())
    }
    pub fn seed(
        &mut self,
        component: Component,
        contribution: Contribution,
    ) -> Result<(), CompileControlError> {
        let slot = &mut self.contributions[component as usize];
        if *slot != Contribution::EMPTY {
            return Err(CompileControlError::ResourceExhausted);
        }
        *slot = contribution;
        self.check_limits()?;
        Ok(())
    }
    pub fn replace(
        &mut self,
        component: Component,
        next: Contribution,
    ) -> Result<(), CompileControlError> {
        let slot = &mut self.contributions[component as usize];
        if !next.includes(*slot) {
            return Err(CompileControlError::ResourceExhausted);
        }
        *slot = next;
        let facts = self.check_limits()?;
        (self.admit)(&facts)?;
        Ok(())
    }
    pub fn facts(&self) -> Result<EncodeAdmissionFacts, CompileControlError> {
        let mut total = Contribution::EMPTY;
        for contribution in self.contributions {
            total = total.add(contribution)?;
        }
        Ok(EncodeAdmissionFacts {
            source_retained_bytes: self.source,
            inline_staging_bytes: self.inline,
            allocation_requests_upper_bound: total.requests,
            allocation_request_bytes_upper_bound: total.bytes,
            coexisting_source_and_request_bytes_upper_bound: sum(
                sum(self.source, self.inline)?,
                total.bytes,
            )?,
            cumulative_work_upper_bound: total.work,
        })
    }
    fn check_limits(&self) -> Result<EncodeAdmissionFacts, CompileControlError> {
        let facts = self.facts()?;
        let limit = self.limits;
        if facts.allocation_requests_upper_bound > limit.max_allocation_requests
            || facts.allocation_request_bytes_upper_bound > limit.max_allocation_request_bytes
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limit.max_coexisting_source_and_request_bytes
            || facts.cumulative_work_upper_bound > limit.max_work
        {
            return Err(CompileControlError::ResourceExhausted);
        }
        Ok(facts)
    }
    // The caller uses this only before a new component starts. It includes
    // the original union once and every earlier admitted backing request,
    // conservatively including temporary requests, plus the fixed inline
    // frame. It never sums namespace floors that already contain source B.
    pub fn next_source(&self) -> Result<usize, CompileControlError> {
        Ok(self
            .check_limits()?
            .coexisting_source_and_request_bytes_upper_bound)
    }
}
