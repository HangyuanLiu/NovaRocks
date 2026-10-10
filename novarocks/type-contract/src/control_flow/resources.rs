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

//! Actual owned Control construction geometry. This is a cumulative request/work
//! upper bound, not retained capacity or an allocator/CPU admission grant.
use crate::{
    CompileControlError,
    owned_resources::{btree, layout::arc_layout},
};
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ControlOwnedResourceFacts {
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlResourceError {
    Control(CompileControlError),
    SourceModel(&'static str),
}
impl From<CompileControlError> for ControlResourceError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
fn resource() -> ControlResourceError {
    ControlResourceError::Control(CompileControlError::ResourceExhausted)
}
pub fn control_resource_add(a: usize, b: usize) -> Result<usize, ControlResourceError> {
    a.checked_add(b).ok_or_else(resource)
}
pub fn control_resource_mul(a: usize, b: usize) -> Result<usize, ControlResourceError> {
    a.checked_mul(b).ok_or_else(resource)
}
fn tree_error(e: btree::BTreeResourceError) -> ControlResourceError {
    match e {
        btree::BTreeResourceError::SourceModel(m) => ControlResourceError::SourceModel(m),
        btree::BTreeResourceError::Arithmetic(_) => resource(),
    }
}
/// Numerical facts only. The original owner decides which actual containers
/// coexist and calls its parent admission before touching their backing.
#[derive(Default)]
pub struct ControlResourceCounter {
    facts: ControlOwnedResourceFacts,
}
impl ControlResourceCounter {
    pub fn facts(&self) -> ControlOwnedResourceFacts {
        self.facts
    }
    pub fn merge(&mut self, f: ControlOwnedResourceFacts) -> Result<(), ControlResourceError> {
        self.facts.allocation_requests_upper_bound = control_resource_add(
            self.facts.allocation_requests_upper_bound,
            f.allocation_requests_upper_bound,
        )?;
        self.facts.allocation_request_bytes_upper_bound = control_resource_add(
            self.facts.allocation_request_bytes_upper_bound,
            f.allocation_request_bytes_upper_bound,
        )?;
        self.work(f.cumulative_work_upper_bound)
    }
    pub fn work(&mut self, n: usize) -> Result<(), ControlResourceError> {
        self.facts.cumulative_work_upper_bound =
            control_resource_add(self.facts.cumulative_work_upper_bound, n)?;
        Ok(())
    }
    pub fn buffer<T>(&mut self, n: usize, requests: usize) -> Result<(), ControlResourceError> {
        if n == 0 {
            return Ok(());
        }
        let l = Layout::array::<T>(n).map_err(|_| resource())?;
        self.layout(l, requests)
    }
    pub fn layout(&mut self, l: Layout, requests: usize) -> Result<(), ControlResourceError> {
        self.facts.allocation_requests_upper_bound =
            control_resource_add(self.facts.allocation_requests_upper_bound, requests)?;
        let bytes = control_resource_mul(l.size(), requests)?;
        self.facts.allocation_request_bytes_upper_bound =
            control_resource_add(self.facts.allocation_request_bytes_upper_bound, bytes)?;
        // Copy, initialization and destruction of the actual closed backing.
        self.work(control_resource_add(
            bytes,
            control_resource_mul(requests, 32)?,
        )?)
    }
    pub fn arc<T>(&mut self, n: usize) -> Result<(), ControlResourceError> {
        let p = Layout::array::<T>(n).map_err(|_| resource())?;
        let l = arc_layout(p).map_err(|e| match e {
            crate::owned_resources::layout::LayoutResourceError::SourceModel => {
                ControlResourceError::SourceModel("Control Arc source model drift")
            }
            _ => resource(),
        })?;
        self.layout(l, 1)
    }
    pub fn tree<K, V>(&mut self, n: usize) -> Result<(), ControlResourceError> {
        let f = btree::insertion_only::<K, V>(n).map_err(tree_error)?;
        self.merge(ControlOwnedResourceFacts {
            allocation_requests_upper_bound: f.allocation_requests_upper_bound,
            allocation_request_bytes_upper_bound: f.request_bytes_upper_bound,
            cumulative_work_upper_bound: f.cumulative_work_upper_bound,
        })?;
        // Actual temporary and failed-construction tree cleanup is bounded by
        // the original locked traversal author, never by source B.
        self.work(btree::retain_work::<K, V>(n).map_err(tree_error)?)
    }
    pub fn tree_entry<K, V>(&mut self, n: usize) -> Result<(), ControlResourceError> {
        let l = btree::node_layout_typed::<K, V>().map_err(tree_error)?;
        let lookup = btree::lookup_work_typed(n).map_err(tree_error)?;
        self.layout(l, 1)?;
        self.work(control_resource_add(
            control_resource_mul(lookup, 4)?,
            control_resource_mul(control_resource_mul(lookup / 16, l.size())?, 16)?,
        )?)
    }
    pub fn lookup_work(n: usize) -> Result<usize, ControlResourceError> {
        btree::lookup_work_typed(n).map_err(tree_error)
    }
}
