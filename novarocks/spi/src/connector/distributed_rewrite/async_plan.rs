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

//! Closed Rewrite CPU borrowing port.

use super::*;
use std::task::{Context, Poll};

/// Only the concrete first-party source selected by the same lease may enter
/// the closed CPU factory. Input/output remain in this source on unwind.
pub trait RewritePlanCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> crate::connector::ProviderBindingEpoch;
    fn run_plan(&self);
}

pub struct RewriteCpuInput(pub Arc<dyn RewritePlanCpuSource>);
impl RewriteCpuInput {
    pub fn original_source_type_id(&self) -> std::any::TypeId {
        std::any::Any::type_id(self.0.as_ref())
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.0.descriptor()
    }
    pub fn incarnation(&self) -> crate::connector::ProviderBindingEpoch {
        self.0.incarnation()
    }
    pub fn run_original_phase(&self) {
        self.0.run_plan();
    }
}

/// Typed facade over the existing serial creator slot, never a closure runner.
pub trait RewriteCpuPort: Send + Sync {
    fn submit_in(&self, input: &mut Option<RewriteCpuInput>) -> Result<(), ConnectorError>;
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>>;
    fn metadata_bytes(&self) -> Result<std::num::NonZeroU64, ConnectorError>;
}
