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

use std::sync::{Arc, Mutex};

use super::{
    ConnectorBeginScanRequest, ConnectorError, ConnectorErrorKind, ConnectorExecutionDeclaration,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorMetadata, ConnectorRequestContext, ConnectorScan, ConnectorScanHandle, ConnectorSplit,
    ConnectorSplitPlanningRequest, ConnectorTableHandle,
};

/// FE-only capability for planning a read after metadata has resolved a table.
/// It intentionally has no reader-opening method.
pub trait ConnectorScanPlanning: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError>;

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError>;
}

/// FE-only capability that turns a logical control binding into the bounded,
/// opaque declaration accepted by a BE execution installer.
pub trait ConnectorExecutionDistribution: Send + Sync {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError>;
}

/// A control-plane Connector generation. Metadata, scan planning, and
/// execution distribution must all describe the same logical descriptor and
/// incarnation. It is deliberately unable to open a batch reader.
pub struct ConnectorControlBinding {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    metadata: Arc<dyn ConnectorMetadata>,
    planning: Arc<dyn ConnectorScanPlanning>,
    distribution: Arc<dyn ConnectorExecutionDistribution>,
}

impl ConnectorControlBinding {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
    ) -> Result<Self, ConnectorError> {
        if metadata.instance_id() != &descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector metadata capability owner does not match its control binding",
            ));
        }
        if planning.instance_id() != &descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector scan planning capability owner does not match its control binding",
            ));
        }
        Ok(Self {
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }

    pub fn planning(&self) -> &Arc<dyn ConnectorScanPlanning> {
        &self.planning
    }

    pub fn execution_declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        let declaration = self.distribution.declaration(context)?;
        if declaration.descriptor() != &self.descriptor
            || declaration.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector execution declaration does not match its control binding generation",
            ));
        }
        Ok(declaration)
    }
}

/// Narrow consumer port used by core planning code. Its implementation belongs
/// to the frontend process; core neither owns the control registry nor creates
/// a control binding.
pub trait ConnectorControlResolver: Send + Sync {
    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError>;
}

/// Lifecycle port owned by the frontend composition root. Core may register
/// or retire a logical control generation, but it never owns the registry.
pub trait ConnectorControlRegistry: ConnectorControlResolver {
    fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError>;

    fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError>;
}

/// Keeps one control generation live from metadata/planning until the caller
/// completes the execution-binding barrier. The opaque release action is
/// frontend-owned and is never part of a wire contract.
#[derive(Clone)]
pub struct ConnectorControlPlanningLease {
    binding: Arc<ConnectorControlBinding>,
    _release: Arc<PlanningLeaseRelease>,
}

struct PlanningLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorControlPlanningLease {
    pub fn new(
        binding: Arc<ConnectorControlBinding>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Self {
        Self {
            binding,
            _release: Arc::new(PlanningLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        }
    }

    pub fn binding(&self) -> &Arc<ConnectorControlBinding> {
        &self.binding
    }
}

impl Drop for PlanningLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}
