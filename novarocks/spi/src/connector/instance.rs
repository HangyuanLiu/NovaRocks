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

use std::sync::Arc;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceDistribution,
    ConnectorMetadata, ConnectorRead,
};

pub struct ConnectorInstance {
    descriptor: ConnectorInstanceDescriptor,
    metadata: Option<Arc<dyn ConnectorMetadata>>,
    read: Arc<dyn ConnectorRead>,
    distribution: Option<Arc<dyn ConnectorInstanceDistribution>>,
}

impl ConnectorInstance {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        metadata: Option<Arc<dyn ConnectorMetadata>>,
        read: Arc<dyn ConnectorRead>,
    ) -> Result<Self, ConnectorError> {
        if read.instance_id() != &descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector read capability owner does not match its instance",
            ));
        }
        if let Some(metadata) = &metadata
            && metadata.instance_id() != &descriptor.instance_id
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector metadata capability owner does not match its instance",
            ));
        }
        Ok(Self {
            descriptor,
            metadata,
            read,
            distribution: None,
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub fn metadata(&self) -> Option<&Arc<dyn ConnectorMetadata>> {
        self.metadata.as_ref()
    }

    pub fn read(&self) -> &Arc<dyn ConnectorRead> {
        &self.read
    }

    pub fn with_distribution(
        mut self,
        distribution: Arc<dyn ConnectorInstanceDistribution>,
    ) -> Self {
        self.distribution = Some(distribution);
        self
    }

    pub fn distribution(&self) -> Option<&Arc<dyn ConnectorInstanceDistribution>> {
        self.distribution.as_ref()
    }
}
