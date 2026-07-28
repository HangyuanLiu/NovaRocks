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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{ConnectorInstance, ConnectorInstanceId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectorHostErrorKind {
    DuplicateInstance,
    UnknownInstance,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectorHostError {
    kind: ConnectorHostErrorKind,
    message: String,
}

impl ConnectorHostError {
    pub(crate) fn kind(&self) -> ConnectorHostErrorKind {
        self.kind
    }
}

impl fmt::Display for ConnectorHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ConnectorHostError {}

#[derive(Clone, Default)]
pub(crate) struct ConnectorHost {
    instances: BTreeMap<ConnectorInstanceId, Arc<ConnectorInstance>>,
}

impl ConnectorHost {
    pub(crate) fn register(
        &mut self,
        instance: ConnectorInstance,
    ) -> Result<(), ConnectorHostError> {
        let instance_id = instance.descriptor().instance_id.clone();
        if self.instances.contains_key(&instance_id) {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::DuplicateInstance,
                message: format!(
                    "connector instance `{}` is already registered",
                    instance_id.as_str()
                ),
            });
        }
        self.instances.insert(instance_id, Arc::new(instance));
        Ok(())
    }

    pub(crate) fn unregister(
        &mut self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.instances
            .remove(instance_id)
            .ok_or_else(|| unknown_instance(instance_id))
    }

    pub(crate) fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.instances
            .get(instance_id)
            .cloned()
            .ok_or_else(|| unknown_instance(instance_id))
    }
}

fn unknown_instance(instance_id: &ConnectorInstanceId) -> ConnectorHostError {
    ConnectorHostError {
        kind: ConnectorHostErrorKind::UnknownInstance,
        message: format!("unknown connector instance `{}`", instance_id.as_str()),
    }
}
