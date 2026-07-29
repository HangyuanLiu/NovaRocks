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

use novarocks_spi::connector::{
    ConnectorInstance, ConnectorInstanceDeclaration, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorInstanceInstaller, ConnectorProviderId,
    ConnectorRequestContext,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectorHostErrorKind {
    DuplicateInstance,
    UnknownInstance,
    UnknownInstaller,
    ConflictingDeclaration,
    StaleIncarnation,
    RetiringInstance,
    InstallerFailure,
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

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: ConnectorHostErrorKind::UnknownInstance,
            message: message.into(),
        }
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
    instances: BTreeMap<ConnectorInstanceId, HostedConnectorInstance>,
    installers: BTreeMap<ConnectorProviderId, Arc<dyn ConnectorInstanceInstaller>>,
}

#[derive(Clone)]
struct HostedConnectorInstance {
    instance: Arc<ConnectorInstance>,
    incarnation: Option<ConnectorInstanceIncarnation>,
    declaration_digest: Option<[u8; 32]>,
    state: ConnectorInstanceState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectorInstanceState {
    Active,
    Retiring,
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
        self.instances.insert(
            instance_id,
            HostedConnectorInstance {
                instance: Arc::new(instance),
                incarnation: None,
                declaration_digest: None,
                state: ConnectorInstanceState::Active,
            },
        );
        Ok(())
    }

    pub(crate) fn unregister(
        &mut self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.instances
            .remove(instance_id)
            .map(|entry| entry.instance)
            .ok_or_else(|| unknown_instance(instance_id))
    }

    pub(crate) fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        let entry = self
            .instances
            .get(instance_id)
            .ok_or_else(|| unknown_instance(instance_id))?;
        if entry.state == ConnectorInstanceState::Retiring {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::RetiringInstance,
                message: format!("connector instance `{}` is retiring", instance_id.as_str()),
            });
        }
        Ok(Arc::clone(&entry.instance))
    }

    pub(crate) fn register_installer(
        &mut self,
        installer: Arc<dyn ConnectorInstanceInstaller>,
    ) -> Result<(), ConnectorHostError> {
        let provider_id = installer.provider_id().clone();
        if self.installers.contains_key(&provider_id) {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::DuplicateInstance,
                message: format!(
                    "connector instance installer `{}` is already registered",
                    provider_id.as_str()
                ),
            });
        }
        self.installers.insert(provider_id, installer);
        Ok(())
    }

    pub(crate) fn install(
        &mut self,
        declaration: &ConnectorInstanceDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        let descriptor = declaration.descriptor();
        let instance_id = descriptor.instance_id.clone();
        let digest = declaration.digest();
        if let Some(existing) = self.instances.get(&instance_id) {
            match existing.incarnation {
                Some(incarnation) if incarnation == declaration.incarnation() => {
                    if existing.declaration_digest == Some(digest) {
                        if existing.state == ConnectorInstanceState::Retiring {
                            return Err(ConnectorHostError {
                                kind: ConnectorHostErrorKind::RetiringInstance,
                                message: format!(
                                    "connector instance `{}` is retiring",
                                    instance_id.as_str()
                                ),
                            });
                        }
                        return Ok(Arc::clone(&existing.instance));
                    }
                    return Err(ConnectorHostError {
                        kind: ConnectorHostErrorKind::ConflictingDeclaration,
                        message: format!(
                            "connector instance `{}` received a conflicting declaration",
                            instance_id.as_str()
                        ),
                    });
                }
                Some(incarnation) if incarnation > declaration.incarnation() => {
                    return Err(ConnectorHostError {
                        kind: ConnectorHostErrorKind::StaleIncarnation,
                        message: format!(
                            "connector instance `{}` received a stale incarnation",
                            instance_id.as_str()
                        ),
                    });
                }
                Some(_) => {}
                None => {
                    return Err(ConnectorHostError {
                        kind: ConnectorHostErrorKind::ConflictingDeclaration,
                        message: format!(
                            "connector instance `{}` was registered without a distributable declaration",
                            instance_id.as_str()
                        ),
                    });
                }
            }
        }

        let installer = self
            .installers
            .get(&descriptor.provider_id)
            .cloned()
            .ok_or_else(|| ConnectorHostError {
                kind: ConnectorHostErrorKind::UnknownInstaller,
                message: format!(
                    "no startup installer is registered for connector provider `{}`",
                    descriptor.provider_id.as_str()
                ),
            })?;
        let instance =
            installer
                .install(declaration, context)
                .map_err(|error| ConnectorHostError {
                    kind: ConnectorHostErrorKind::InstallerFailure,
                    message: format!(
                        "connector provider `{}` could not install instance `{}`: {error}",
                        descriptor.provider_id.as_str(),
                        instance_id.as_str()
                    ),
                })?;
        if instance.descriptor() != descriptor {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::InstallerFailure,
                message: format!(
                    "connector provider `{}` installed a mismatched instance descriptor",
                    descriptor.provider_id.as_str()
                ),
            });
        }
        let instance = Arc::new(instance);
        self.instances.insert(
            instance_id,
            HostedConnectorInstance {
                instance: Arc::clone(&instance),
                incarnation: Some(declaration.incarnation()),
                declaration_digest: Some(digest),
                state: ConnectorInstanceState::Active,
            },
        );
        Ok(instance)
    }

    pub(crate) fn retire(
        &mut self,
        instance_id: &ConnectorInstanceId,
        incarnation: ConnectorInstanceIncarnation,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        let entry = self
            .instances
            .get_mut(instance_id)
            .ok_or_else(|| unknown_instance(instance_id))?;
        if entry.incarnation != Some(incarnation) {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::StaleIncarnation,
                message: format!(
                    "connector instance `{}` incarnation does not match the active binding",
                    instance_id.as_str()
                ),
            });
        }
        entry.state = ConnectorInstanceState::Retiring;
        Ok(Arc::clone(&entry.instance))
    }
}

fn unknown_instance(instance_id: &ConnectorInstanceId) -> ConnectorHostError {
    ConnectorHostError {
        kind: ConnectorHostErrorKind::UnknownInstance,
        message: format!("unknown connector instance `{}`", instance_id.as_str()),
    }
}

/// Keeps a decoder-created connector instance registered for the lifetime of
/// its physical scan source.  Dropping the last lease unregisters the exact
/// instance, so query-local credentials never accumulate in the shared host.
pub(crate) struct ConnectorInstanceLease {
    host: Arc<std::sync::RwLock<ConnectorHost>>,
    instance_id: ConnectorInstanceId,
}

impl ConnectorInstanceLease {
    pub(crate) fn new(
        host: Arc<std::sync::RwLock<ConnectorHost>>,
        instance_id: ConnectorInstanceId,
    ) -> Self {
        Self { host, instance_id }
    }
}

impl Drop for ConnectorInstanceLease {
    fn drop(&mut self) {
        if let Ok(mut host) = self.host.write() {
            let _ = host.unregister(&self.instance_id);
        }
    }
}
