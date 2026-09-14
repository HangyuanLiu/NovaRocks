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
use std::time::Instant;

use novarocks_state_store_api::{
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest, StateStoreProviderId,
    StateStoreProviderInstance, StateStoreProviderLifecycle,
};

use crate::{
    StateStoreHostError, StateStoreHostErrorKind, StateStoreHostInput, StateStoreProviderRegistry,
    StateStoreRunPolicy,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreHostLifecycle {
    Ready,
    Draining,
    Stopped,
}

pub struct StateStoreHost {
    provider_id: StateStoreProviderId,
    lifecycle: StateStoreHostLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    run_policy: StateStoreRunPolicy,
    instance: Box<dyn StateStoreProviderInstance>,
}

impl StateStoreHost {
    pub async fn open(
        registry: &StateStoreProviderRegistry,
        input: StateStoreHostInput,
        deadline: Instant,
    ) -> Result<Self, StateStoreHostError> {
        let provider_id = input.provider_id;
        let run_policy = input.run_policy;
        let bound = registry.bind(&input)?;
        let request = StateStoreOpenRequest {
            cluster_id: input.cluster_id,
            limits: input.limits,
            deadline,
        };
        let mut instance = bound.factory.open(request).await.map_err(|error| {
            StateStoreHostError::provider_failure(
                StateStoreHostErrorKind::Open,
                provider_id,
                "state store provider failed to open",
                error,
            )
        })?;

        let validation = validate_open_instance(bound.descriptor, instance.as_ref());
        if let Err(error) = validation {
            return match instance.shutdown(deadline).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.with_cleanup(cleanup)),
            };
        }
        let state_store = instance.state_store().expect("validated provider exposure");
        Ok(Self {
            provider_id,
            lifecycle: StateStoreHostLifecycle::Ready,
            state_store: Some(state_store),
            run_policy,
            instance,
        })
    }

    pub const fn provider_id(&self) -> StateStoreProviderId {
        self.provider_id
    }

    pub const fn lifecycle(&self) -> StateStoreHostLifecycle {
        self.lifecycle
    }

    pub fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store.clone()
    }

    pub const fn run_policy(&self) -> StateStoreRunPolicy {
        self.run_policy
    }

    pub fn durable(&self) -> Option<(Arc<dyn StateStore>, StateStoreRunPolicy)> {
        self.state_store
            .clone()
            .map(|store| (store, self.run_policy))
    }

    pub async fn release_abandoned_attempts(&self) -> Result<usize, StateStoreHostError> {
        let Some(store) = &self.state_store else {
            return Ok(0);
        };
        store
            .attempts()
            .drain_abandoned_attempts()
            .await
            .map_err(|error| {
                StateStoreHostError::provider_failure(
                    StateStoreHostErrorKind::Shutdown,
                    self.provider_id,
                    "state store provider failed to release abandoned attempt evidence",
                    error,
                )
            })
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreHostError> {
        if self.lifecycle == StateStoreHostLifecycle::Stopped {
            return Ok(());
        }
        if let Err(error) = self.release_abandoned_attempts().await {
            tracing::warn!(%error, "abandoned state store attempts were not released before shutdown");
        }
        self.lifecycle = StateStoreHostLifecycle::Draining;
        self.state_store.take();
        match self.instance.shutdown(deadline).await {
            Ok(()) => {
                self.lifecycle = StateStoreHostLifecycle::Stopped;
                Ok(())
            }
            Err(error) => {
                let kind = if error.kind() == StateStoreErrorKind::DeadlineExceeded {
                    StateStoreHostErrorKind::ShutdownDeadlineExceeded
                } else {
                    StateStoreHostErrorKind::Shutdown
                };
                Err(StateStoreHostError::provider_failure(
                    kind,
                    self.provider_id,
                    "state store provider failed to shut down",
                    error,
                ))
            }
        }
    }
}

fn validate_open_instance(
    descriptor: novarocks_state_store_api::StateStoreProviderDescriptor,
    instance: &dyn StateStoreProviderInstance,
) -> Result<(), StateStoreHostError> {
    let provider_id = descriptor.id;
    if instance.descriptor() != &descriptor {
        return Err(StateStoreHostError::provider_failure(
            StateStoreHostErrorKind::DescriptorMismatch,
            provider_id,
            "state store provider instance descriptor does not match the selected provider",
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store provider instance descriptor mismatch",
            ),
        ));
    }
    if instance.lifecycle() != StateStoreProviderLifecycle::Ready {
        return Err(StateStoreHostError::provider_failure(
            StateStoreHostErrorKind::Open,
            provider_id,
            "state store provider instance did not become ready",
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store provider instance is not ready",
            ),
        ));
    }
    if instance.state_store().is_none() {
        return Err(StateStoreHostError::provider_failure(
            StateStoreHostErrorKind::Open,
            provider_id,
            "state store provider instance did not expose a store",
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store provider instance has no store",
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use novarocks_state_store_api::{
        StateStoreLimits, StateStoreProviderDescriptor, StateStoreProviderId,
    };
    use novarocks_state_store_testkit::testing::InMemoryStateStoreProviderFactory;

    use super::*;
    use crate::{
        StateStoreHostErrorKind, StateStoreHostInput, StateStoreProviderRegistration,
        StateStoreProviderRegistry,
    };

    const PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("runtime-host-test");
    const DESCRIPTOR: StateStoreProviderDescriptor =
        StateStoreProviderDescriptor::new(PROVIDER_ID, novarocks_state_store_api::MAX_KEY_BYTES);

    fn registry() -> StateStoreProviderRegistry {
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::new(DESCRIPTOR, |_| {
                Ok(Box::new(InMemoryStateStoreProviderFactory::new(DESCRIPTOR)))
            }))
            .expect("register test provider");
        registry
    }

    fn input(cluster_id: impl Into<String>) -> StateStoreHostInput {
        StateStoreHostInput {
            cluster_id: cluster_id.into(),
            provider_id: PROVIDER_ID,
            limits: StateStoreLimits::default(),
            run_policy: StateStoreRunPolicy::default(),
        }
    }

    #[tokio::test]
    async fn host_owns_provider_lifecycle_and_releases_the_store_before_shutdown() {
        let mut host = StateStoreHost::open(
            &registry(),
            input("runtime-host-lifecycle"),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("open host");

        assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Ready);
        assert!(host.state_store().is_some());
        host.shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown host");
        assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Stopped);
        assert!(host.state_store().is_none());
    }

    #[tokio::test]
    async fn host_rejects_invalid_server_opening_facts_before_provider_open() {
        let result = StateStoreHost::open(
            &registry(),
            input("  "),
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("empty cluster identity must fail closed"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), StateStoreHostErrorKind::InvalidConfiguration);
    }
}
