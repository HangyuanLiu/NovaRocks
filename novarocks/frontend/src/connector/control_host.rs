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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use novarocks_spi::connector::{
    CatalogHandle, ConnectorCatalogMutationLease, ConnectorCatalogMutationResolver,
    ConnectorCleanupMaintenanceLease, ConnectorCleanupMaintenanceResolver, ConnectorControlBinding,
    ConnectorControlFactory, ConnectorControlFactoryRequest, ConnectorControlFactoryResolver,
    ConnectorControlPlanningLease, ConnectorControlRegistry, ConnectorControlResolver,
    ConnectorControlRuntimeId, ConnectorDataMutationLease, ConnectorDataMutationResolver,
    ConnectorDistributedRewriteLease, ConnectorDistributedRewriteResolver, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorMetadataMaintenanceLease, ConnectorMetadataMaintenanceResolver, ConnectorProviderId,
    ConnectorStatisticsLease, ConnectorStatisticsResolver, ConnectorWriteLease,
};

/// FE process owner of logical Connector control generations. It contains no
/// BE reader/runtime state and exposes only a narrow planning resolver to core.
#[derive(Clone, Default)]
pub struct ConnectorControlHost {
    state: Arc<Mutex<ControlHostState>>,
    factories: Arc<BTreeMap<ConnectorProviderId, Arc<dyn ConnectorControlFactory>>>,
}

#[derive(Default)]
struct ControlHostState {
    active: BTreeMap<ConnectorInstanceId, ConnectorControlRuntimeId>,
    generations: BTreeMap<ConnectorControlRuntimeId, ControlGeneration>,
    retired: BTreeSet<ConnectorControlRuntimeId>,
    /// Temporary bridge for the legacy FE effect contract.
    /// It is not a control-generation owner and is removed with that contract.
    legacy_execution_index: BTreeMap<ConnectorExecutionBindingKey, ConnectorControlRuntimeId>,
    /// Compatibility-only evidence retained until the FE effect contract stops
    /// carrying legacy execution keys. It never drives BE retirement.
    installed_backends: BTreeMap<ConnectorExecutionBindingKey, BTreeSet<String>>,
    ready_retires: Vec<ConnectorControlRetirement>,
}

impl ControlHostState {
    fn runtime_for_legacy_effect(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        self.legacy_execution_index
            .get(key)
            .copied()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "connector legacy effect generation is not registered",
                )
            })
    }

    fn active_legacy_effect(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
        let runtime_id = self.active.get(instance_id).copied().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })?;
        self.generations
            .get(&runtime_id)
            .map(|generation| generation.legacy_execution_key.clone())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })
    }
}

// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)
struct ControlGeneration {
    binding: Arc<ConnectorControlBinding>,
    legacy_execution_key: ConnectorExecutionBindingKey,
    state: ControlGenerationState,
    planning_leases: usize,
    mutation_leases: usize,
    data_mutation_leases: usize,
    metadata_maintenance_leases: usize,
    distributed_rewrite_leases: usize,
    cleanup_maintenance_leases: usize,
    write_leases: usize,
    statistics_leases: usize,
}

impl ControlGeneration {
    fn all_leases_released(&self) -> bool {
        self.planning_leases == 0
            && self.mutation_leases == 0
            && self.data_mutation_leases == 0
            && self.metadata_maintenance_leases == 0
            && self.distributed_rewrite_leases == 0
            && self.cleanup_maintenance_leases == 0
            && self.write_leases == 0
            && self.statistics_leases == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlGenerationState {
    Active,
    Retiring,
}

/// Compatibility-only retirement evidence for the remaining FE effect bridge.
/// It is local bookkeeping; BE catalog eviction is driven only by complete
/// reachability snapshots and `PruneCatalogs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorControlRetirement {
    pub key: ConnectorExecutionBindingKey,
    pub installed_backends: Vec<String>,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl ConnectorControlHost {
    pub fn new() -> Self {
        Self::with_factories(Vec::new()).expect("an empty connector factory set is valid")
    }

    /// Creates a control host with an immutable provider factory map. Provider
    /// IDs are process-level composition keys, so duplicates are rejected
    /// before the host can publish any generation.
    pub fn with_factories(
        factories: Vec<Arc<dyn ConnectorControlFactory>>,
    ) -> Result<Self, ConnectorError> {
        let mut factory_map = BTreeMap::new();
        for factory in factories {
            let provider_id = factory.provider_id().clone();
            if factory_map.insert(provider_id.clone(), factory).is_some() {
                return Err(invalid(format!(
                    "duplicate connector control factory for provider `{}`",
                    provider_id.as_str()
                )));
            }
        }
        Ok(Self {
            state: Arc::new(Mutex::new(ControlHostState::default())),
            factories: Arc::new(factory_map),
        })
    }

    /// Every exact catalog handle still protected by this FE process.
    ///
    /// Retiring generations remain present until their last planning or effect
    /// lease drains. Callers combine this with one complete desired-state
    /// snapshot before issuing a best-effort BE prune.
    pub(crate) fn reachable_catalog_handles(
        &self,
    ) -> Result<BTreeSet<CatalogHandle>, ConnectorError> {
        let state = self.lock_state()?;
        Ok(state
            .generations
            .values()
            .filter_map(|generation| generation.binding.catalog_handle().ok().cloned())
            .collect())
    }

    pub fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError> {
        let binding = Arc::new(binding);
        let legacy_execution_key = ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        };
        let control_runtime_id = binding.control_runtime_id();
        let instance_id = binding.descriptor().instance_id.clone();
        let mut state = self.lock_state()?;
        if state.retired.contains(&control_runtime_id) {
            return Err(invalid(
                "retired connector control generation cannot be registered again",
            ));
        }
        if let Some(existing) = state.generations.get(&control_runtime_id) {
            if existing.state == ControlGenerationState::Active {
                return Ok(());
            }
            return Err(invalid(
                "retiring connector control generation cannot be registered again",
            ));
        }
        if state
            .legacy_execution_index
            .contains_key(&legacy_execution_key)
        {
            return Err(invalid(
                "connector legacy effect generation is already registered",
            ));
        }
        if state.active.contains_key(&instance_id) {
            return Err(invalid(format!(
                "connector control instance `{}` already has an active generation",
                instance_id.as_str()
            )));
        }
        state.active.insert(instance_id, control_runtime_id);
        state
            .legacy_execution_index
            .insert(legacy_execution_key.clone(), control_runtime_id);
        state.generations.insert(
            control_runtime_id,
            ControlGeneration {
                binding,
                legacy_execution_key,
                state: ControlGenerationState::Active,
                planning_leases: 0,
                mutation_leases: 0,
                data_mutation_leases: 0,
                metadata_maintenance_leases: 0,
                distributed_rewrite_leases: 0,
                cleanup_maintenance_leases: 0,
                write_leases: 0,
                statistics_leases: 0,
            },
        );
        Ok(())
    }

    /// Prevents new planning immediately. Existing leases retain their exact
    /// effect owner until their final local release, after which the control
    /// generation is removed. BE catalog eviction is independently driven by
    /// the complete desired-state snapshot.
    pub fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        let key = state.active.remove(instance_id).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })?;
        let generation = state.generations.get_mut(&key).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "active connector control generation is missing",
            )
        })?;
        generation.state = ControlGenerationState::Retiring;
        let should_retire = generation.all_leases_released();
        if should_retire {
            if let Some(retirement) = queue_retirement(&mut state, key) {
                state.ready_retires.push(retirement);
            }
        }
        Ok(())
    }

    /// Records compatibility evidence from the retired FE effect bridge. This
    /// data is not used to manage BE catalog lifetime.
    pub fn record_installed_backend(
        &self,
        key: &ConnectorExecutionBindingKey,
        endpoint: impl Into<String>,
    ) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        state.runtime_for_legacy_effect(key)?;
        state
            .installed_backends
            .entry(key.clone())
            .or_default()
            .insert(endpoint.into());
        Ok(())
    }

    /// Returns compatibility retirement evidence. There is deliberately no
    /// production dispatch sink for it.
    pub fn take_ready_retires(&self) -> Result<Vec<ConnectorControlRetirement>, ConnectorError> {
        let mut state = self.lock_state()?;
        Ok(std::mem::take(&mut state.ready_retires))
    }

    fn acquire(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        let (binding, key) = {
            let mut state = self.lock_state()?;
            let key = state.active.get(instance_id).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&key).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            generation.planning_leases = generation.planning_leases.saturating_add(1);
            (Arc::clone(&generation.binding), key)
        };
        let state = Arc::downgrade(&self.state);
        Ok(ConnectorControlPlanningLease::new(binding, move || {
            release_lease(&state, key, LeaseKind::Planning);
        }))
    }

    fn acquire_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        let control_runtime_id = {
            let state = self.lock_state()?;
            state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?
        };
        self.acquire_exact_mutation(control_runtime_id, true)
    }

    fn acquire_exact_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
        require_active: bool,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        let (descriptor, provider_incarnation, mutation) = {
            let mut state = self.lock_state()?;
            let generation = state
                .generations
                .get_mut(&control_runtime_id)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "connector control runtime is not registered",
                    )
                })?;
            if require_active && generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let mutation = generation.binding.mutation().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no catalog mutation capability",
                )
            })?;
            generation.mutation_leases = generation.mutation_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                mutation,
            )
        };
        let state = Arc::downgrade(&self.state);
        ConnectorCatalogMutationLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            mutation,
            move || release_lease(&state, control_runtime_id, LeaseKind::Mutation),
        )
    }

    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let control_runtime_id = {
            let state = self.lock_state()?;
            state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?
        };
        self.acquire_data_mutation(control_runtime_id, true)
    }

    fn acquire_exact_data_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        self.acquire_data_mutation(control_runtime_id, false)
    }

    fn acquire_data_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
        require_active: bool,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let (descriptor, provider_incarnation, metadata, mutation) = {
            let mut state = self.lock_state()?;
            let generation = state
                .generations
                .get_mut(&control_runtime_id)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "connector control runtime is not registered",
                    )
                })?;
            if require_active && generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let mutation = generation.binding.data_mutation().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no data mutation capability",
                )
            })?;
            generation.data_mutation_leases = generation.data_mutation_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                Arc::clone(generation.binding.metadata()),
                mutation,
            )
        };
        let state = Arc::downgrade(&self.state);
        ConnectorDataMutationLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            metadata,
            mutation,
            move || release_lease(&state, control_runtime_id, LeaseKind::DataMutation),
        )
    }

    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        let control_runtime_id = {
            let state = self.lock_state()?;
            state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?
        };
        self.acquire_metadata_maintenance(control_runtime_id, true)
    }

    fn acquire_exact_metadata_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        self.acquire_metadata_maintenance(control_runtime_id, false)
    }

    fn acquire_metadata_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
        require_active: bool,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        let (descriptor, provider_incarnation, metadata, maintenance) = {
            let mut state = self.lock_state()?;
            let generation = state
                .generations
                .get_mut(&control_runtime_id)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "connector control runtime is not registered",
                    )
                })?;
            if require_active && generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let maintenance = generation
                .binding
                .metadata_maintenance()
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "connector control generation has no metadata maintenance capability",
                    )
                })?;
            generation.metadata_maintenance_leases =
                generation.metadata_maintenance_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                Arc::clone(generation.binding.metadata()),
                maintenance,
            )
        };
        let state = Arc::downgrade(&self.state);
        ConnectorMetadataMaintenanceLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            metadata,
            maintenance,
            move || release_lease(&state, control_runtime_id, LeaseKind::MetadataMaintenance),
        )
    }

    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        let control_runtime_id = {
            let state = self.lock_state()?;
            state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?
        };
        self.acquire_cleanup_maintenance(control_runtime_id, true)
    }

    fn acquire_exact_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        self.acquire_cleanup_maintenance(control_runtime_id, false)
    }

    /// Acquires metadata and cleanup from one exact control generation. Cleanup
    /// is FE-only and its lease keeps a retiring generation alive for replay of
    /// immutable prepared evidence; it never substitutes a current generation.
    fn acquire_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
        require_active: bool,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        let (descriptor, provider_incarnation, metadata, cleanup) = {
            let mut state = self.lock_state()?;
            let generation = state
                .generations
                .get_mut(&control_runtime_id)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "connector control runtime is not registered",
                    )
                })?;
            if require_active && generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let cleanup = generation
                .binding
                .cleanup_maintenance()
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "connector control generation has no cleanup maintenance capability",
                    )
                })?;
            generation.cleanup_maintenance_leases =
                generation.cleanup_maintenance_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                Arc::clone(generation.binding.metadata()),
                cleanup,
            )
        };
        let state = Arc::downgrade(&self.state);
        ConnectorCleanupMaintenanceLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            metadata,
            cleanup,
            move || release_lease(&state, control_runtime_id, LeaseKind::CleanupMaintenance),
        )
    }

    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        let control_runtime_id = {
            let state = self.lock_state()?;
            state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?
        };
        self.acquire_distributed_rewrite(control_runtime_id, true)
    }

    fn acquire_exact_distributed_rewrite(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        self.acquire_distributed_rewrite(control_runtime_id, false)
    }

    /// Acquire the metadata, rewrite planning, write-control, and execution
    /// distribution capabilities from exactly one registered generation. The
    /// resulting lease intentionally owns a single retirement counter: a
    /// derived C1 writer lease retains this parent rather than acquiring a
    /// separate current write generation.
    fn acquire_distributed_rewrite(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
        require_active: bool,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        let (
            binding,
            descriptor,
            provider_incarnation,
            metadata,
            planning,
            rewrite,
            write,
            distribution,
        ) = {
            let mut state = self.lock_state()?;
            let generation = state
                .generations
                .get_mut(&control_runtime_id)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "connector control runtime is not registered",
                    )
                })?;
            if require_active && generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let rewrite = generation
                .binding
                .distributed_rewrite()
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "connector control generation has no distributed rewrite capability",
                    )
                })?;
            let write = generation.binding.write().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no distributed write capability",
                )
            })?;
            generation.distributed_rewrite_leases =
                generation.distributed_rewrite_leases.saturating_add(1);
            generation.planning_leases = generation.planning_leases.saturating_add(1);
            (
                Arc::clone(&generation.binding),
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                Arc::clone(generation.binding.metadata()),
                Arc::clone(generation.binding.planning()),
                rewrite,
                write,
                Arc::clone(generation.binding.execution_distribution()),
            )
        };
        let state = Arc::downgrade(&self.state);
        let planning_state = Arc::downgrade(&self.state);
        let planning_runtime_id = control_runtime_id;
        let planning_lease = ConnectorControlPlanningLease::new(binding, move || {
            release_lease(&planning_state, planning_runtime_id, LeaseKind::Planning);
        });
        ConnectorDistributedRewriteLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            planning_lease,
            metadata,
            planning,
            rewrite,
            write,
            distribution,
            move || {
                release_lease(&state, control_runtime_id, LeaseKind::DistributedRewrite);
            },
        )
    }

    fn acquire_write(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorWriteLease, ConnectorError> {
        let (write, provider_id, distribution, catalog_properties, legacy_key, runtime_id) = {
            let mut state = self.lock_state()?;
            let runtime_id = state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&runtime_id).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let write = generation.binding.write().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no distributed write capability",
                )
            })?;
            let provider_id = generation.binding.descriptor().provider_id.clone();
            let distribution = generation.binding.execution_distribution().clone();
            let catalog_properties = generation.binding.catalog_properties()?.clone();
            generation.write_leases = generation.write_leases.saturating_add(1);
            (
                write,
                provider_id,
                distribution,
                catalog_properties,
                generation.legacy_execution_key.clone(),
                runtime_id,
            )
        };
        let state = Arc::downgrade(&self.state);
        ConnectorWriteLease::new_with_execution_distribution(
            runtime_id,
            legacy_key,
            write,
            provider_id,
            distribution,
            move || release_lease(&state, runtime_id, LeaseKind::Write),
        )
        .and_then(|lease| lease.with_catalog_properties(catalog_properties))
    }

    fn acquire_statistics(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorStatisticsLease, ConnectorError> {
        let (descriptor, incarnation, statistics, runtime_id) = {
            let mut state = self.lock_state()?;
            let runtime_id = state.active.get(instance_id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&runtime_id).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let statistics = generation.binding.statistics().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no statistics capability",
                )
            })?;
            generation.statistics_leases = generation.statistics_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                statistics,
                runtime_id,
            )
        };
        let state = Arc::downgrade(&self.state);
        ConnectorStatisticsLease::new(descriptor, incarnation, statistics, move || {
            release_lease(&state, runtime_id, LeaseKind::Statistics);
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ControlHostState>, ConnectorError> {
        self.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector control host lock poisoned",
            )
        })
    }
}
impl ConnectorControlResolver for ConnectorControlHost {
    fn observe_current_binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
        let state = self.lock_state()?;
        let runtime_id = state.active.get(instance_id).copied().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })?;
        let generation = state.generations.get(&runtime_id).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "active connector control generation is missing",
            )
        })?;
        if generation.state != ControlGenerationState::Active {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "connector control generation is retiring",
            ));
        }
        Ok(generation.legacy_execution_key.clone())
    }

    fn observe_current_control_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        let state = self.lock_state()?;
        let runtime_id = state.active.get(instance_id).copied().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })?;
        let generation = state.generations.get(&runtime_id).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "active connector control generation is missing",
            )
        })?;
        if generation.state != ControlGenerationState::Active {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "connector control generation is retiring",
            ));
        }
        Ok(runtime_id)
    }

    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        self.acquire(instance_id)
    }
}

impl ConnectorControlFactoryResolver for ConnectorControlHost {
    fn create_control(
        &self,
        request: ConnectorControlFactoryRequest,
    ) -> Result<novarocks_spi::connector::ConnectorControlCreation, ConnectorError> {
        let factory = self.factories.get(request.provider_id()).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector control factory for provider `{}` is not installed",
                    request.provider_id().as_str()
                ),
            )
        })?;
        // Do not acquire the generation mutex while invoking provider code.
        // Providers may resolve local clients or perform validation that
        // re-enters frontend-owned lifecycle paths.
        let creation = factory.create_control(request.clone())?;
        let descriptor = creation.binding().descriptor();
        if descriptor.provider_id != *request.provider_id()
            || descriptor.instance_id != *request.instance_id()
        {
            return Err(invalid(
                "connector control factory returned a binding for a different owner",
            ));
        }
        Ok(creation)
    }
}

impl ConnectorCatalogMutationResolver for ConnectorControlHost {
    fn acquire_current_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        self.acquire_mutation(instance_id)
    }

    fn acquire_exact_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        Self::acquire_exact_mutation(self, control_runtime_id, false)
    }
}

impl ConnectorDataMutationResolver for ConnectorControlHost {
    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        Self::acquire_current_data_mutation(self, instance_id)
    }

    fn acquire_exact_data_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        Self::acquire_exact_data_mutation(self, control_runtime_id)
    }
}

impl ConnectorMetadataMaintenanceResolver for ConnectorControlHost {
    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        Self::acquire_current_metadata_maintenance(self, instance_id)
    }

    fn acquire_exact_metadata_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        Self::acquire_exact_metadata_maintenance(self, control_runtime_id)
    }
}

impl ConnectorCleanupMaintenanceResolver for ConnectorControlHost {
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        Self::acquire_current_cleanup_maintenance(self, instance_id)
    }

    fn acquire_exact_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        Self::acquire_exact_cleanup_maintenance(self, control_runtime_id)
    }
}

impl ConnectorDistributedRewriteResolver for ConnectorControlHost {
    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        Self::acquire_current_distributed_rewrite(self, instance_id)
    }

    fn acquire_exact_distributed_rewrite(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        Self::acquire_exact_distributed_rewrite(self, control_runtime_id)
    }
}

impl ConnectorStatisticsResolver for ConnectorControlHost {
    fn acquire_current_statistics(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorStatisticsLease, ConnectorError> {
        self.acquire_statistics(instance_id)
    }
}

impl ConnectorControlRegistry for ConnectorControlHost {
    fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError> {
        Self::register(self, binding)
    }

    fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        Self::retire_current(self, instance_id)
    }
}

#[derive(Clone, Copy)]
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
enum LeaseKind {
    Planning,
    Mutation,
    DataMutation,
    MetadataMaintenance,
    DistributedRewrite,
    CleanupMaintenance,
    Write,
    Statistics,
}

fn release_lease(
    state: &Weak<Mutex<ControlHostState>>,
    runtime_id: ConnectorControlRuntimeId,
    kind: LeaseKind,
) {
    let Some(host_state) = state.upgrade() else {
        return;
    };
    let Ok(mut state) = host_state.lock() else {
        return;
    };
    let Some(generation) = state.generations.get_mut(&runtime_id) else {
        return;
    };
    match kind {
        LeaseKind::Planning => {
            generation.planning_leases = generation.planning_leases.saturating_sub(1);
        }
        LeaseKind::Mutation => {
            generation.mutation_leases = generation.mutation_leases.saturating_sub(1);
        }
        LeaseKind::DataMutation => {
            generation.data_mutation_leases = generation.data_mutation_leases.saturating_sub(1);
        }
        LeaseKind::MetadataMaintenance => {
            generation.metadata_maintenance_leases =
                generation.metadata_maintenance_leases.saturating_sub(1);
        }
        LeaseKind::DistributedRewrite => {
            generation.distributed_rewrite_leases =
                generation.distributed_rewrite_leases.saturating_sub(1);
        }
        LeaseKind::CleanupMaintenance => {
            generation.cleanup_maintenance_leases =
                generation.cleanup_maintenance_leases.saturating_sub(1);
        }
        LeaseKind::Write => {
            generation.write_leases = generation.write_leases.saturating_sub(1);
        }
        LeaseKind::Statistics => {
            generation.statistics_leases = generation.statistics_leases.saturating_sub(1);
        }
    }
    if generation.state == ControlGenerationState::Retiring && generation.all_leases_released() {
        if let Some(retirement) = queue_retirement(&mut state, runtime_id) {
            state.ready_retires.push(retirement);
        }
    }
}

fn queue_retirement(
    state: &mut ControlHostState,
    runtime_id: ConnectorControlRuntimeId,
) -> Option<ConnectorControlRetirement> {
    let Some(generation) = state.generations.remove(&runtime_id) else {
        return None;
    };
    debug_assert_eq!(generation.state, ControlGenerationState::Retiring);
    state.retired.insert(runtime_id);
    state
        .legacy_execution_index
        .remove(&generation.legacy_execution_key);
    let installed_backends = state
        .installed_backends
        .remove(&generation.legacy_execution_key)
        .unwrap_or_default()
        .into_iter()
        .collect();
    Some(ConnectorControlRetirement {
        key: generation.legacy_execution_key,
        installed_backends,
    })
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_connector_starrocks::{
        StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksControlGeneration,
        StarRocksMetadataSource, StarRocksResolvedTable,
    };
    use novarocks_spi::connector::{
        ConnectorBeginScanRequest, ConnectorControlCreation, ConnectorControlFactory,
        ConnectorControlFactoryRequest, ConnectorControlFactoryResolver, ConnectorError,
        ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
        ConnectorInstanceIncarnation, ConnectorListTablesRequest, ConnectorMetadata,
        ConnectorNamespaceRequest, ConnectorProviderId, ConnectorScan, ConnectorScanHandle,
        ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorTableHandle,
        ConnectorTableMetadata, ConnectorTableRequest,
    };

    use super::*;

    struct TestControlFactory {
        provider_id: ConnectorProviderId,
    }

    impl ConnectorControlFactory for TestControlFactory {
        fn provider_id(&self) -> &ConnectorProviderId {
            &self.provider_id
        }

        fn create_control(
            &self,
            _request: ConnectorControlFactoryRequest,
        ) -> Result<ConnectorControlCreation, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test factory does not create controls",
            ))
        }
    }

    struct OwnerMismatchFactory;

    impl ConnectorControlFactory for OwnerMismatchFactory {
        fn provider_id(&self) -> &ConnectorProviderId {
            // The static provider ID is only used for factory-map lookup. The
            // returned binding intentionally belongs to another instance.
            static PROVIDER: std::sync::OnceLock<ConnectorProviderId> = std::sync::OnceLock::new();
            PROVIDER.get_or_init(|| ConnectorProviderId::parse("iceberg").expect("provider ID"))
        }

        fn create_control(
            &self,
            request: ConnectorControlFactoryRequest,
        ) -> Result<ConnectorControlCreation, ConnectorError> {
            let wrong_request = ConnectorControlFactoryRequest::try_new(
                request.provider_id().clone(),
                ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
                Vec::new(),
            )?;
            ConnectorControlCreation::try_new(&wrong_request, binding(1), Vec::new())
        }
    }

    #[test]
    fn factory_host_rejects_duplicate_provider_ids_before_startup() {
        let provider_id = ConnectorProviderId::parse("test").expect("provider ID");
        let first: Arc<dyn ConnectorControlFactory> = Arc::new(TestControlFactory {
            provider_id: provider_id.clone(),
        });
        let second: Arc<dyn ConnectorControlFactory> = Arc::new(TestControlFactory { provider_id });
        let error = match ConnectorControlHost::with_factories(vec![first, second]) {
            Ok(_) => panic!("duplicate provider factory must fail fast"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("duplicate connector control factory")
        );
    }

    #[test]
    fn factory_host_rejects_requests_for_uninstalled_providers() {
        let host = ConnectorControlHost::new();
        let request = ConnectorControlFactoryRequest::try_new(
            ConnectorProviderId::parse("iceberg").expect("provider ID"),
            ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
            Vec::new(),
        )
        .expect("factory request");

        let error = match ConnectorControlFactoryResolver::create_control(&host, request) {
            Ok(_) => panic!("missing provider factory must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
        assert!(error.to_string().contains("is not installed"));
        assert!(
            host.observe_current_binding(
                &ConnectorInstanceId::parse("catalog.analytics").expect("instance ID")
            )
            .is_err(),
            "a missing factory must not publish a generation"
        );
    }

    #[test]
    fn factory_host_propagates_provider_error_without_publishing_generation() {
        let provider_id = ConnectorProviderId::parse("test").expect("provider ID");
        let factory: Arc<dyn ConnectorControlFactory> = Arc::new(TestControlFactory {
            provider_id: provider_id.clone(),
        });
        let host = ConnectorControlHost::with_factories(vec![factory]).expect("factory host");
        let request = ConnectorControlFactoryRequest::try_new(
            provider_id,
            ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
            Vec::new(),
        )
        .expect("factory request");
        let error = match ConnectorControlFactoryResolver::create_control(&host, request) {
            Ok(_) => panic!("provider factory error must be returned"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(
            host.observe_current_binding(&ConnectorInstanceId::parse("catalog.analytics").unwrap())
                .is_err()
        );
    }

    #[test]
    fn factory_host_rejects_returned_binding_for_different_owner() {
        let factory: Arc<dyn ConnectorControlFactory> = Arc::new(OwnerMismatchFactory);
        let host = ConnectorControlHost::with_factories(vec![factory]).expect("factory host");
        let request = ConnectorControlFactoryRequest::try_new(
            ConnectorProviderId::parse("iceberg").expect("provider ID"),
            ConnectorInstanceId::parse("catalog.requested").expect("instance ID"),
            Vec::new(),
        )
        .expect("factory request");
        let error = match ConnectorControlFactoryResolver::create_control(&host, request) {
            Ok(_) => panic!("factory owner mismatch must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("different owner"));
    }

    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct StarRocksFixtureSource;

    impl StarRocksMetadataSource for StarRocksFixtureSource {
        fn namespace_exists(
            &self,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn table_exists(
            &self,
            _: &str,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn list_tables(
            &self,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<Vec<String>, ConnectorError> {
            Ok(vec![])
        }
        fn load_table(
            &self,
            _: &str,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<StarRocksResolvedTable, ConnectorError> {
            StarRocksResolvedTable::try_new(
                "db",
                "table",
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksCapabilitySnapshot {
                    api_contract_version: 1,
                },
            )
        }
    }

    fn starrocks_binding() -> ConnectorControlBinding {
        let config = StarRocksConnectorConfig::new(
            ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID"),
            novarocks_connector_starrocks::StarRocksLocalBindingRef::parse("test")
                .expect("binding"),
        );
        StarRocksControlGeneration::try_new(config, Arc::new(StarRocksFixtureSource))
            .expect("StarRocks control binding")
    }

    fn starrocks_context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            16 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .expect("context")
    }

    struct TestControl {
        instance_id: ConnectorInstanceId,
        incarnation: ConnectorInstanceIncarnation,
    }

    impl ConnectorMetadata for TestControl {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Err(unsupported())
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(unsupported())
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            Err(unsupported())
        }

        fn load_table(
            &self,
            _request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(unsupported())
        }
    }

    impl ConnectorScanPlanning for TestControl {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            Err(unsupported())
        }

        fn plan_splits(
            &self,
            _scan: &ConnectorScanHandle,
            _request: ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            Err(unsupported())
        }
    }

    impl ConnectorExecutionDistribution for TestControl {
        fn declaration(
            &self,
            _context: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            ConnectorExecutionDeclaration::iceberg(
                self.instance_id.as_str(),
                self.incarnation.to_bytes(),
                "default",
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
        }
    }

    fn binding(incarnation: u8) -> ConnectorControlBinding {
        test_control_binding_for(
            ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
            incarnation,
        )
    }

    pub(crate) fn test_control_binding(incarnation: u8) -> ConnectorControlBinding {
        binding(incarnation)
    }

    /// A control binding for an arbitrary instance ID, so a factory fixture can
    /// answer whichever catalog name the request carries.
    pub(crate) fn test_control_binding_for(
        instance_id: ConnectorInstanceId,
        incarnation: u8,
    ) -> ConnectorControlBinding {
        let provider = Arc::new(TestControl {
            instance_id,
            incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
        });
        ConnectorControlBinding::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: provider.instance_id.clone(),
            },
            provider.incarnation,
            provider.clone(),
            provider.clone(),
            provider,
            None,
        )
        .expect("control binding")
    }

    #[test]
    fn observing_current_binding_does_not_require_a_planning_lease() {
        let host = ConnectorControlHost::new();
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
        let binding = binding(7);
        let control_runtime_id = binding.control_runtime_id();
        host.register(binding).expect("register generation");

        assert_eq!(
            host.observe_current_binding(&instance_id)
                .expect("observe active generation")
                .incarnation
                .to_bytes(),
            [7; 16]
        );
        assert_eq!(
            host.observe_current_control_runtime(&instance_id)
                .expect("observe active control runtime"),
            control_runtime_id
        );
        let planning_lease = host.acquire_current(&instance_id).expect("planning lease");
        assert_eq!(planning_lease.control_runtime_id(), control_runtime_id);
        drop(planning_lease);
        host.retire_current(&instance_id)
            .expect("retire unleased generation");
        assert!(
            host.acquire_current(&instance_id).is_err(),
            "an observation must not keep a retiring generation live"
        );
    }

    #[test]
    fn retiring_generation_waits_for_planning_lease_before_remote_retire() {
        let host = ConnectorControlHost::new();
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
        host.register(binding(7)).expect("register old generation");
        let lease = host.acquire_current(&instance_id).expect("planning lease");
        let old_key = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        host.record_installed_backend(&old_key, "be-1")
            .expect("record ensure ack");
        host.retire_current(&instance_id)
            .expect("retire old generation");
        assert!(host.take_ready_retires().expect("retire queue").is_empty());

        host.register(binding(8))
            .expect("register replacement generation");
        assert_eq!(lease.binding().incarnation().to_bytes(), [7; 16]);
        drop(lease);

        let ready = host.take_ready_retires().expect("retire queue");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].key, old_key);
        assert_eq!(ready[0].installed_backends, vec![String::from("be-1")]);
        assert_eq!(
            host.acquire_current(&instance_id)
                .expect("replacement planning lease")
                .binding()
                .incarnation()
                .to_bytes(),
            [8; 16]
        );
    }

    #[test]
    fn starrocks_control_host_keeps_the_retiring_generation_leased_and_accepts_its_replacement() {
        let host = ConnectorControlHost::new();
        let first = starrocks_binding();
        let instance = first.descriptor().instance_id.clone();
        let first_incarnation = first.incarnation();
        host.register(first)
            .expect("register first StarRocks generation");
        let lease = host
            .acquire_current(&instance)
            .expect("acquire first lease");
        let declaration = lease
            .binding()
            .execution_declaration(&starrocks_context())
            .expect("declaration");
        assert_eq!(
            declaration.binding_key().incarnation(),
            first_incarnation.to_bytes()
        );

        host.retire_current(&instance)
            .expect("retire first generation");
        host.register(starrocks_binding())
            .expect("register replacement generation");
        assert_eq!(lease.binding().incarnation(), first_incarnation);
        drop(lease);

        assert_ne!(
            host.acquire_current(&instance)
                .expect("acquire replacement")
                .binding()
                .incarnation(),
            first_incarnation
        );
    }

    fn unsupported() -> ConnectorError {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "test-only control capability",
        )
    }
}
