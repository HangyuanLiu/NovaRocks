// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use crate::api::QueryExecutionKind;
use crate::coordination::{ExecutionEffect, RecoveryMode};

use super::CompletedPhysicalPlanCandidate;

/// What one statement delivers.
///
/// A row-producing statement names each column it returns; a statement whose
/// only result is that it finished names none. The fields are the client's
/// own view of the result, which is all any consumer of this contract has
/// ever read from it.
#[derive(Clone, Debug)]
pub enum OutputContract {
    Rows(Arc<[crate::api::ResultField]>),
    CompletionOnly,
}

impl OutputContract {
    /// The same contract, from a completed plan's own result port.
    ///
    /// A completed plan states what it delivers as part of being complete:
    /// the port names each field, and the name is the alias where the
    /// statement gave one, which is the name the client asked for.
    pub fn from_completed_plan(
        kind: QueryExecutionKind,
        plan: &novarocks_physical_plan::PhysicalPlan,
    ) -> Result<Self, String> {
        // A write plan's root port carries the connector commit relation for
        // its internal finish. It is not a client row result.
        if kind == QueryExecutionKind::Write {
            return Ok(Self::CompletionOnly);
        }
        match plan.result_port() {
            Some(result) if !result.fields.is_empty() => Ok(Self::Rows(
                result
                    .fields
                    .iter()
                    .map(|field| {
                        crate::api::ResultField::new(
                            field.alias.as_deref().unwrap_or(&field.name).to_string(),
                            field.ty.data_type.clone(),
                            field.ty.nullable,
                            None,
                        )
                    })
                    .collect(),
            )),
            _ if kind == QueryExecutionKind::Read => {
                Err("completed read plan has no row output contract".to_string())
            }
            _ => Ok(Self::CompletionOnly),
        }
    }

    pub fn fields(&self) -> &[crate::api::ResultField] {
        match self {
            Self::Rows(fields) => fields,
            Self::CompletionOnly => &[],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ResidualResponsibility {
    EngineFilter,
    EngineLimit,
    EffectCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenEstimateUnknownReason {
    MissingRootFragment,
    FallbackRowEstimate,
    MissingCostEstimate,
    NonFinite,
    Negative,
    NotProjected,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FrozenCostValue {
    Known(f64),
    Unknown(FrozenEstimateUnknownReason),
}

impl FrozenCostValue {
    pub const fn known(self) -> Option<f64> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }

    pub const fn unknown_reason(self) -> Option<FrozenEstimateUnknownReason> {
        match self {
            Self::Known(_) => None,
            Self::Unknown(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrozenCostEstimate {
    root_rows: FrozenCostValue,
    cpu: FrozenCostValue,
    memory: FrozenCostValue,
    network: FrozenCostValue,
}

impl FrozenCostEstimate {
    pub const fn new(
        root_rows: FrozenCostValue,
        cpu: FrozenCostValue,
        memory: FrozenCostValue,
        network: FrozenCostValue,
    ) -> Self {
        Self {
            root_rows,
            cpu,
            memory,
            network,
        }
    }

    pub const fn unknown(reason: FrozenEstimateUnknownReason) -> Self {
        Self::new(
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
        )
    }

    pub const fn root_rows(self) -> FrozenCostValue {
        self.root_rows
    }

    pub const fn cpu(self) -> FrozenCostValue {
        self.cpu
    }

    pub const fn memory(self) -> FrozenCostValue {
        self.memory
    }

    pub const fn network(self) -> FrozenCostValue {
        self.network
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenResourceValue {
    Known(u64),
    Unknown(FrozenEstimateUnknownReason),
}

impl FrozenResourceValue {
    pub const fn known(self) -> Option<u64> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }

    pub const fn unknown_reason(self) -> Option<FrozenEstimateUnknownReason> {
        match self {
            Self::Known(_) => None,
            Self::Unknown(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionResourceRequirements {
    minimum_memory_bytes: FrozenResourceValue,
    result_credit_bytes: FrozenResourceValue,
    spill_bytes: FrozenResourceValue,
}

impl ExecutionResourceRequirements {
    pub const fn new(
        minimum_memory_bytes: FrozenResourceValue,
        result_credit_bytes: FrozenResourceValue,
        spill_bytes: FrozenResourceValue,
    ) -> Self {
        Self {
            minimum_memory_bytes,
            result_credit_bytes,
            spill_bytes,
        }
    }

    pub const fn unknown(reason: FrozenEstimateUnknownReason) -> Self {
        Self::new(
            FrozenResourceValue::Unknown(reason),
            FrozenResourceValue::Unknown(reason),
            FrozenResourceValue::Unknown(reason),
        )
    }

    pub const fn minimum_memory_bytes(self) -> FrozenResourceValue {
        self.minimum_memory_bytes
    }

    pub const fn result_credit_bytes(self) -> FrozenResourceValue {
        self.result_credit_bytes
    }

    pub const fn spill_bytes(self) -> FrozenResourceValue {
        self.spill_bytes
    }
}

/// Immutable semantic input shared by every attempt of one completed plan.
#[derive(Clone, Debug)]
pub struct FrozenExecutionDescription {
    plan: crate::api::PlanSeal,
    candidate: CompletedPhysicalPlanCandidate,
    kind: QueryExecutionKind,
    scan_identities: Arc<[crate::api::PlanScanIdentity]>,
    output: OutputContract,
    effect: ExecutionEffect,
    recovery: RecoveryMode,
    residuals: Arc<[ResidualResponsibility]>,
    cost: FrozenCostEstimate,
    resources: ExecutionResourceRequirements,
}

impl FrozenExecutionDescription {
    /// Freeze the exact candidate and scan cover as one logical execution.
    pub fn for_completed_plan(
        kind: QueryExecutionKind,
        candidate: super::CompletedPhysicalPlanCandidate,
        scan_identities: Vec<crate::api::PlanScanIdentity>,
        output: OutputContract,
        effect: ExecutionEffect,
        recovery: RecoveryMode,
        residuals: Vec<ResidualResponsibility>,
        cost: FrozenCostEstimate,
        resources: ExecutionResourceRequirements,
    ) -> Result<Self, String> {
        validate_frozen_cost(cost)?;
        validate_effect_recovery(effect, recovery)?;
        let plan = candidate.plan().version();
        let expected_output = OutputContract::from_completed_plan(kind, candidate.plan())?;
        if output.fields() != expected_output.fields()
            || matches!(output, OutputContract::CompletionOnly)
                != matches!(expected_output, OutputContract::CompletionOnly)
        {
            return Err(
                "completed plan output differs from its frozen execution contract".to_string(),
            );
        }
        let expected_scans = candidate
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| {
                fragment.nodes().values().filter_map(|node| {
                    matches!(&node.kind, novarocks_physical_plan::NodeKind::Scan { .. }).then(
                        || {
                            let node_id = i32::try_from(node.id.get()).map_err(|_| {
                                "completed plan scan node id exceeds native scheduling range"
                                    .to_string()
                            })?;
                            Ok(crate::api::PlanScanIdentity::new(
                                crate::api::PlanSeal::Version(plan),
                                fragment.id().get(),
                                node_id,
                            ))
                        },
                    )
                })
            })
            .collect::<Result<std::collections::BTreeSet<_>, String>>()?;
        let supplied_scans = scan_identities
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if supplied_scans.len() != scan_identities.len() || supplied_scans != expected_scans {
            return Err(
                "completed plan scans differ from its frozen execution contract".to_string(),
            );
        }
        Ok(Self {
            plan: crate::api::PlanSeal::Version(plan),
            candidate,
            kind,
            scan_identities: scan_identities.into(),
            output,
            effect,
            recovery,
            residuals: residuals.into(),
            cost,
            resources,
        })
    }

    pub const fn kind(&self) -> QueryExecutionKind {
        self.kind
    }

    pub(crate) const fn plan_identity(&self) -> crate::api::PlanSeal {
        self.plan
    }

    pub(crate) const fn completed_candidate(&self) -> &CompletedPhysicalPlanCandidate {
        &self.candidate
    }

    pub fn scan_identities(&self) -> &[crate::api::PlanScanIdentity] {
        &self.scan_identities
    }

    pub fn matches_plan_seal(&self, plan: crate::api::PlanSeal) -> bool {
        self.plan_identity() == plan
    }

    pub const fn output(&self) -> &OutputContract {
        &self.output
    }
    pub const fn effect(&self) -> ExecutionEffect {
        self.effect
    }
    pub const fn recovery(&self) -> RecoveryMode {
        self.recovery
    }
    pub fn residuals(&self) -> &[ResidualResponsibility] {
        &self.residuals
    }
    pub const fn cost(&self) -> FrozenCostEstimate {
        self.cost
    }
    pub const fn resources(&self) -> ExecutionResourceRequirements {
        self.resources
    }
}

/// Every known cost is a finite, non-negative quantity.
fn validate_frozen_cost(cost: FrozenCostEstimate) -> Result<(), String> {
    for (name, value) in [
        ("root rows", cost.root_rows()),
        ("cpu", cost.cpu()),
        ("memory", cost.memory()),
        ("network", cost.network()),
    ] {
        if let FrozenCostValue::Known(value) = value
            && (!value.is_finite() || value < 0.0)
        {
            return Err(format!(
                "frozen {name} cost must be finite and non-negative"
            ));
        }
    }
    Ok(())
}

/// An execution that commits outside this process cannot be retried by
/// running it again.
fn validate_effect_recovery(effect: ExecutionEffect, recovery: RecoveryMode) -> Result<(), String> {
    if matches!(effect, ExecutionEffect::External) && !matches!(recovery, RecoveryMode::NoRecovery)
    {
        return Err("external effects cannot use replay recovery".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completed_description_rejects_a_substituted_output_or_scan() {
        let candidate = crate::completed_plan_fixture::completed_values_plan([13; 16])
            .await
            .candidate()
            .clone();
        let output =
            OutputContract::from_completed_plan(QueryExecutionKind::Read, candidate.plan())
                .expect("values plan has a row result");
        let freeze = |candidate, scans, output| {
            FrozenExecutionDescription::for_completed_plan(
                QueryExecutionKind::Read,
                candidate,
                scans,
                output,
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            )
        };
        assert!(
            freeze(
                candidate.clone(),
                Vec::new(),
                OutputContract::CompletionOnly
            )
            .is_err()
        );
        assert!(
            freeze(
                candidate.clone(),
                vec![crate::api::PlanScanIdentity::new(
                    crate::api::PlanSeal::Version(candidate.plan().version()),
                    1,
                    1,
                )],
                output.clone(),
            )
            .is_err()
        );
        assert!(freeze(candidate, Vec::new(), output).is_ok());
    }

    #[test]
    fn cost_and_effect_recovery_are_checked_before_publication() {
        assert!(
            validate_frozen_cost(FrozenCostEstimate::new(
                FrozenCostValue::Known(f64::NAN),
                FrozenCostValue::Unknown(FrozenEstimateUnknownReason::NotProjected),
                FrozenCostValue::Unknown(FrozenEstimateUnknownReason::NotProjected),
                FrozenCostValue::Unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .is_err()
        );
        assert!(
            validate_effect_recovery(
                ExecutionEffect::External,
                RecoveryMode::RestartAttemptBeforeVisibility,
            )
            .is_err()
        );
    }
}
