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

//! Statement-local `TRUNCATE TABLE` publication flow.
//!
//! Planning is read-only. Once this module marks dispatch possible, no error
//! or negative observation can authorize a follow-up mutation: the only
//! allowed follow-up is one read-only adjudication on the retained session.

use novarocks_proto::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    LakePublicationFamily, LakePublicationId, LakePublicationStatementTag, LakePublicationTarget,
    LakePublicationTerminal,
};

use crate::common::admitted_query_context::RequestContext;
use crate::dml::attempt::{
    DmlPublicationAdjudication, DmlPublicationAdjudicationOutcome, DmlPublicationAttempt,
    DmlPublicationFinalization,
};
use crate::dml::error::DmlError;
use crate::dml::service::DmlService;
use crate::query_execution::dml::truncate::{
    PlanTruncateRequest, PreparedTruncate, TruncateCommand, TruncateEngine, TruncateFailure,
    TruncateFinalization, TruncateOutcome, TruncatePlanError, TruncatePlanFacts,
};

impl DmlService {
    /// Executes an admitted TRUNCATE as one non-durable statement attempt.
    #[allow(clippy::result_large_err)]
    pub fn execute_truncate(
        &self,
        engine: &dyn TruncateEngine,
        command: TruncateCommand,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let publication_id = LakePublicationId::new_v7();
        let session = context.session();
        let prepared = engine
            .plan_truncate(PlanTruncateRequest {
                command,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                mutation_operation_id: publication_id.to_bytes(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(plan_error)?;

        let mut attempt = attempt(&prepared.facts, publication_id)?;
        attempt
            .mark_dispatch_possible()
            .map_err(DmlError::executor)?;
        finish(engine, prepared, &mut attempt)
    }
}

fn attempt(
    facts: &TruncatePlanFacts,
    publication_id: LakePublicationId,
) -> Result<DmlPublicationAttempt, DmlError> {
    let target = LakePublicationTarget::try_new(
        facts.catalog.clone(),
        facts.namespace.clone(),
        Some(facts.table.clone()),
        Some(facts.target_ref.clone()),
    )
    .map_err(DmlError::executor)?;
    let tag =
        LakePublicationStatementTag::try_new("truncate".to_string()).map_err(DmlError::executor)?;
    Ok(DmlPublicationAttempt::new(
        publication_id,
        LakePublicationFamily::DataMutation,
        target,
        Some(tag),
    ))
}

fn finish(
    engine: &dyn TruncateEngine,
    prepared: PreparedTruncate,
    attempt: &mut DmlPublicationAttempt,
) -> Result<(), DmlError> {
    match engine.execute_truncate(prepared.handle.as_ref()) {
        TruncateOutcome::KnownCommitted { finalization, .. } => committed(attempt, finalization),
        TruncateOutcome::CommitUnknown {
            failure: _,
            evidence,
        } => {
            let capability = attempt.begin_adjudication().map_err(DmlError::executor)?;
            match engine.adjudicate_truncate(prepared.handle.as_ref(), &evidence) {
                TruncateOutcome::KnownCommitted { finalization, .. } => {
                    committed_after_adjudication(attempt, capability, finalization)
                }
                TruncateOutcome::CommitUnknown { failure, .. }
                | TruncateOutcome::KnownUncommitted { failure }
                | TruncateOutcome::ContractFailure { failure, .. } => {
                    unknown_after_adjudication(attempt, capability, failure)
                }
            }
        }
        // Dispatch was fenced before the call. A provider's claimed
        // known-uncommitted result cannot relax that conservative boundary.
        TruncateOutcome::KnownUncommitted { failure }
        | TruncateOutcome::ContractFailure { failure, .. } => unknown(attempt, failure),
    }
}

fn committed(
    attempt: &mut DmlPublicationAttempt,
    finalization: TruncateFinalization,
) -> Result<(), DmlError> {
    let finalization = finalization_state(&finalization);
    let terminal = attempt
        .terminal_known_committed(finalization)
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization)
}

fn committed_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    finalization: TruncateFinalization,
) -> Result<(), DmlError> {
    let finalization = finalization_state(&finalization);
    let terminal = attempt
        .finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::KnownCommitted,
            finalization,
        )
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization)
}

fn unknown(attempt: &mut DmlPublicationAttempt, failure: TruncateFailure) -> Result<(), DmlError> {
    let terminal = attempt
        .terminal_commit_unknown()
        .map_err(DmlError::executor)?
        .clone();
    Err(DmlError::commit(failure.message).with_publication_terminal(terminal))
}

fn unknown_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    failure: TruncateFailure,
) -> Result<(), DmlError> {
    let terminal = attempt
        .finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::CommitUnknown,
            DmlPublicationFinalization::NotApplicable,
        )
        .map_err(DmlError::executor)?
        .clone();
    Err(DmlError::commit(failure.message).with_publication_terminal(terminal))
}

fn finalization_state(finalization: &TruncateFinalization) -> DmlPublicationFinalization {
    match finalization {
        TruncateFinalization::Complete => DmlPublicationFinalization::Succeeded,
        TruncateFinalization::Failed(_) => DmlPublicationFinalization::Failed,
    }
}

fn finalization_error(
    terminal: LakePublicationTerminal,
    finalization: DmlPublicationFinalization,
) -> Result<(), DmlError> {
    if finalization == DmlPublicationFinalization::Failed {
        return Err(DmlError::known_committed_finalization_failed(
            terminal,
            "TRUNCATE finalization failed",
        ));
    }
    Ok(())
}

fn plan_error(error: TruncatePlanError) -> DmlError {
    match error {
        TruncatePlanError::KnownUncommitted(failure)
        | TruncatePlanError::ContractFailure { failure, .. } => DmlError::executor(failure.message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_truncate_uses_data_mutation_marker_family() {
        assert_eq!(
            LakePublicationFamily::DataMutation.as_str(),
            "data_mutation"
        );
    }
}
