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

//! Frontend-owned durable CTAS application saga.
//!
//! Core retains the admitted source artifact and provider-private staged
//! handles. The frontend owns only bounded neutral facts, stable child
//! operation IDs and the durable ordering barriers around every external
//! effect.

use crate::common::admitted_query_context::RequestContext;
use crate::query_execution::dml::ctas::{
    CtasCommand, CtasEngine, CtasFailure, CtasFailureKind, CtasTargetPreflightOutcome,
    PrepareCtasSourceRequest, PreparedCtasSource, PreparedStandardCtasTarget,
    PreparedStandardCtasWrite, StandardCtasPublishOutcome, StandardCtasStageOutcome,
    StandardCtasTargetFacts, StandardCtasWriteOutcome,
};
use novarocks_proto::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorWriteOperationCompletion, ConnectorWriteOperationId, CreatePolicy,
    ExternalMutationEvidence,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::coordination::ActiveDmlOperation;
use crate::dml::error::{AdmitError, DmlError, DmlErrorKind};
use crate::dml::model::{
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord, DML_CTAS_FACT_ENCODED_LIMIT,
    DmlOperationId, DurableExternalFact, ExternalFactOutcome, OperationKind, OperationPayload,
    OperationState, OperationTarget, StatementNextAction, StoredOperation,
};
use crate::dml::service::DmlService;

const DURABLE_CTAS_FACT_VERSION: u8 = 1;
const DURABLE_FAILURE_PREFIX_BYTES: usize = 2 * 1024;

#[derive(Serialize)]
struct DurableCtasWriteCompletionV1 {
    version: u8,
    instance_id: String,
    incarnation: String,
    operation_id: String,
    cohort_id: String,
    cohort_set_digest: String,
    aggregate_digest: String,
}

#[derive(Serialize)]
struct DurableCtasFailureV1<'a> {
    version: u8,
    kind: &'static str,
    message_prefix: &'a str,
    message_truncated: bool,
    original_message_bytes: usize,
    original_message_sha256: String,
}

impl DmlService {
    /// Execute an already-admitted CTAS through the frontend durable saga owner.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn try_execute_ctas(
        &self,
        engine: &dyn CtasEngine,
        statement: &novarocks_parser::ast::CreateTableAsSelect,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let command = CtasCommand::from_typed(statement, source).map_err(|error| {
            DmlError::admit(AdmitError::CreateTableUnsupportedForm.to_user_error(
                source,
                error.span,
                error.message,
            ))
        })?;
        let session = context.session();
        let operation_id = DmlOperationId::new_v7();
        // The standard staged-create adapter uses this exact ID to derive the
        // warehouse-owned, unanchored CTAS root.  It must therefore be the
        // admission publication ID, not a second child UUID.
        let prepare_operation_id = *operation_id.as_uuid();
        // These fields retain their phase names for durable-payload
        // compatibility, but they are not child attempts. A CTAS statement
        // has exactly one publication identity across prepare, write,
        // publication and any diagnostic record.
        let write_operation_id = prepare_operation_id;
        let publish_operation_id = prepare_operation_id;
        let abort_staging_operation_id = prepare_operation_id;
        let policy = if command.if_not_exists {
            CreatePolicy::NoOpIfExists
        } else {
            CreatePolicy::FailIfExists
        };
        let initial = CtasSagaRecord {
            phase: CtasSagaPhase::PreparingSource,
            prepare_operation_id,
            write_operation_id,
            publish_operation_id,
            abort_staging_operation_id,
            create_policy: policy_name(policy).to_string(),
            provider_id: None,
            connector_instance_id: None,
            connector_incarnation: None,
            source_plan_digest: None,
            source_schema_digest: None,
            source_execution_identity: None,
            write_cohort_id: None,
            staged_handle_digest: None,
            write_cohort_set_digest: None,
            aggregate_write_digest: None,
            prepare_fact: None,
            write_fact: None,
            publish_fact: None,
            abort_staging_fact: None,
            next_action: StatementNextAction::None,
        };
        let mut active = self
            .begin_statement_operation(CreateStatementOperationRequest {
                operation_id,
                mutation_id: Uuid::now_v7(),
                operation_kind: OperationKind::CreateTableAsSelect,
                target: syntactic_target(
                    &command.target_parts,
                    session.current_catalog(),
                    session.current_database(),
                ),
                attempt_id: operation_id.to_string(),
                payload: OperationPayload::CtasSaga(initial),
                created_at_ms: crate::dml::now_unix_millis(),
            })
            .map_err(|error| journal_error(error, operation_id))?;

        let result = execute_standard_ctas_operation(
            engine,
            statement,
            source,
            context,
            query_options,
            command,
            prepare_operation_id,
            policy,
            &mut active,
        );
        let _ = active.release();
        result
    }
}

/// Execute CTAS through the crash-only staged-create contract.  The only
/// externally visible frontier is standard staged-create publication; frontend
/// neither reconstructs catalog authority nor cleans up a possibly-live staged
/// target.  Definite failures are terminal and all ambiguous outcomes remain
/// explicitly inspectable until age-based GC retires their owned roots.
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
#[expect(
    clippy::too_many_arguments,
    reason = "The statement boundary keeps its source, session, and durable owner explicit."
)]
fn execute_standard_ctas_operation(
    engine: &dyn CtasEngine,
    statement: &novarocks_parser::ast::CreateTableAsSelect,
    source: &str,
    context: &RequestContext,
    query_options: Option<&QueryOptions>,
    command: CtasCommand,
    prepare_operation_id: Uuid,
    policy: CreatePolicy,
    active: &mut ActiveDmlOperation,
) -> Result<(), DmlError> {
    let session = context.session();
    let statement_source = source;
    let publication_id =
        novarocks_spi::connector::LakePublicationId::from_bytes(*prepare_operation_id.as_bytes());

    active.check_before_dispatch()?;
    let preflight = match engine.preflight_standard_ctas_target(
        statement,
        source,
        &command,
        session.current_catalog(),
        session.current_database(),
    ) {
        Ok(CtasTargetPreflightOutcome::ExistsNoOp) => {
            return finalize_standard_ctas_noop(active);
        }
        Ok(CtasTargetPreflightOutcome::Ready(preflight)) => preflight,
        Err(failure) => {
            return finish_source_failure(active, active.stored.clone(), source, failure);
        }
    };
    validate_preflight_facts(&active.stored, &preflight.facts)?;

    let source = match engine.prepare_standard_ctas_source(
        preflight.handle.as_ref(),
        PrepareCtasSourceRequest {
            command,
            current_catalog: session.current_catalog().map(ToOwned::to_owned),
            current_database: session.current_database().to_string(),
            query_options: query_options.cloned(),
            execution: context.execution().clone(),
        },
    ) {
        Ok(source) => source,
        Err(failure) => {
            return finish_source_failure(active, active.stored.clone(), source, failure);
        }
    };
    validate_source_facts(
        &active.stored,
        &source,
        session.current_catalog(),
        session.current_database(),
    )?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::PreparingStagedTable;
    saga.provider_id = Some(preflight.facts.provider_id.clone());
    saga.connector_instance_id = Some(preflight.facts.instance_id.clone());
    saga.connector_incarnation = Some(hex::encode(preflight.facts.incarnation));
    saga.source_plan_digest = Some(hex::encode(source.facts.plan_digest));
    saga.source_schema_digest = Some(hex::encode(source.facts.schema_digest));
    saga.source_execution_identity = Some(hex::encode(source.facts.execution_identity));
    saga.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::Preparing,
        OperationPayload::CtasSaga(saga),
        None,
    )?;

    let stage =
        match engine.prepare_standard_ctas_target(source.handle.as_ref(), publication_id, policy) {
            Ok(action) => {
                active.check_before_dispatch()?;
                engine.stage_standard_ctas_target(action.handle.as_ref())
            }
            Err(failure) => {
                return finish_standard_known_uncommitted(
                    active,
                    CtasSagaPhase::Failed,
                    FactSlot::Prepare,
                    failure,
                    Some(statement_source),
                );
            }
        };
    let target = match stage {
        Ok(StandardCtasStageOutcome::Prepared { target, receipt }) => {
            validate_standard_target_facts(&active.stored, &preflight.facts, &target.facts)?;
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::Staged;
            saga.staged_handle_digest = Some(hex::encode(target.facts.target_handle_digest));
            saga.prepare_fact = Some(staged_create_fact(
                ExternalFactOutcome::KnownCommitted,
                &receipt,
            ));
            saga.next_action = StatementNextAction::None;
            active.mutate_statement(
                OperationState::Writing,
                OperationPayload::CtasSaga(saga),
                None,
            )?;
            target
        }
        Ok(StandardCtasStageOutcome::KnownUncommitted { failure }) => {
            return finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Prepare,
                failure,
                Some(statement_source),
            );
        }
        Ok(StandardCtasStageOutcome::CommitUnknown { failure, evidence }) => {
            return finish_standard_unknown(
                active,
                CtasSagaPhase::PrepareUnknown,
                FactSlot::Prepare,
                failure,
                evidence,
                "CTAS staged-create",
            );
        }
        Err(failure) => {
            return finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Prepare,
                failure,
                Some(statement_source),
            );
        }
    };

    execute_standard_foreground_write(engine, active, source, target, publication_id)
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn execute_standard_foreground_write(
    engine: &dyn CtasEngine,
    active: &mut ActiveDmlOperation,
    source: PreparedCtasSource,
    target: PreparedStandardCtasTarget,
    publication_id: novarocks_spi::connector::LakePublicationId,
) -> Result<(), DmlError> {
    let prepared = match engine.prepare_standard_ctas_write(
        source.handle.as_ref(),
        target.handle.as_ref(),
        ConnectorWriteOperationId::from(publication_id),
    ) {
        Ok(prepared) => prepared,
        Err(failure) => {
            return finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Write,
                failure,
                None,
            );
        }
    };
    let native_bundle = match standard_native_bundle(&prepared) {
        Ok(bundle) => bundle,
        Err(failure) => {
            return finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Write,
                failure,
                None,
            );
        }
    };
    if let Err(failure) =
        engine.bind_standard_ctas_write_native_bundle(prepared.handle.as_ref(), native_bundle)
    {
        return finish_standard_known_uncommitted(
            active,
            CtasSagaPhase::Failed,
            FactSlot::Write,
            failure,
            None,
        );
    }
    validate_standard_prepared_write(&active.stored, &source, &target, &prepared)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Writing;
    saga.write_cohort_set_digest = Some(hex::encode(prepared.cohort_set_digest));
    active.mutate_statement(
        OperationState::Writing,
        OperationPayload::CtasSaga(saga),
        None,
    )?;
    active.check_before_dispatch()?;
    match engine.execute_standard_ctas_write(prepared.handle.as_ref()) {
        StandardCtasWriteOutcome::Completed {
            completion,
            execution_identity,
        } => {
            validate_standard_completion(
                &active.stored,
                &source,
                &target,
                &prepared,
                &completion,
                execution_identity,
            )?;
            let (encoded, cohort) = encode_write_completion(&completion)?;
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::Publishing;
            saga.write_cohort_id = Some(cohort);
            saga.aggregate_write_digest = Some(hex::encode(completion.aggregate_digest()));
            saga.write_fact = Some(DurableExternalFact {
                outcome: ExternalFactOutcome::KnownCommitted,
                receipt: Some(encoded),
                evidence: None,
                finalization_failure: None,
                failure: None,
            });
            active.mutate_statement(
                OperationState::Committing,
                OperationPayload::CtasSaga(saga),
                None,
            )?;
            execute_standard_publish(engine, active, target, publication_id, completion)
        }
        StandardCtasWriteOutcome::KnownUncommitted { failure } => {
            finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Write,
                failure,
                None,
            )
        }
        StandardCtasWriteOutcome::CommitUnknown { failure, evidence } => finish_standard_unknown(
            active,
            CtasSagaPhase::WriteUnknown,
            FactSlot::Write,
            failure,
            evidence,
            "CTAS writer",
        ),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn execute_standard_publish(
    engine: &dyn CtasEngine,
    active: &mut ActiveDmlOperation,
    target: PreparedStandardCtasTarget,
    publication_id: novarocks_spi::connector::LakePublicationId,
    completion: ConnectorWriteOperationCompletion,
) -> Result<(), DmlError> {
    let action = match engine.prepare_standard_publish_ctas(
        target.handle.as_ref(),
        publication_id,
        completion,
    ) {
        Ok(action) => action,
        Err(failure) => {
            return finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Publish,
                failure,
                None,
            );
        }
    };
    active.check_before_dispatch()?;
    match engine.publish_standard_ctas(action.handle.as_ref()) {
        Ok(StandardCtasPublishOutcome::Applied { receipt }) => {
            finalize_standard_ctas_publication(active, CtasSagaPhase::Committed, receipt)
        }
        Ok(StandardCtasPublishOutcome::NoOp { receipt }) => {
            finalize_standard_ctas_publication(active, CtasSagaPhase::NoOp, receipt)
        }
        Ok(StandardCtasPublishOutcome::KnownUncommitted { failure }) => {
            finish_standard_known_uncommitted(
                active,
                CtasSagaPhase::Failed,
                FactSlot::Publish,
                failure,
                None,
            )
        }
        Ok(StandardCtasPublishOutcome::CommitUnknown { failure, evidence }) => {
            finish_standard_unknown(
                active,
                CtasSagaPhase::PublishUnknown,
                FactSlot::Publish,
                failure,
                evidence,
                "CTAS publication",
            )
        }
        Err(failure) => finish_standard_known_uncommitted(
            active,
            CtasSagaPhase::Failed,
            FactSlot::Publish,
            failure,
            None,
        ),
    }
}

#[derive(Clone, Copy)]
enum FactSlot {
    Prepare,
    Write,
    Publish,
    Abort,
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn validate_preflight_facts(
    stored: &StoredOperation,
    facts: &crate::query_execution::dml::ctas::CtasTargetPreflightFacts,
) -> Result<(), DmlError> {
    if facts.capability_version == 1
        && facts.instance_id == stored.target.catalog
        && facts.target_namespace == stored.target.namespace
        && facts.target_table == stored.target.table
        && !facts.provider_id.is_empty()
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS preflight facts conflict with the durable statement target",
        ))
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn finish_source_failure(
    active: &mut ActiveDmlOperation,
    stored: StoredOperation,
    source: &str,
    failure: CtasFailure,
) -> Result<(), DmlError> {
    let mut record = ctas_record(&stored)?;
    record.phase = if failure.kind == CtasFailureKind::Unsupported {
        CtasSagaPhase::Unsupported
    } else {
        CtasSagaPhase::Failed
    };
    record.prepare_fact = Some(failure_fact(&failure));
    record.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(record),
        None,
    )?;
    Err(source_failure_error(
        active.operation_id(),
        Some(source),
        failure,
    ))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn validate_source_facts(
    stored: &StoredOperation,
    source: &PreparedCtasSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), DmlError> {
    let matches = source.handle.execution_identity() == source.facts.execution_identity
        && source.facts.target_catalog == stored.target.catalog
        && source.facts.target_namespace == stored.target.namespace
        && source.facts.target_table == stored.target.table
        && source.facts.source_catalog.as_deref() == current_catalog
        && source.facts.source_database == current_database
        && !source.facts.output_columns.is_empty();
    if matches {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS prepared source facts conflict with the durable statement identity",
        ))
    }
}

fn source_failure_error(
    operation_id: DmlOperationId,
    source: Option<&str>,
    failure: CtasFailure,
) -> DmlError {
    if let Some(error) = failure.user_error(source) {
        return DmlError::admit(error);
    }
    operation_error(
        DmlErrorKind::Executor,
        operation_id,
        StatementNextAction::None,
        format_failure("CTAS request preparation failed", &failure),
    )
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
#[expect(
    clippy::too_many_arguments,
    reason = "The CTAS execution boundary keeps each fenced lifecycle dependency explicit."
)]
fn validate_standard_target_facts(
    stored: &StoredOperation,
    preflight: &crate::query_execution::dml::ctas::CtasTargetPreflightFacts,
    facts: &StandardCtasTargetFacts,
) -> Result<(), DmlError> {
    if facts.publication_id.to_bytes() == *stored.operation_id.as_uuid().as_bytes()
        && facts.provider_id == preflight.provider_id
        && facts.instance_id == preflight.instance_id
        && facts.incarnation == preflight.incarnation
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Commit,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "standard CTAS staged target facts conflict with its publication identity",
        ))
    }
}

fn validate_standard_prepared_write(
    stored: &StoredOperation,
    source: &PreparedCtasSource,
    target: &PreparedStandardCtasTarget,
    write: &PreparedStandardCtasWrite,
) -> Result<(), DmlError> {
    let expected = ctas_record(stored)?.write_operation_id;
    if write.write_operation_id.to_bytes() == *expected.as_bytes()
        && write.execution_identity == source.facts.execution_identity
        && write.handle.execution_identity() == source.facts.execution_identity
        && write.target_facts == target.facts
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "standard CTAS prepared write facts drifted from the source or staged target",
        ))
    }
}

fn validate_standard_completion(
    stored: &StoredOperation,
    source: &PreparedCtasSource,
    target: &PreparedStandardCtasTarget,
    prepared: &PreparedStandardCtasWrite,
    completion: &ConnectorWriteOperationCompletion,
    execution_identity: [u8; 32],
) -> Result<(), DmlError> {
    let expected_write = ctas_record(stored)?.write_operation_id;
    let matching = execution_identity == source.facts.execution_identity
        && execution_identity == prepared.execution_identity
        && completion.owner().instance_id.as_str() == target.facts.instance_id
        && completion.owner().incarnation.to_bytes() == target.facts.incarnation
        && completion.sealed().operation_id().to_bytes() == *expected_write.as_bytes()
        && completion.sealed().cohorts().len() == 1;
    if matching {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Commit,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "standard CTAS writer completion conflicts with durable source/target identity",
        ))
    }
}

fn standard_native_bundle(
    prepared: &PreparedStandardCtasWrite,
) -> Result<crate::query_execution::native_fragment::NativeFragmentAttachment, CtasFailure> {
    let encoding = prepared.handle.native_encoding()?;
    let input = encoding.input()?;
    crate::native::fragment_encoder::encode_native_fragment_bundle(input.encoding_view()).map_err(
        |message| CtasFailure {
            kind: CtasFailureKind::Internal,
            message,
            user_error: None,
        },
    )
}

fn finalize_standard_ctas_noop(active: &mut ActiveDmlOperation) -> Result<(), DmlError> {
    let mut record = ctas_record(&active.stored)?;
    record.phase = CtasSagaPhase::NoOp;
    record.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::Committing,
        OperationPayload::CtasSaga(record.clone()),
        None,
    )?;
    active.mutate_statement(
        OperationState::Committed,
        OperationPayload::CtasSaga(record.clone()),
        None,
    )?;
    active.mutate_statement(
        OperationState::Finalized,
        OperationPayload::CtasSaga(record),
        None,
    )
}

fn finalize_standard_ctas_publication(
    active: &mut ActiveDmlOperation,
    phase: CtasSagaPhase,
    receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
) -> Result<(), DmlError> {
    let outcome = if phase == CtasSagaPhase::NoOp {
        ExternalFactOutcome::NoOp
    } else {
        ExternalFactOutcome::KnownCommitted
    };
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = phase;
    saga.publish_fact = Some(staged_create_fact(outcome, &receipt));
    saga.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::Committed,
        OperationPayload::CtasSaga(saga.clone()),
        None,
    )?;
    active.mutate_statement(
        OperationState::Finalized,
        OperationPayload::CtasSaga(saga),
        None,
    )
}

fn staged_create_fact(
    outcome: ExternalFactOutcome,
    receipt: &novarocks_spi::connector::ConnectorStagedCreateReceipt,
) -> DurableExternalFact {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.staged-create-receipt-observation.v1");
    digest.update(receipt.owner().instance_id.as_str().as_bytes());
    digest.update(receipt.owner().incarnation.to_bytes());
    digest.update(receipt.operation_id().to_bytes());
    digest.update(format!("{:?}", receipt.phase()).as_bytes());
    digest.update(format!("{:?}", receipt.effect()).as_bytes());
    digest.update(receipt.provider_payload());
    DurableExternalFact {
        outcome,
        // This bounded observation digest is diagnostic only; publication
        // truth is the catalog's atomic staged-create frontier.
        receipt: Some(hex::encode(digest.finalize())),
        evidence: None,
        finalization_failure: None,
        failure: None,
    }
}

fn finish_standard_known_uncommitted(
    active: &mut ActiveDmlOperation,
    phase: CtasSagaPhase,
    slot: FactSlot,
    failure: CtasFailure,
    source: Option<&str>,
) -> Result<(), DmlError> {
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = if failure.kind == CtasFailureKind::Unsupported {
        CtasSagaPhase::Unsupported
    } else {
        phase
    };
    install_fact(&mut saga, slot, failure_fact(&failure));
    saga.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(saga),
        None,
    )?;
    Err(source_failure_error(active.operation_id(), source, failure))
}

fn finish_standard_unknown(
    active: &mut ActiveDmlOperation,
    phase: CtasSagaPhase,
    slot: FactSlot,
    failure: CtasFailure,
    evidence: ExternalMutationEvidence,
    label: &str,
) -> Result<(), DmlError> {
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = phase;
    install_fact(
        &mut saga,
        slot,
        DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence: encode_evidence(&evidence).ok(),
            finalization_failure: None,
            failure: Some(encode_failure(&failure)),
        },
    );
    saga.next_action = StatementNextAction::ManualInspect;
    active.mutate_statement(
        OperationState::CommitUnknown,
        OperationPayload::CtasSaga(saga),
        Some(crate::dml::now_unix_millis()),
    )?;
    Err(unknown_error(active.operation_id(), label, &failure))
}

fn failure_fact(failure: &CtasFailure) -> DurableExternalFact {
    DurableExternalFact {
        outcome: match failure.kind {
            CtasFailureKind::Unsupported => ExternalFactOutcome::Unsupported,
            CtasFailureKind::AlreadyExists | CtasFailureKind::Conflict => {
                ExternalFactOutcome::Conflict
            }
            _ => ExternalFactOutcome::KnownUncommitted,
        },
        receipt: None,
        evidence: None,
        finalization_failure: None,
        failure: Some(encode_failure(failure)),
    }
}

fn install_fact(record: &mut CtasSagaRecord, slot: FactSlot, fact: DurableExternalFact) {
    match slot {
        FactSlot::Prepare => record.prepare_fact = Some(fact),
        FactSlot::Write => record.write_fact = Some(fact),
        FactSlot::Publish => record.publish_fact = Some(fact),
        FactSlot::Abort => record.abort_staging_fact = Some(fact),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn encode_write_completion(
    completion: &ConnectorWriteOperationCompletion,
) -> Result<(String, String), DmlError> {
    let cohort = completion
        .sealed()
        .cohorts()
        .first()
        .ok_or_else(|| DmlError::commit("CTAS completion has no write cohort"))?;
    let cohort_id = hex::encode(cohort.cohort_id().to_bytes());
    let encoded = serde_json::to_string(&DurableCtasWriteCompletionV1 {
        version: DURABLE_CTAS_FACT_VERSION,
        instance_id: completion.owner().instance_id.as_str().to_string(),
        incarnation: hex::encode(completion.owner().incarnation.to_bytes()),
        operation_id: hex::encode(completion.sealed().operation_id().to_bytes()),
        cohort_id: cohort_id.clone(),
        cohort_set_digest: hex::encode(completion.sealed().digest()),
        aggregate_digest: hex::encode(completion.aggregate_digest()),
    })
    .map_err(DmlError::journal_corruption)?;
    ensure_fact_bound("CTAS writer completion", &encoded)?;
    Ok((encoded, cohort_id))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn encode_evidence(evidence: &ExternalMutationEvidence) -> Result<String, DmlError> {
    let wire = evidence.try_to_wire_v1().map_err(DmlError::commit)?;
    let encoded = hex::encode(wire);
    ensure_fact_bound("CTAS evidence", &encoded)?;
    Ok(encoded)
}

fn encode_failure(failure: &CtasFailure) -> String {
    let original_message_bytes = failure.message.len();
    let mut prefix_end = original_message_bytes.min(DURABLE_FAILURE_PREFIX_BYTES);
    while !failure.message.is_char_boundary(prefix_end) {
        prefix_end -= 1;
    }
    serde_json::to_string(&DurableCtasFailureV1 {
        version: DURABLE_CTAS_FACT_VERSION,
        kind: failure_kind(failure.kind),
        message_prefix: &failure.message[..prefix_end],
        message_truncated: prefix_end < original_message_bytes,
        original_message_bytes,
        original_message_sha256: hex::encode(Sha256::digest(failure.message.as_bytes())),
    })
    .unwrap_or_else(|_| {
        r#"{"version":1,"kind":"INTERNAL","message_prefix":"failure encoding failed","message_truncated":true,"original_message_bytes":0,"original_message_sha256":""}"#.to_string()
    })
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn ensure_fact_bound(label: &str, value: &str) -> Result<(), DmlError> {
    if value.len() <= DML_CTAS_FACT_ENCODED_LIMIT {
        Ok(())
    } else {
        Err(DmlError::journal_unavailable(format!(
            "{label} encoded size {} exceeds CTAS fact limit {DML_CTAS_FACT_ENCODED_LIMIT}",
            value.len()
        )))
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn ctas_record(stored: &StoredOperation) -> Result<CtasSagaRecord, DmlError> {
    match &stored.payload {
        OperationPayload::CtasSaga(record) => Ok(record.clone()),
        _ => Err(operation_error(
            DmlErrorKind::JournalCorruption,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "durable CTAS operation has the wrong payload kind",
        )),
    }
}

fn syntactic_target(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> OperationTarget {
    let (catalog, namespace, table) = match parts {
        [table] => (
            current_catalog.unwrap_or_default().to_string(),
            current_database.to_string(),
            table.clone(),
        ),
        [namespace, table] => (
            current_catalog.unwrap_or_default().to_string(),
            namespace.clone(),
            table.clone(),
        ),
        [catalog, namespace, table] => (catalog.clone(), namespace.clone(), table.clone()),
        _ => (
            current_catalog.unwrap_or_default().to_string(),
            current_database.to_string(),
            parts.join("."),
        ),
    };
    OperationTarget {
        catalog,
        namespace,
        table,
        ref_name: None,
    }
}

fn policy_name(policy: CreatePolicy) -> &'static str {
    match policy {
        CreatePolicy::FailIfExists => CTAS_CREATE_POLICY_FAIL_IF_EXISTS,
        CreatePolicy::NoOpIfExists => CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    }
}

fn journal_error(error: DmlError, operation_id: DmlOperationId) -> DmlError {
    operation_error(
        error.kind(),
        operation_id,
        StatementNextAction::ManualInspect,
        error,
    )
}

fn unknown_error(operation_id: DmlOperationId, phase: &str, failure: &CtasFailure) -> DmlError {
    operation_error(
        DmlErrorKind::Commit,
        operation_id,
        StatementNextAction::ManualInspect,
        format_failure(&format!("{phase} remains unresolved"), failure),
    )
}

fn operation_error(
    kind: DmlErrorKind,
    operation_id: DmlOperationId,
    next_action: StatementNextAction,
    message: impl std::fmt::Display,
) -> DmlError {
    DmlError::new(kind, message)
        .with_operation_id(operation_id)
        .with_next_action(next_action)
}

fn format_failure(prefix: &str, failure: &CtasFailure) -> String {
    format!(
        "{prefix}: {}: {}",
        failure_kind(failure.kind),
        failure.message
    )
}

fn failure_kind(kind: CtasFailureKind) -> &'static str {
    match kind {
        CtasFailureKind::InvalidRequest => "INVALID_REQUEST",
        CtasFailureKind::NotFound => "NOT_FOUND",
        CtasFailureKind::AlreadyExists => "ALREADY_EXISTS",
        CtasFailureKind::Conflict => "CONFLICT",
        CtasFailureKind::Unsupported => "UNSUPPORTED",
        CtasFailureKind::Cancelled => "CANCELLED",
        CtasFailureKind::DeadlineExceeded => "DEADLINE_EXCEEDED",
        CtasFailureKind::Unavailable => "UNAVAILABLE",
        CtasFailureKind::Internal => "INTERNAL",
    }
}
