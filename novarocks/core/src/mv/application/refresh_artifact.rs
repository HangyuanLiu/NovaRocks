// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Application-owned MV refresh activation artifacts.
//!
//! SQL produces an immutable first-refresh plan. This module adds the
//! operation, cohort, connector binding and persisted MV facts needed by the
//! frontend staging lifecycle; none of those authorities can cross back into
//! `sql/**`.

use std::collections::BTreeMap;

use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorTableHandle, ConnectorWriteCohortId,
    ConnectorWriteOperationId,
};

use crate::sql::mv_refresh::first_refresh::{
    MvFirstRefreshPhysicalSql, MvFirstRefreshShape, MvFirstRefreshTargetContract, SqlMvSnapshotPin,
};
use crate::sql::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME;

/// The application commit semantics selected after first-refresh SQL planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvStagedRefreshWriteMode {
    Append,
    FullOverwrite,
}

/// Application facts retained for the typed join activation path. The SQL
/// artifact contains its logical plan only; persistence and refresh-context
/// reconstruction stay at this application boundary.
pub(crate) struct MvFirstRefreshLogicalContext {
    pub(crate) mv_definition: crate::mv::persistence::definition::StoredMvDefinition,
    pub(crate) canonical_select_query: sqlparser::ast::Query,
    pub(crate) base_refs: Vec<novarocks_catalog::identifier::TableIdentity>,
    pub(crate) pin: SqlMvSnapshotPin,
    pub(crate) previous_snapshot_ids: BTreeMap<String, i64>,
    pub(crate) previous_table_uuids: BTreeMap<String, String>,
    pub(crate) target_table_uuid: String,
    pub(crate) affected_partitions: crate::mv::model::AffectedTargetPartitions,
    /// Base-table materializations admitted while the first-refresh artifact
    /// was prepared.  The overlays retain their exact control leases, files,
    /// and snapshot facts until activation creates the request-local binding
    /// store. `None` identifies artifact modes that have not admitted a
    /// logical join input and therefore cannot use this handoff.
    pub(crate) frozen_base_overlays:
        Option<Vec<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay>>,
}

/// Application envelope for a join first-refresh artifact.
///
/// The canonical SELECT is deliberately not compiled until activation, after
/// the frontend has retained the exact planning lease and admitted the query
/// execution.  Carrying only immutable refresh facts here prevents an
/// unscoped logical plan from outliving the request-local table bindings that
/// must prepare its scans.
pub(crate) struct MvFirstRefreshLogicalArtifact {
    context: MvFirstRefreshLogicalContext,
}

impl MvFirstRefreshLogicalArtifact {
    pub(crate) fn from_join_context(context: MvFirstRefreshLogicalContext) -> Self {
        Self { context }
    }

    pub(crate) fn into_context(self) -> MvFirstRefreshLogicalContext {
        self.context
    }

    pub(crate) const fn root_hash_column(&self) -> &str {
        JOIN_APPLY_KEY_COLUMN_NAME
    }
}

pub(crate) enum MvFirstRefreshExecutionArtifact {
    Sql(MvFirstRefreshPhysicalSql),
    Logical(MvFirstRefreshLogicalArtifact),
}

impl MvFirstRefreshExecutionArtifact {
    pub(crate) fn root_hash_column(&self) -> &str {
        match self {
            Self::Sql(sql) => sql.root_hash_column(),
            Self::Logical(logical) => logical.root_hash_column(),
        }
    }
}

/// Application handoff before a first-refresh writer is admitted.
#[derive(Clone)]
pub(crate) struct MvFirstRefreshWriteRequest {
    canonical_select_sql: String,
    shape: MvFirstRefreshShape,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
    staging_branch: String,
    current_catalog: Option<String>,
    current_database: String,
    expected_target_snapshot_id: Option<i64>,
    target_table: ConnectorTableHandle,
    target_contract: MvFirstRefreshTargetContract,
    observed_binding: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
}

impl MvFirstRefreshWriteRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        canonical_select_sql: String,
        shape: MvFirstRefreshShape,
        target_catalog: String,
        target_namespace: String,
        target_name: String,
        staging_branch: String,
        current_catalog: Option<String>,
        current_database: String,
        expected_target_snapshot_id: Option<i64>,
        target_table: ConnectorTableHandle,
        target_contract: MvFirstRefreshTargetContract,
        observed_binding: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<Self, String> {
        if canonical_select_sql.trim().is_empty()
            || target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
            || staging_branch.is_empty()
            || current_database.is_empty()
            || target_table.owner() != &observed_binding.instance_id
        {
            return Err("invalid MV first-refresh write request identity".to_string());
        }
        Ok(Self {
            canonical_select_sql,
            shape,
            target_catalog,
            target_namespace,
            target_name,
            staging_branch,
            current_catalog,
            current_database,
            expected_target_snapshot_id,
            target_table,
            target_contract,
            observed_binding,
            operation_id,
        })
    }

    pub(crate) fn canonical_select_sql(&self) -> &str {
        &self.canonical_select_sql
    }

    pub(crate) const fn shape(&self) -> MvFirstRefreshShape {
        self.shape
    }

    pub(crate) fn target_catalog(&self) -> &str {
        &self.target_catalog
    }

    pub(crate) fn target_namespace(&self) -> &str {
        &self.target_namespace
    }

    pub(crate) fn target_name(&self) -> &str {
        &self.target_name
    }

    pub(crate) fn staging_branch(&self) -> &str {
        &self.staging_branch
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub(crate) fn current_database(&self) -> &str {
        &self.current_database
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.expected_target_snapshot_id
    }

    pub(crate) fn target_table(&self) -> &ConnectorTableHandle {
        &self.target_table
    }

    pub(crate) fn target_contract(&self) -> &MvFirstRefreshTargetContract {
        &self.target_contract
    }

    pub(crate) fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        &self.observed_binding
    }

    pub(crate) const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
}

/// Opaque application artifact consumed by the staging lifecycle.
pub struct PreparedMvFirstRefreshWrite {
    request: MvFirstRefreshWriteRequest,
    artifact: MvFirstRefreshExecutionArtifact,
    primary_cohort: ConnectorWriteCohortId,
    write_mode: MvStagedRefreshWriteMode,
    provenance_properties: BTreeMap<String, String>,
}

impl PreparedMvFirstRefreshWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.request.operation_id()
    }

    pub fn primary_cohort(&self) -> ConnectorWriteCohortId {
        self.primary_cohort
    }

    pub(crate) fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        self.request.observed_binding()
    }

    pub(crate) fn target_contract(&self) -> &MvFirstRefreshTargetContract {
        self.request.target_contract()
    }

    pub(crate) const fn shape(&self) -> MvFirstRefreshShape {
        self.request.shape()
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        self.artifact.root_hash_column()
    }

    pub(crate) fn target_catalog(&self) -> &str {
        self.request.target_catalog()
    }

    pub(crate) fn target_namespace(&self) -> &str {
        self.request.target_namespace()
    }

    pub(crate) fn target_name(&self) -> &str {
        self.request.target_name()
    }

    pub(crate) fn staging_branch(&self) -> &str {
        self.request.staging_branch()
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.request.current_catalog()
    }

    pub(crate) fn current_database(&self) -> &str {
        self.request.current_database()
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.request.expected_target_snapshot_id()
    }

    pub(crate) const fn write_mode(&self) -> MvStagedRefreshWriteMode {
        self.write_mode
    }

    pub(crate) fn into_full_overwrite(mut self) -> Self {
        self.write_mode = MvStagedRefreshWriteMode::FullOverwrite;
        self
    }

    pub(crate) fn with_provenance_properties(
        mut self,
        provenance_properties: BTreeMap<String, String>,
    ) -> Self {
        self.provenance_properties = provenance_properties;
        self
    }

    pub(crate) fn provenance_properties(&self) -> &BTreeMap<String, String> {
        &self.provenance_properties
    }

    pub(crate) fn into_execution_artifact(self) -> MvFirstRefreshExecutionArtifact {
        self.artifact
    }

    pub(crate) fn physical_sql(&self) -> Option<&str> {
        match &self.artifact {
            MvFirstRefreshExecutionArtifact::Sql(sql) => Some(sql.sql()),
            MvFirstRefreshExecutionArtifact::Logical(_) => None,
        }
    }
}

pub(crate) struct MvFirstRefreshWritePreparer;

impl MvFirstRefreshWritePreparer {
    pub(crate) fn prepare(
        request: MvFirstRefreshWriteRequest,
        physical_sql: MvFirstRefreshPhysicalSql,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Sql(physical_sql),
            MvStagedRefreshWriteMode::Append,
        )
    }

    pub(crate) fn prepare_full_overwrite(
        request: MvFirstRefreshWriteRequest,
        physical_sql: MvFirstRefreshPhysicalSql,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Sql(physical_sql),
            MvStagedRefreshWriteMode::FullOverwrite,
        )
    }

    pub(crate) fn prepare_join_logical(
        request: MvFirstRefreshWriteRequest,
        context: MvFirstRefreshLogicalContext,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Logical(
                MvFirstRefreshLogicalArtifact::from_join_context(context),
            ),
            MvStagedRefreshWriteMode::Append,
        )
    }

    fn prepare_artifact(
        request: MvFirstRefreshWriteRequest,
        artifact: MvFirstRefreshExecutionArtifact,
        write_mode: MvStagedRefreshWriteMode,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        if artifact.root_hash_column() != request.target_contract().hidden_hash_key() {
            return Err(
                "MV first-refresh root distribution does not match the target hidden hash key"
                    .to_string(),
            );
        }
        if matches!(&artifact, MvFirstRefreshExecutionArtifact::Sql(physical_sql)
            if physical_sql.sql().contains("QueryResult")
                || physical_sql.sql().contains("RecordBatch")
                || physical_sql.sql().contains("Chunk"))
        {
            return Err(
                "MV first-refresh SQL artifact contains a frontend row carrier".to_string(),
            );
        }
        let operation_id = request.operation_id();
        Ok(PreparedMvFirstRefreshWrite {
            request,
            artifact,
            primary_cohort: ConnectorWriteCohortId::primary(operation_id),
            write_mode,
            provenance_properties: BTreeMap::new(),
        })
    }
}

/// Application activation shape for an incremental IMV change-stream write.
pub(crate) enum MvIncrementalExecutionArtifact {
    CanonicalQuery,
    /// The join shape is frozen before connector admission, but construction
    /// of its SQL logical plan waits for the exact query-local target binding.
    /// A pre-admission artifact must never fabricate an unbound SQL token.
    JoinLogical {
        mode: MvIncrementalJoinMode,
    },
}

/// Join refresh shape retained by the application artifact until exact
/// connector admission makes target-token-dependent logical planning valid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvIncrementalJoinMode {
    AppendOnly,
    Coalesce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvIncrementalWriteMode {
    FastAppend,
    RowDelta,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvIncrementalRewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

/// Application handoff before an incremental write is admitted. It carries
/// request lifecycle identity but no provider table or prepared writer.
pub(crate) struct MvIncrementalWriteRequest {
    pub(crate) target_catalog: String,
    pub(crate) target_namespace: String,
    pub(crate) target_name: String,
    pub(crate) staging_branch: String,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) expected_target_snapshot_id: Option<i64>,
    pub(crate) observed_binding: ConnectorExecutionBindingKey,
    pub(crate) operation_id: ConnectorWriteOperationId,
}

impl MvIncrementalWriteRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        target_catalog: String,
        target_namespace: String,
        target_name: String,
        staging_branch: String,
        current_catalog: Option<String>,
        current_database: String,
        expected_target_snapshot_id: Option<i64>,
        observed_binding: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<Self, String> {
        if target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
            || staging_branch.is_empty()
            || current_database.is_empty()
        {
            return Err("invalid MV incremental write request identity".to_string());
        }
        Ok(Self {
            target_catalog,
            target_namespace,
            target_name,
            staging_branch,
            current_catalog,
            current_database,
            expected_target_snapshot_id,
            observed_binding,
            operation_id,
        })
    }
}

#[cfg(test)]
mod incremental_tests {
    use super::*;

    #[test]
    fn sqlx2_mv_incremental_request_rejects_missing_staging_identity() {
        let result = MvIncrementalWriteRequest::try_new(
            "ice".to_string(),
            "db".to_string(),
            "mv".to_string(),
            String::new(),
            Some("ice".to_string()),
            "db".to_string(),
            Some(7),
            ConnectorExecutionBindingKey {
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                    .expect("instance"),
                incarnation: novarocks_spi::connector::ConnectorInstanceIncarnation::from_bytes(
                    [1; 16],
                ),
            },
            ConnectorWriteOperationId::from_bytes([2; 16]),
        );
        match result {
            Err(error) => assert_eq!(error, "invalid MV incremental write request identity"),
            Ok(_) => panic!("missing staging branch must fail"),
        }
    }
}

/// Opaque incremental artifact owned by the application staging lifecycle.
pub struct PreparedMvIncrementalWrite {
    request: MvIncrementalWriteRequest,
    logical_context: MvFirstRefreshLogicalContext,
    mode: MvIncrementalWriteMode,
    evidence: MvIncrementalRewriteEvidence,
    execution_artifact: MvIncrementalExecutionArtifact,
    provenance_properties: BTreeMap<String, String>,
}

impl PreparedMvIncrementalWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.request.operation_id
    }

    pub fn primary_cohort(&self) -> ConnectorWriteCohortId {
        ConnectorWriteCohortId::primary(self.request.operation_id)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        MvIncrementalWriteRequest,
        MvFirstRefreshLogicalContext,
        MvIncrementalWriteMode,
        MvIncrementalRewriteEvidence,
        MvIncrementalExecutionArtifact,
        BTreeMap<String, String>,
    ) {
        (
            self.request,
            self.logical_context,
            self.mode,
            self.evidence,
            self.execution_artifact,
            self.provenance_properties,
        )
    }
}

pub(crate) struct MvIncrementalWritePreparer;

impl MvIncrementalWritePreparer {
    pub(crate) fn prepare(
        request: MvIncrementalWriteRequest,
        logical_context: MvFirstRefreshLogicalContext,
        mode: MvIncrementalWriteMode,
        evidence: MvIncrementalRewriteEvidence,
        execution_artifact: MvIncrementalExecutionArtifact,
        provenance_properties: BTreeMap<String, String>,
    ) -> Result<PreparedMvIncrementalWrite, String> {
        if logical_context.base_refs.is_empty() || logical_context.pin.is_empty() {
            return Err("MV incremental write requires pinned base facts".to_string());
        }
        if logical_context.pin.len() != logical_context.base_refs.len() {
            return Err("MV incremental write has incomplete base snapshot pins".to_string());
        }
        Ok(PreparedMvIncrementalWrite {
            request,
            logical_context,
            mode,
            evidence,
            execution_artifact,
            provenance_properties,
        })
    }
}
