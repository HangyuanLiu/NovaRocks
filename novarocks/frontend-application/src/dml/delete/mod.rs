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

//! Frontend-owned DELETE statement recognition and application routing.

use std::convert::Infallible;
use std::sync::Arc;

use crate::query_execution::dml::delete::{
    DeleteCommit, DeleteEngine, DeleteStatement, DeleteWriteReport, PrepareDeleteRequest,
    PreparedDelete,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_spi::connector::LakePublicationId;

use crate::dml::error::DmlError;
use crate::dml::runner::{
    CoordinatedWriteReport, StatementWriteTransactionRunner, WriteExecutor, WriteTarget,
    WriteTransactionSpec,
};
use crate::dml::service::DmlService;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;
use novarocks_spi::connector::LakePublicationFamily;

/// A prepared DELETE whose provider write has not begun.
///
/// The retained handle is exact to one statement and may be dropped before
/// dispatch. No publication attempt exists until the write edge is admitted.
pub(crate) struct PreparedDeleteAttempt {
    prepared: PreparedDelete,
}

struct DeleteWriteExecutor<'a> {
    engine: &'a dyn DeleteEngine,
    prepared: &'a PreparedDelete,
}

impl WriteExecutor for DeleteWriteExecutor<'_> {
    type CommitHandle = Arc<dyn DeleteCommit>;
    type AbortHandle = Infallible;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, DmlError> {
        Ok(
            match self
                .engine
                .run_delete(self.prepared.handle.as_ref())
                .map_err(|error| error.into_dml_error(Some(&self.prepared.sql_source)))?
            {
                DeleteWriteReport::NoOp => CoordinatedWriteReport::NoOp,
                DeleteWriteReport::CommitRequired(handle) => {
                    CoordinatedWriteReport::CommitRequired(handle)
                }
            },
        )
    }

    fn abort(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        match *handle {}
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        self.engine
            .commit_delete_terminal(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn adjudicate_publication(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        self.engine.adjudicate_delete_publication(
            self.prepared.handle.as_ref(),
            handle.as_ref(),
            evidence,
        )
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine.finalize_delete(self.prepared.handle.as_ref())
    }
}

fn write_transaction_spec(prepared: &PreparedDelete) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        publication_id: operation.publication_id,
        target: WriteTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            reference: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
    }
}

impl DmlService {
    /// Prepares a parser-owned DELETE variant without beginning its write.
    /// `source` is carried only for AST-span slices during preparation; it is
    /// never reparsed here.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn prepare_delete(
        &self,
        engine: &dyn DeleteEngine,
        statement: DeleteStatement<'_>,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<PreparedDeleteAttempt, DmlError> {
        if let DeleteStatement::Predicate(delete) = statement
            && delete.selection.is_none()
        {
            return Err(DmlError::admit(DmlAdmissionError::DeleteRequiresWhere.to_user_error(
                source,
                delete.span,
                "DELETE requires a WHERE clause; for full table replacement use INSERT OVERWRITE t SELECT * FROM t WHERE FALSE",
            )));
        }

        let publication_id = LakePublicationId::new_v7();
        let session = context.session();
        let prepared = engine
            .prepare_delete(PrepareDeleteRequest {
                publication_id,
                statement,
                source,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(DmlError::executor)?;
        Ok(PreparedDeleteAttempt { prepared })
    }

    /// Crosses the statement's one-shot DELETE write boundary.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn execute_prepared_delete(
        &self,
        engine: &dyn DeleteEngine,
        prepared: PreparedDeleteAttempt,
    ) -> Result<(), DmlError> {
        let _ = self;
        let executor = DeleteWriteExecutor {
            engine,
            prepared: &prepared.prepared,
        };
        let spec = write_transaction_spec(&prepared.prepared);
        StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::DataMutation)
            .run(spec)?;
        Ok(())
    }
}

#[cfg(test)]
mod execution_error_tests {
    use super::*;
    use crate::dml::error::DmlExecutionError;
    use crate::query_execution::contract::DistributedQueryError;
    use crate::query_execution::dml::delete::{DeleteOperation, DeletePrepared};
    use novarocks_failure_contract::FailureProjection;
    use novarocks_plan_codec::PhysicalEncodeError;

    struct Prepared;
    impl DeletePrepared for Prepared {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    struct RefusingEngine {
        source: PhysicalEncodeError,
    }
    impl DeleteEngine for RefusingEngine {
        fn prepare_delete(&self, _: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String> {
            panic!("already prepared");
        }

        fn run_delete(
            &self,
            _: &dyn DeletePrepared,
        ) -> Result<DeleteWriteReport, DmlExecutionError> {
            Err(DmlExecutionError::from_distributed(
                DistributedQueryError::from_encode(self.source.clone()),
            ))
        }

        fn finalize_delete(&self, _: &dyn DeletePrepared) -> Result<(), String> {
            panic!("refused write cannot finalize");
        }
    }

    #[test]
    fn delete_execution_port_preserves_typed_runtime_identity() {
        for source in [
            PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
            PhysicalEncodeError::UnsupportedCapability("unsupported diagnostic".into()),
        ] {
            let expected = source.failure_fact();
            let engine = RefusingEngine { source };
            let prepared = PreparedDelete {
                operation: DeleteOperation {
                    publication_id: LakePublicationId::new_v7(),
                    catalog: "ice".into(),
                    namespace: "db".into(),
                    table: "orders".into(),
                    target_ref: "main".into(),
                    attempt_id: "fixture".into(),
                    base_snapshot_id: None,
                },
                handle: Arc::new(Prepared),
                sql_source: "DELETE FROM orders WHERE id = 1".into(),
            };
            let executor = DeleteWriteExecutor {
                engine: &engine,
                prepared: &prepared,
            };
            let error = executor
                .run_coordinated_write(&write_transaction_spec(&prepared))
                .err()
                .expect("write must fail");
            assert_eq!(error.execution_failure().unwrap().fact(), expected);
            assert_eq!(error.engine_error_code(), None);
            assert_eq!(
                error.to_string(),
                format!(
                    "Executor: native attempt initialization failed in its owning domain (reason {})",
                    expected.reason(),
                ),
            );
        }
    }
}
