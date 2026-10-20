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

use std::convert::Infallible;
use std::sync::Arc;

use crate::query_execution::dml::insert::{
    IcebergInsertCommit, IcebergWriteReport, InsertEngine, PreparedIcebergInsert,
};

use crate::dml::runner::{
    CoordinatedWriteReport, WriteExecutor, WriteTarget, WriteTransactionSpec,
};

pub(super) struct IcebergInsertWriteExecutor<'a> {
    engine: &'a dyn InsertEngine,
    prepared: &'a PreparedIcebergInsert,
}

impl<'a> IcebergInsertWriteExecutor<'a> {
    pub(super) fn new(engine: &'a dyn InsertEngine, prepared: &'a PreparedIcebergInsert) -> Self {
        Self { engine, prepared }
    }
}

impl WriteExecutor for IcebergInsertWriteExecutor<'_> {
    type CommitHandle = Arc<dyn IcebergInsertCommit>;
    type AbortHandle = Infallible;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, crate::dml::error::DmlError> {
        Ok(
            match self
                .engine
                .run_iceberg_write(self.prepared.handle.as_ref())
                .map_err(|error| error.into_dml_error(Some(&self.prepared.sql_source)))?
            {
                IcebergWriteReport::NoOp => CoordinatedWriteReport::NoOp,
                IcebergWriteReport::CommitRequired(handle) => {
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
            .commit_iceberg_write_terminal(self.prepared.handle.as_ref(), handle.as_ref())
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
        self.engine.adjudicate_iceberg_write_publication(
            self.prepared.handle.as_ref(),
            handle.as_ref(),
            evidence,
        )
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine
            .finalize_iceberg_write(self.prepared.handle.as_ref())
    }
}

pub(super) fn write_transaction_spec(prepared: &PreparedIcebergInsert) -> WriteTransactionSpec {
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

#[cfg(test)]
mod runtime_identity_tests {
    use super::*;
    use crate::dml::error::DmlExecutionError;
    use crate::query_execution::contract::DistributedQueryError;
    use crate::query_execution::dml::insert::{
        IcebergInsertOperation, IcebergPreparedInsert, PrepareIcebergInsert, ResolveInsertTarget,
        ResolvedInsertTarget,
    };
    use novarocks_failure_contract::FailureProjection;
    use novarocks_plan_codec::PhysicalEncodeError;

    struct Prepared;
    impl IcebergPreparedInsert for Prepared {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
    struct RefusingEngine {
        source: PhysicalEncodeError,
    }
    impl InsertEngine for RefusingEngine {
        fn resolve_target(&self, _: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String> {
            panic!("already prepared");
        }
        fn prepare_iceberg_write(
            &self,
            _: PrepareIcebergInsert,
        ) -> Result<PreparedIcebergInsert, String> {
            panic!("already prepared");
        }
        fn run_iceberg_write(
            &self,
            _: &dyn IcebergPreparedInsert,
        ) -> Result<IcebergWriteReport, DmlExecutionError> {
            Err(DmlExecutionError::from_distributed(
                DistributedQueryError::from_encode(self.source.clone()),
            ))
        }
        fn finalize_iceberg_write(&self, _: &dyn IcebergPreparedInsert) -> Result<(), String> {
            panic!("refused write cannot finalize");
        }
    }

    #[test]
    fn insert_execution_port_keeps_typed_runtime_identity_through_executor_projection() {
        for source in [
            PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
            PhysicalEncodeError::UnsupportedCapability("unsupported diagnostic".into()),
        ] {
            let expected = source.failure_fact();
            let engine = RefusingEngine { source };
            let prepared = PreparedIcebergInsert {
                operation: IcebergInsertOperation {
                    publication_id: novarocks_spi::connector::LakePublicationId::new_v7(),
                    catalog: "ice".into(),
                    namespace: "db".into(),
                    table: "orders".into(),
                    target_ref: "main".into(),
                    attempt_id: "fixture".into(),
                    is_overwrite: false,
                    base_snapshot_id: None,
                },
                handle: Arc::new(Prepared),
                sql_source: "INSERT INTO orders SELECT ...".into(),
            };
            let error = IcebergInsertWriteExecutor::new(&engine, &prepared)
                .run_coordinated_write(&write_transaction_spec(&prepared))
                .err()
                .unwrap();
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
