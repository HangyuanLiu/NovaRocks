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
use std::convert::Infallible;
use std::sync::Arc;

use crate::query_execution::dml::insert::{
    IcebergInsertCommit, IcebergWriteReport, InsertEngine, PreparedIcebergInsert,
};

use crate::dml::model::{OperationKind, OperationTarget, WriteTransactionSpec};
use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};

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
        let encoding = self
            .engine
            .iceberg_write_native_encoding(self.prepared.handle.as_ref())
            .map_err(|error| error.into_dml_error(Some(&self.prepared.sql_source)))?;
        let input = encoding
            .input()
            .map_err(crate::dml::error::DmlError::executor)?;
        let native_bundle =
            crate::native::fragment_encoder::encode_native_fragment_bundle(input.encoding_view())
                .map_err(crate::dml::error::DmlError::executor)?;
        drop(encoding);
        Ok(
            match self
                .engine
                .run_iceberg_write_with_native_bundle(self.prepared.handle.as_ref(), native_bundle)
                .map_err(crate::dml::error::DmlError::executor)?
            {
                IcebergWriteReport::Aborted {
                    reason,
                    has_staged_files: _,
                } => CoordinatedWriteReport::Aborted { reason },
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

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine
            .finalize_iceberg_write(self.prepared.handle.as_ref())
    }
}

pub(super) fn write_transaction_spec(prepared: &PreparedIcebergInsert) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        publication_id: operation.publication_id,
        target: OperationTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            ref_name: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
        operation_kind: if operation.is_overwrite {
            OperationKind::InsertOverwrite
        } else {
            OperationKind::InsertAppend
        },
        operation_subkind: None,
        attempt_id: operation.attempt_id.clone(),
        base_snapshot_id: operation.base_snapshot_id,
        base_snapshot_map: BTreeMap::new(),
    }
}
