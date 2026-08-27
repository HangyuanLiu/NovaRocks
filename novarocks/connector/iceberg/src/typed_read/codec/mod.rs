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

//! Iceberg's concrete implementation of the closed connector-read codec.
//!
//! This is the only Iceberg module that turns central-IDL read carriers into
//! the Connector-owned runtime enums.  Ordinary metadata, split enumeration,
//! and reader calls use those enums directly.

use std::sync::Arc;

use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    CatalogTableHandle, ConnectorReadCodec, ConnectorReadCodecError, ConnectorRelation,
    ValidatedColumnHandle, ValidatedConnectorSplit, ValidatedTransactionHandle,
};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime;
use novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter;
use novarocks_spi::connector::read_stack::{
    ConnectorReadColumnHandle, ConnectorReadRelation, ConnectorReadSplit,
    ConnectorReadTransactionHandle,
};

use super::{
    FilesTableSplit, HiveTransactionHandle, IcebergChangeSplit, IcebergChangeWindowHandle,
    IcebergColumnHandle, IcebergMergeTableHandle, IcebergReadSplit,
    IcebergRewritePositionDeleteFilesSplit, IcebergRuntimeRelation, IcebergSystemTableReference,
    IcebergTableExecuteHandle, IcebergTableHandle, TableChangesFunctionHandle, TableChangesSplit,
};

#[derive(Clone)]
pub struct IcebergConnectorReadCodec<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    adapter: ReadRuntimeAdapter<P>,
    owner: Arc<str>,
}

impl<P> IcebergConnectorReadCodec<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    pub fn new(adapter: ReadRuntimeAdapter<P>) -> Self {
        Self {
            owner: Arc::from(adapter.binding().descriptor().instance_id.as_str()),
            adapter,
        }
    }

    fn invalid(&self, path: FieldPath, error: impl std::fmt::Display) -> ConnectorReadCodecError {
        ConnectorReadCodecError::invalid(self.owner(), path, error.to_string())
    }

    fn ensure_outer_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<(), ConnectorReadCodecError> {
        let binding = self.adapter.binding();
        if relation.catalog_name() != binding.descriptor().instance_id.as_str() {
            return Err(self.invalid(
                FieldPath::root("catalog_table_handle").field("catalog_name"),
                "iceberg catalog table handle names another catalog",
            ));
        }
        if relation.instance_incarnation() != binding.incarnation().to_bytes() {
            return Err(self.invalid(
                FieldPath::root("catalog_table_handle").field("instance_incarnation"),
                "iceberg catalog table handle names another instance incarnation",
            ));
        }
        Ok(())
    }

    fn decode_transaction_value(
        &self,
        transaction: &dto::ConnectorTransactionHandle,
    ) -> Result<HiveTransactionHandle, ConnectorReadCodecError> {
        HiveTransactionHandle::from_transaction_handle_proto(transaction)
            .map_err(|error| self.invalid(FieldPath::root("transaction"), error))
    }

    fn encode_outer_relation(
        &self,
        transaction: &HiveTransactionHandle,
        relation: dto::catalog_table_handle::Relation,
    ) -> dto::CatalogTableHandle {
        dto::CatalogTableHandle {
            catalog_name: self
                .adapter
                .binding()
                .descriptor()
                .instance_id
                .as_str()
                .to_string(),
            instance_incarnation: self.adapter.binding().incarnation().to_bytes().to_vec(),
            transaction: Some(transaction.to_transaction_handle_proto()),
            relation: Some(relation),
        }
    }
}

impl<P> ConnectorReadCodec for IcebergConnectorReadCodec<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn owner(&self) -> &str {
        &self.owner
    }

    fn decode_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError> {
        self.ensure_outer_relation(relation)?;
        let transaction = relation.transaction();
        let _transaction = self.decode_transaction_value(transaction)?;
        let table = match relation.relation() {
            ConnectorRelation::Table(value) => IcebergRuntimeRelation::Table(
                IcebergTableHandle::from_table_handle_proto(value).map_err(|error| {
                    self.invalid(FieldPath::root("catalog_table_handle"), error)
                })?,
            ),
            ConnectorRelation::TableFunction(value) => IcebergRuntimeRelation::TableFunction(
                TableChangesFunctionHandle::from_table_function_handle_proto(value).map_err(
                    |error| self.invalid(FieldPath::root("catalog_table_handle"), error),
                )?,
            ),
            ConnectorRelation::ChangeWindow(value) => IcebergRuntimeRelation::ChangeWindow(
                IcebergChangeWindowHandle::from_change_window_handle_proto(value).map_err(
                    |error| self.invalid(FieldPath::root("catalog_table_handle"), error),
                )?,
            ),
            ConnectorRelation::SystemTable(value) => IcebergRuntimeRelation::SystemTable(
                IcebergSystemTableReference::from_system_table_reference_proto(value).map_err(
                    |error| self.invalid(FieldPath::root("catalog_table_handle"), error),
                )?,
            ),
            ConnectorRelation::TableExecute(value) => IcebergRuntimeRelation::TableExecute(
                IcebergTableExecuteHandle::from_table_execute_handle_proto(value).map_err(
                    |error| self.invalid(FieldPath::root("catalog_table_handle"), error),
                )?,
            ),
            ConnectorRelation::MergeTable(value) => IcebergRuntimeRelation::MergeTable(
                IcebergMergeTableHandle::from_merge_table_handle_proto(value).map_err(|error| {
                    self.invalid(FieldPath::root("catalog_table_handle"), error)
                })?,
            ),
        };
        let kind = table.kind();
        let wrapped = self.adapter.wrap_table(table);
        self.adapter
            .relation(kind, wrapped)
            .map_err(|error| self.invalid(FieldPath::root("catalog_table_handle"), error))
    }

    fn encode_relation(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError> {
        let table = self
            .adapter
            .table(relation.table())
            .map_err(|error| self.invalid(FieldPath::root("relation").field("table"), error))?;
        let transaction = self
            .adapter
            .transaction(relation.transaction())
            .map_err(|error| {
                self.invalid(FieldPath::root("relation").field("transaction"), error)
            })?;
        let raw = match table {
            IcebergRuntimeRelation::Table(value) => {
                dto::catalog_table_handle::Relation::Table(value.to_table_handle_proto())
            }
            IcebergRuntimeRelation::TableFunction(value) => {
                dto::catalog_table_handle::Relation::TableFunction(
                    value.to_table_function_handle_proto(),
                )
            }
            IcebergRuntimeRelation::ChangeWindow(value) => {
                dto::catalog_table_handle::Relation::ChangeWindow(
                    value.to_change_window_handle_proto(),
                )
            }
            IcebergRuntimeRelation::SystemTable(value) => {
                dto::catalog_table_handle::Relation::SystemTable(
                    value.to_system_table_reference_proto(),
                )
            }
            IcebergRuntimeRelation::TableExecute(value) => {
                dto::catalog_table_handle::Relation::TableExecute(
                    value.to_table_execute_handle_proto(),
                )
            }
            IcebergRuntimeRelation::MergeTable(value) => {
                dto::catalog_table_handle::Relation::MergeTable(value.to_merge_table_handle_proto())
            }
        };
        Ok(self.encode_outer_relation(transaction, raw))
    }

    fn decode_column(
        &self,
        column: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError> {
        let column = IcebergColumnHandle::from_column_handle_proto(column.as_proto())
            .map_err(|error| self.invalid(FieldPath::root("column_handle"), error))?;
        Ok(self.adapter.wrap_column(column))
    }

    fn encode_column(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError> {
        let column = self
            .adapter
            .column(column)
            .map_err(|error| self.invalid(FieldPath::root("column_handle"), error))?;
        Ok(column.to_column_handle_proto())
    }

    fn decode_transaction(
        &self,
        transaction: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError> {
        Ok(self
            .adapter
            .wrap_transaction(self.decode_transaction_value(transaction.as_proto())?))
    }

    fn encode_transaction(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError> {
        let transaction = self
            .adapter
            .transaction(transaction)
            .map_err(|error| self.invalid(FieldPath::root("transaction"), error))?;
        Ok(transaction.to_transaction_handle_proto())
    }

    fn decode_split(
        &self,
        split: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError> {
        let split = match split.category() {
            novarocks_proto_codec::connector_read::SplitCategory::Data => IcebergReadSplit::Data(
                IcebergTableHandleSplit::from_connector_split_proto(split.as_proto())
                    .map_err(|error| self.invalid(FieldPath::root("connector_split"), error))?,
            ),
            novarocks_proto_codec::connector_read::SplitCategory::TableChanges => {
                IcebergReadSplit::TableChanges(
                    TableChangesSplit::from_connector_split_proto(split.as_proto())
                        .map_err(|error| self.invalid(FieldPath::root("connector_split"), error))?,
                )
            }
            novarocks_proto_codec::connector_read::SplitCategory::ChangeWindow => {
                IcebergReadSplit::ChangeWindow(
                    IcebergChangeSplit::from_connector_split_proto(split.as_proto())
                        .map_err(|error| self.invalid(FieldPath::root("connector_split"), error))?,
                )
            }
            novarocks_proto_codec::connector_read::SplitCategory::SystemFiles => {
                IcebergReadSplit::SystemFiles(
                    FilesTableSplit::from_connector_split_proto(split.as_proto())
                        .map_err(|error| self.invalid(FieldPath::root("connector_split"), error))?,
                )
            }
            novarocks_proto_codec::connector_read::SplitCategory::RewritePositionDeleteFiles => {
                IcebergReadSplit::RewritePositionDeleteFiles(
                    IcebergRewritePositionDeleteFilesSplit::from_connector_split_proto(
                        split.as_proto(),
                    )
                    .map_err(|error| self.invalid(FieldPath::root("connector_split"), error))?,
                )
            }
        };
        Ok(self.adapter.wrap_split(split))
    }

    fn encode_split(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError> {
        let split = self
            .adapter
            .split(split)
            .map_err(|error| self.invalid(FieldPath::root("connector_split"), error))?;
        Ok(match split {
            IcebergReadSplit::Data(value) => value.to_connector_split_proto(),
            IcebergReadSplit::TableChanges(value) => value.to_connector_split_proto(),
            IcebergReadSplit::ChangeWindow(value) => value.to_connector_split_proto(),
            IcebergReadSplit::SystemFiles(value) => value.to_connector_split_proto(),
            IcebergReadSplit::RewritePositionDeleteFiles(value) => value.to_connector_split_proto(),
        })
    }
}

type IcebergTableHandleSplit = super::IcebergSplit;
