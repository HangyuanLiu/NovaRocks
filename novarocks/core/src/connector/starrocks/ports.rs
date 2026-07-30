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

//! Explicit core-to-compat connector ports.
//!
//! These are consumer-owned composition contracts, not a provider SPI. The
//! core kernel owns domain facts; a compat application installs the StarRocks
//! wire implementation for its fragment/service host.

use std::collections::HashMap;
use std::fmt;

use crate::common::types::UniqueId;
use crate::connector::starrocks::lake::storage_domain::{
    StorageBundleFile, StorageBundleMetadata, StorageCombinedTransactionLog, StorageTabletMetadata,
    StorageTransactionLog,
};
use crate::connector::starrocks::lake_meta::{LakeMetaStorageFacts, LakeMetaStorageRequest};
use crate::connector::starrocks::schema::LakeScanTableSchema;
use crate::runtime::starlet_shard_registry::{S3StoreConfig, StarletShardInfo};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendEndpoint {
    pub host: String,
    pub port: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableSchemaRequestSource {
    Scan,
    Load,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSchemaRequest {
    pub endpoint: FrontendEndpoint,
    pub db_id: i64,
    pub table_id: i64,
    pub schema_id: i64,
    pub source: TableSchemaRequestSource,
    pub tablet_id: Option<i64>,
    pub query_id: Option<UniqueId>,
    pub txn_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWireErrorKind {
    Unavailable,
    Transport,
    NotFound,
    Invalid,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWireError {
    kind: ConnectorWireErrorKind,
    message: String,
}

impl ConnectorWireError {
    pub fn new(kind: ConnectorWireErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> ConnectorWireErrorKind {
        self.kind
    }
}

impl fmt::Display for ConnectorWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ConnectorWireError {}

pub trait TableSchemaProvider: Send + Sync {
    fn fetch_table_schema(
        &self,
        request: &TableSchemaRequest,
    ) -> Result<LakeScanTableSchema, ConnectorWireError>;
}

pub trait LakeMetaStorageResolver: Send + Sync {
    fn resolve(&self, request: &LakeMetaStorageRequest) -> Result<LakeMetaStorageFacts, String>;
}

/// File-boundary codec for lake tablet metadata. Core consumes the decoded
/// domain facts, while compat owns StarRocks protobuf parsing and encoding.
pub trait StorageMetadataProvider: Send + Sync {
    /// Encodes a tablet schema only at a StarRocks storage boundary. Core
    /// callers retain the domain schema and never observe generated messages.
    fn encode_tablet_schema(
        &self,
        schema: &crate::connector::starrocks::schema::StarRocksTabletSchema,
    ) -> Result<Vec<u8>, String>;

    /// Decodes a persisted StarRocks schema into the core domain model.
    fn decode_tablet_schema(
        &self,
        bytes: &[u8],
    ) -> Result<crate::connector::starrocks::schema::StarRocksTabletSchema, String>;

    fn decode_tablet_metadata(&self, bytes: &[u8]) -> Result<StorageTabletMetadata, String>;

    fn encode_tablet_metadata(&self, metadata: &StorageTabletMetadata) -> Result<Vec<u8>, String>;

    fn decode_bundle_metadata(&self, bytes: &[u8]) -> Result<StorageBundleMetadata, String>;

    fn decode_bundle_file(&self, bytes: &[u8]) -> Result<StorageBundleFile, String>;

    fn encode_bundle_file(&self, bundle: &StorageBundleFile) -> Result<Vec<u8>, String>;

    fn rewrite_tablet_metadata_version(
        &self,
        bytes: &[u8],
        version: i64,
    ) -> Result<Vec<u8>, String>;

    fn decode_transaction_log(&self, bytes: &[u8]) -> Result<StorageTransactionLog, String>;

    fn encode_transaction_log(&self, log: &StorageTransactionLog) -> Result<Vec<u8>, String>;

    fn decode_combined_transaction_log(
        &self,
        bytes: &[u8],
    ) -> Result<StorageCombinedTransactionLog, String>;

    fn encode_combined_transaction_log(
        &self,
        log: &StorageCombinedTransactionLog,
    ) -> Result<Vec<u8>, String>;
}

/// Narrow Starlet/StarManager metadata port. The core execution kernel only
/// consumes resolved shard locations and object-store credentials; the compat
/// adapter owns the StarOS protobuf and gRPC client.
pub trait StarletMetadataProvider: Send + Sync {
    fn retrieve_shard_infos(
        &self,
        tablet_ids: &[i64],
    ) -> Result<HashMap<i64, StarletShardInfo>, String>;

    fn retrieve_s3_config_for_path(&self, path: &str) -> Result<Option<S3StoreConfig>, String>;
}

/// Domain facts returned by the FE automatic-partition operation. These are
/// deliberately independent of generated Thrift values so the write kernel
/// can consume them without owning the FE wire protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkFrontendAddress {
    pub host: String,
    pub port: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticPartitionRequest {
    pub frontend: SinkFrontendAddress,
    pub db_id: i64,
    pub table_id: i64,
    pub txn_id: i64,
    pub is_temp: bool,
    pub partition_values: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticPartitionResult {
    pub partitions: Vec<AutomaticPartitionEntry>,
    pub tablets: Vec<AutomaticPartitionTablet>,
    pub nodes: Vec<AutomaticPartitionNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticPartitionEntry {
    pub partition_id: i64,
    pub is_shadow: bool,
    pub indexes: Vec<AutomaticPartitionIndex>,
    pub start_key: Option<Vec<AutomaticPartitionKey>>,
    pub end_key: Option<Vec<AutomaticPartitionKey>>,
    pub in_keys: Vec<Vec<AutomaticPartitionKey>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticPartitionIndex {
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticPartitionTablet {
    pub tablet_id: i64,
    pub node_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticPartitionNode {
    pub id: i64,
    pub option: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutomaticPartitionKey {
    Null,
    Bool(bool),
    Int(i128),
    Date32(i32),
    TimestampMicros(i64),
    Decimal { value: i128, scale: i8 },
    Utf8(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoIncrementRange {
    pub start: i64,
    pub end: i64,
}

/// Narrow FE control port used by the StarRocks write kernel. A compat host
/// installs the wire implementation while native mode leaves it absent.
pub trait SinkFrontendProvider: Send + Sync {
    fn create_automatic_partitions(
        &self,
        request: &AutomaticPartitionRequest,
    ) -> Result<AutomaticPartitionResult, String>;

    fn allocate_auto_increment_range(
        &self,
        frontend: &SinkFrontendAddress,
        table_id: i64,
        rows: usize,
    ) -> Result<AutoIncrementRange, String>;
}
