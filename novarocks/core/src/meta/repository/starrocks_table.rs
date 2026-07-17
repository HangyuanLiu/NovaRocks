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

use serde::de::{Error as DeError, SeqAccess, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_STARROCKS, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};

const STARROCKS_DATABASE_KIND: &str = "starrocks.database";
const STARROCKS_DATABASE_NAME_KIND: &str = "starrocks.database_name";
const STARROCKS_TABLE_KIND: &str = "starrocks.table";
const STARROCKS_TABLE_NAME_KIND: &str = "starrocks.table_name";
const STARROCKS_SCHEMA_KIND: &str = "starrocks.schema";
const STARROCKS_COLUMN_KIND: &str = "starrocks.column";
const STARROCKS_PARTITION_KIND: &str = "starrocks.partition";
const STARROCKS_INDEX_KIND: &str = "starrocks.index";
const STARROCKS_TABLET_KIND: &str = "starrocks.tablet";

#[derive(Default)]
pub struct StarRocksTableMetaRepository;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StarRocksTableSnapshot {
    pub databases: Vec<StoredStarRocksDatabase>,
    pub tables: Vec<StoredStarRocksTable>,
    pub schemas: Vec<StoredStarRocksSchema>,
    pub columns: Vec<StoredStarRocksColumn>,
    pub partitions: Vec<StoredStarRocksPartition>,
    pub indexes: Vec<StoredStarRocksIndex>,
    pub tablets: Vec<StoredStarRocksTablet>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksDatabase {
    pub db_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksTable {
    pub table_id: i64,
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
    pub state: StarRocksTableState,
    pub kind: StarRocksTableKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksSchema {
    pub schema_id: i64,
    pub table_id: i64,
    pub schema_version: i64,
    pub tablet_schema_pb: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredStarRocksSchemaAvro {
    schema_id: i64,
    table_id: i64,
    schema_version: i64,
    #[serde(with = "avro_bytes_vec")]
    tablet_schema_pb: Vec<u8>,
}

impl From<&StoredStarRocksSchema> for StoredStarRocksSchemaAvro {
    fn from(value: &StoredStarRocksSchema) -> Self {
        Self {
            schema_id: value.schema_id,
            table_id: value.table_id,
            schema_version: value.schema_version,
            tablet_schema_pb: value.tablet_schema_pb.clone(),
        }
    }
}

impl From<StoredStarRocksSchemaAvro> for StoredStarRocksSchema {
    fn from(value: StoredStarRocksSchemaAvro) -> Self {
        Self {
            schema_id: value.schema_id,
            table_id: value.table_id,
            schema_version: value.schema_version,
            tablet_schema_pb: value.tablet_schema_pb,
        }
    }
}

mod avro_bytes_vec {
    use super::*;

    pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(BytesVecVisitor)
    }

    struct BytesVecVisitor;

    impl<'de> Visitor<'de> for BytesVecVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("Avro bytes")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            Ok(value.to_vec())
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            Ok(value)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(byte) = seq.next_element()? {
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksColumn {
    pub schema_id: i64,
    pub ordinal: i64,
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksPartition {
    pub partition_id: i64,
    pub table_id: i64,
    pub name: String,
    pub visible_version: i64,
    pub next_version: i64,
    pub state: StarRocksPartitionState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksIndex {
    pub index_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub index_type: String,
    pub state: StarRocksIndexState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksTablet {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub index_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StarRocksPartitionState {
    Creating,
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StarRocksTableState {
    Creating,
    Active,
    Dropping,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StarRocksTableKind {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StarRocksIndexState {
    Creating,
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateStarRocksDatabaseRequest {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateStarRocksTableRequest {
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
    pub state: StarRocksTableState,
    pub kind: StarRocksTableKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateStarRocksColumnRequest {
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateStarRocksTableLayoutRequest {
    pub db_id: i64,
    pub table_name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub kind: StarRocksTableKind,
    pub schema_version: i64,
    pub tablet_schema_pb: Vec<u8>,
    pub columns: Vec<CreateStarRocksColumnRequest>,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedStarRocksTableLayout {
    pub table: StoredStarRocksTable,
    pub schema: StoredStarRocksSchema,
    pub columns: Vec<StoredStarRocksColumn>,
    pub partition: StoredStarRocksPartition,
    pub index: StoredStarRocksIndex,
    pub tablets: Vec<StoredStarRocksTablet>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageStarRocksTruncateRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedStarRocksTruncate {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageStarRocksMvRefreshRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedStarRocksMvRefresh {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IdLookup {
    id: i64,
}

impl StarRocksTableMetaRepository {
    pub fn get_or_create_database(
        &self,
        txn: &mut dyn MetaWriteTxn,
        name: &str,
    ) -> RepositoryResult<StoredStarRocksDatabase> {
        if let Some(database) = self.load_database_by_name(txn, name)? {
            return Ok(database);
        }
        self.create_database(
            txn,
            CreateStarRocksDatabaseRequest {
                name: name.to_string(),
            },
        )
    }

    pub fn load_database_by_name(
        &self,
        txn: &dyn MetaReadTxn,
        name: &str,
    ) -> RepositoryResult<Option<StoredStarRocksDatabase>> {
        let Some(record) = txn.get(&key_database_name(name)?)? else {
            return Ok(None);
        };
        let lookup: IdLookup = decode_record_payload(&record, STARROCKS_DATABASE_NAME_KIND)?;
        self.load_database(txn, lookup.id)
    }

    pub fn load_database(
        &self,
        txn: &dyn MetaReadTxn,
        db_id: i64,
    ) -> RepositoryResult<Option<StoredStarRocksDatabase>> {
        txn.get(&key_database(db_id)?)?
            .map(|record| decode_record_payload(&record, STARROCKS_DATABASE_KIND))
            .transpose()
    }

    pub fn create_database(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateStarRocksDatabaseRequest,
    ) -> RepositoryResult<StoredStarRocksDatabase> {
        let lookup_key = key_database_name(&req.name)?;
        if let Some(record) = txn.get(&lookup_key)? {
            let _: IdLookup = decode_record_payload(&record, STARROCKS_DATABASE_NAME_KIND)?;
            return Err(RepositoryError::conflict(format!(
                "StarRocks database {} already exists",
                req.name
            )));
        }

        let database = StoredStarRocksDatabase {
            db_id: txn.allocate_id(id_scopes::starrocks_db())?,
            name: req.name,
        };
        txn.put(MetaRecordPut::new(
            key_database(database.db_id)?,
            record_kind(STARROCKS_DATABASE_KIND)?,
            ExpectedRevision::NotExists,
            encode_record_payload(STARROCKS_DATABASE_KIND, &database)?,
        ))?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            record_kind(STARROCKS_DATABASE_NAME_KIND)?,
            ExpectedRevision::NotExists,
            encode_record_payload(
                STARROCKS_DATABASE_NAME_KIND,
                &IdLookup { id: database.db_id },
            )?,
        ))?;
        Ok(database)
    }

    pub fn create_table(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateStarRocksTableRequest,
    ) -> RepositoryResult<StoredStarRocksTable> {
        let lookup_key = key_table_name(req.db_id, &req.name)?;
        if let Some(record) = txn.get(&lookup_key)? {
            let _: IdLookup = decode_record_payload(&record, STARROCKS_TABLE_NAME_KIND)?;
            return Err(RepositoryError::conflict(format!(
                "StarRocks table {} already exists",
                req.name
            )));
        }

        let table = StoredStarRocksTable {
            table_id: txn.allocate_id(id_scopes::starrocks_table())?,
            db_id: req.db_id,
            name: req.name,
            keys_type: req.keys_type,
            bucket_num: req.bucket_num,
            current_schema_id: req.current_schema_id,
            state: req.state,
            kind: req.kind,
        };
        txn.put(MetaRecordPut::new(
            key_table(table.table_id)?,
            record_kind(STARROCKS_TABLE_KIND)?,
            ExpectedRevision::NotExists,
            encode_record_payload(STARROCKS_TABLE_KIND, &table)?,
        ))?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            record_kind(STARROCKS_TABLE_NAME_KIND)?,
            ExpectedRevision::NotExists,
            encode_record_payload(STARROCKS_TABLE_NAME_KIND, &IdLookup { id: table.table_id })?,
        ))?;
        Ok(table)
    }

    pub fn create_table_layout(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateStarRocksTableLayoutRequest,
    ) -> RepositoryResult<CreatedStarRocksTableLayout> {
        if req.bucket_num <= 0 {
            return Err(RepositoryError::invalid(format!(
                "StarRocks table bucket_num must be positive, got {}",
                req.bucket_num
            )));
        }
        self.load_database(txn, req.db_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("StarRocks database {} not found", req.db_id))
        })?;

        let lookup_key = key_table_name(req.db_id, &req.table_name)?;
        if let Some(record) = txn.get(&lookup_key)? {
            let _: IdLookup = decode_record_payload(&record, STARROCKS_TABLE_NAME_KIND)?;
            return Err(RepositoryError::conflict(format!(
                "StarRocks table {} already exists",
                req.table_name
            )));
        }

        let table_id = txn.allocate_id(id_scopes::starrocks_table())?;
        let schema_id = table_id;
        let partition_id = txn.allocate_id(id_scopes::starrocks_partition())?;
        let index_id = txn.allocate_id(id_scopes::starrocks_index())?;
        let partition_root_path =
            tablet_root_path(&req.warehouse_uri, req.db_id, table_id, partition_id);

        let table = StoredStarRocksTable {
            table_id,
            db_id: req.db_id,
            name: req.table_name,
            keys_type: req.keys_type,
            bucket_num: req.bucket_num,
            current_schema_id: schema_id,
            state: StarRocksTableState::Active,
            kind: req.kind,
        };
        put_table(txn, &table, ExpectedRevision::NotExists)?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            record_kind(STARROCKS_TABLE_NAME_KIND)?,
            ExpectedRevision::NotExists,
            encode_record_payload(STARROCKS_TABLE_NAME_KIND, &IdLookup { id: table.table_id })?,
        ))?;

        let schema = StoredStarRocksSchema {
            schema_id,
            table_id,
            schema_version: req.schema_version,
            tablet_schema_pb: req.tablet_schema_pb,
        };
        put_schema(txn, &schema, ExpectedRevision::NotExists)?;

        let columns = req
            .columns
            .into_iter()
            .enumerate()
            .map(|(ordinal, column)| StoredStarRocksColumn {
                schema_id,
                ordinal: ordinal as i64,
                column_name: column.column_name,
                logical_type: column.logical_type,
                nullable: column.nullable,
                visible: column.visible,
                is_key: column.is_key,
            })
            .collect::<Vec<_>>();
        for column in &columns {
            put_column(txn, column, ExpectedRevision::NotExists)?;
        }

        let partition = StoredStarRocksPartition {
            partition_id,
            table_id,
            name: req.partition_name,
            visible_version: 1,
            next_version: 2,
            state: StarRocksPartitionState::Active,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;

        let index = StoredStarRocksIndex {
            index_id,
            table_id,
            partition_id,
            index_type: "BASE".to_string(),
            state: StarRocksIndexState::Active,
        };
        put_index(txn, &index, ExpectedRevision::NotExists)?;

        let mut tablets = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet = StoredStarRocksTablet {
                tablet_id: txn.allocate_id(id_scopes::starrocks_tablet())?,
                partition_id,
                index_id,
                bucket_seq,
                tablet_root_path: partition_root_path.clone(),
            };
            put_tablet(txn, &tablet, ExpectedRevision::NotExists)?;
            tablets.push(tablet);
        }

        Ok(CreatedStarRocksTableLayout {
            table,
            schema,
            columns,
            partition,
            index,
            tablets,
            partition_root_path,
        })
    }

    pub fn create_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        name: &str,
        visible_version: i64,
    ) -> RepositoryResult<StoredStarRocksPartition> {
        let partition = StoredStarRocksPartition {
            partition_id: txn.allocate_id(id_scopes::starrocks_partition())?,
            table_id,
            name: name.to_string(),
            visible_version,
            next_version: visible_version + 1,
            state: StarRocksPartitionState::Active,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;
        Ok(partition)
    }

    pub fn load_snapshot(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<StarRocksTableSnapshot> {
        let mut snapshot = StarRocksTableSnapshot {
            databases: scan_values(txn, "database", STARROCKS_DATABASE_KIND)?,
            tables: scan_values(txn, "table", STARROCKS_TABLE_KIND)?,
            schemas: scan_values::<StoredStarRocksSchemaAvro>(
                txn,
                "schema",
                STARROCKS_SCHEMA_KIND,
            )?
            .into_iter()
            .map(Into::into)
            .collect(),
            columns: scan_values(txn, "column", STARROCKS_COLUMN_KIND)?,
            partitions: scan_values(txn, "partition", STARROCKS_PARTITION_KIND)?,
            indexes: scan_values(txn, "index", STARROCKS_INDEX_KIND)?,
            tablets: scan_values(txn, "tablet", STARROCKS_TABLET_KIND)?,
        };
        snapshot.databases.sort_by_key(|value| value.db_id);
        snapshot.tables.sort_by_key(|value| value.table_id);
        snapshot.schemas.sort_by_key(|value| value.schema_id);
        snapshot
            .columns
            .sort_by_key(|value| (value.schema_id, value.ordinal));
        snapshot.partitions.sort_by_key(|value| value.partition_id);
        snapshot.indexes.sort_by_key(|value| value.index_id);
        snapshot.tablets.sort_by_key(|value| value.tablet_id);
        Ok(snapshot)
    }

    pub fn load_partition(
        &self,
        txn: &dyn MetaReadTxn,
        partition_id: i64,
    ) -> RepositoryResult<Option<StoredStarRocksPartition>> {
        Ok(self
            .load_versioned_partition(txn, partition_id)?
            .map(|(_, partition)| partition))
    }

    pub fn load_versioned_partition(
        &self,
        txn: &dyn MetaReadTxn,
        partition_id: i64,
    ) -> RepositoryResult<Option<(MetaRevision, StoredStarRocksPartition)>> {
        txn.get(&key_partition(partition_id)?)?
            .map(|record| {
                let revision = record.revision.clone();
                let partition = decode_record_payload(&record, STARROCKS_PARTITION_KIND)?;
                Ok((revision, partition))
            })
            .transpose()
    }

    pub fn update_partition_exact(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition: &StoredStarRocksPartition,
        expected: MetaRevision,
    ) -> RepositoryResult<()> {
        put_partition(txn, partition, ExpectedRevision::Exact(expected))
    }

    pub fn update_schema_payload(
        &self,
        txn: &mut dyn MetaWriteTxn,
        schema_id: i64,
        tablet_schema_pb: Vec<u8>,
    ) -> RepositoryResult<()> {
        let Some((revision, mut schema)) = load_versioned_schema(txn, schema_id)? else {
            return Err(RepositoryError::not_found(format!(
                "StarRocks schema {schema_id} not found"
            )));
        };
        schema.tablet_schema_pb = tablet_schema_pb;
        put_schema(txn, &schema, ExpectedRevision::Exact(revision))
    }

    pub fn mark_table_dropping(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
    ) -> RepositoryResult<()> {
        let (table_revision, mut table) =
            self.load_versioned_table(txn, table_id)?.ok_or_else(|| {
                RepositoryError::not_found(format!("StarRocks table {table_id} not found"))
            })?;
        if table.state == StarRocksTableState::Dropping {
            return Ok(());
        }
        if table.state != StarRocksTableState::Active {
            return Err(RepositoryError::conflict(format!(
                "StarRocks table {table_id} is {:?}, expected Active",
                table.state
            )));
        }
        if self.load_snapshot(txn)?.partitions.iter().any(|partition| {
            partition.table_id == table_id && partition.state == StarRocksPartitionState::Creating
        }) {
            return Err(RepositoryError::conflict(format!(
                "cannot drop table {table_id}: refresh in progress"
            )));
        }

        table.state = StarRocksTableState::Dropping;
        put_table(txn, &table, ExpectedRevision::Exact(table_revision))?;

        for (revision, mut partition) in self.load_versioned_partitions_for_table(txn, table_id)? {
            if partition.state == StarRocksPartitionState::Active {
                partition.state = StarRocksPartitionState::Retired;
                put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
            }
        }
        for (revision, mut index) in self.load_versioned_indexes_for_table(txn, table_id)? {
            if index.state == StarRocksIndexState::Active {
                index.state = StarRocksIndexState::Retired;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            }
        }
        Ok(())
    }

    pub fn stage_truncate_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: StageStarRocksTruncateRequest,
    ) -> RepositoryResult<StagedStarRocksTruncate> {
        if req.bucket_num <= 0 {
            return Err(RepositoryError::invalid(format!(
                "StarRocks table bucket_num must be positive, got {}",
                req.bucket_num
            )));
        }
        let table = self.load_table(txn, req.table_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("StarRocks table {} not found", req.table_id))
        })?;
        if table.state != StarRocksTableState::Active {
            return Err(RepositoryError::conflict(format!(
                "StarRocks table {} is {:?}, expected Active",
                req.table_id, table.state
            )));
        }

        let partition_id = txn.allocate_id(id_scopes::starrocks_partition())?;
        let index_id = txn.allocate_id(id_scopes::starrocks_index())?;
        let partition_root_path =
            tablet_root_path(&req.warehouse_uri, req.db_id, req.table_id, partition_id);

        let partition = StoredStarRocksPartition {
            partition_id,
            table_id: req.table_id,
            name: req.partition_name,
            visible_version: 1,
            next_version: 2,
            state: StarRocksPartitionState::Creating,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;

        let index = StoredStarRocksIndex {
            index_id,
            table_id: req.table_id,
            partition_id,
            index_type: "BASE".to_string(),
            state: StarRocksIndexState::Creating,
        };
        put_index(txn, &index, ExpectedRevision::NotExists)?;

        let mut tablet_ids = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet_id = txn.allocate_id(id_scopes::starrocks_tablet())?;
            let tablet = StoredStarRocksTablet {
                tablet_id,
                partition_id,
                index_id,
                bucket_seq,
                tablet_root_path: partition_root_path.clone(),
            };
            put_tablet(txn, &tablet, ExpectedRevision::NotExists)?;
            tablet_ids.push(tablet_id);
        }

        Ok(StagedStarRocksTruncate {
            partition_id,
            index_id,
            tablet_ids,
            partition_root_path,
        })
    }

    pub fn activate_truncate_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        old_partition_id: i64,
        new_partition_id: i64,
        new_index_id: i64,
    ) -> RepositoryResult<()> {
        let partitions = self.load_versioned_partitions_for_table(txn, table_id)?;
        let mut saw_old = false;
        let mut saw_new = false;
        for (revision, mut partition) in partitions {
            if partition.partition_id == new_partition_id {
                if partition.state != StarRocksPartitionState::Creating {
                    return Err(RepositoryError::conflict(format!(
                        "StarRocks partition {new_partition_id} is {:?}, expected Creating",
                        partition.state
                    )));
                }
                partition.state = StarRocksPartitionState::Active;
                partition.visible_version = 1;
                partition.next_version = 2;
                saw_new = true;
                put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
            } else if partition.partition_id == old_partition_id {
                if partition.state == StarRocksPartitionState::Active {
                    partition.state = StarRocksPartitionState::Retired;
                    put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
                }
                saw_old = true;
            }
        }
        if !saw_old {
            return Err(RepositoryError::not_found(format!(
                "StarRocks partition {old_partition_id} not found"
            )));
        }
        if !saw_new {
            return Err(RepositoryError::not_found(format!(
                "StarRocks partition {new_partition_id} not found"
            )));
        }

        let mut saw_new_index = false;
        for (revision, mut index) in self.load_versioned_indexes_for_table(txn, table_id)? {
            if index.index_id == new_index_id {
                if index.state != StarRocksIndexState::Creating {
                    return Err(RepositoryError::conflict(format!(
                        "StarRocks index {new_index_id} is {:?}, expected Creating",
                        index.state
                    )));
                }
                index.state = StarRocksIndexState::Active;
                saw_new_index = true;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            } else if index.partition_id == old_partition_id
                && index.state == StarRocksIndexState::Active
            {
                index.state = StarRocksIndexState::Retired;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            }
        }
        if !saw_new_index {
            return Err(RepositoryError::not_found(format!(
                "StarRocks index {new_index_id} not found"
            )));
        }
        Ok(())
    }

    pub fn stage_mv_refresh_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: StageStarRocksMvRefreshRequest,
    ) -> RepositoryResult<StagedStarRocksMvRefresh> {
        if req.bucket_num <= 0 {
            return Err(RepositoryError::invalid(format!(
                "StarRocks materialized view bucket_num must be positive, got {}",
                req.bucket_num
            )));
        }
        let table = self.load_table(txn, req.table_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("StarRocks table {} not found", req.table_id))
        })?;
        if table.kind != StarRocksTableKind::MaterializedView {
            return Err(RepositoryError::conflict(format!(
                "table {} is not a materialized view",
                req.table_id
            )));
        }
        if table.state != StarRocksTableState::Active {
            return Err(RepositoryError::conflict(format!(
                "materialized view {} is {:?}, expected Active",
                req.table_id, table.state
            )));
        }
        if self.load_snapshot(txn)?.partitions.iter().any(|partition| {
            partition.table_id == req.table_id
                && partition.state == StarRocksPartitionState::Creating
        }) {
            return Err(RepositoryError::conflict(format!(
                "cannot refresh materialized view {}: refresh already in progress",
                req.table_id
            )));
        }

        let partition_id = txn.allocate_id(id_scopes::starrocks_partition())?;
        let index_id = txn.allocate_id(id_scopes::starrocks_index())?;
        let partition_root_path =
            tablet_root_path(&req.warehouse_uri, req.db_id, req.table_id, partition_id);

        let partition = StoredStarRocksPartition {
            partition_id,
            table_id: req.table_id,
            name: req.partition_name,
            visible_version: 1,
            next_version: 2,
            state: StarRocksPartitionState::Creating,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;

        let index = StoredStarRocksIndex {
            index_id,
            table_id: req.table_id,
            partition_id,
            index_type: "BASE".to_string(),
            state: StarRocksIndexState::Creating,
        };
        put_index(txn, &index, ExpectedRevision::NotExists)?;

        let mut tablet_ids = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet_id = txn.allocate_id(id_scopes::starrocks_tablet())?;
            let tablet = StoredStarRocksTablet {
                tablet_id,
                partition_id,
                index_id,
                bucket_seq,
                tablet_root_path: partition_root_path.clone(),
            };
            put_tablet(txn, &tablet, ExpectedRevision::NotExists)?;
            tablet_ids.push(tablet_id);
        }

        Ok(StagedStarRocksMvRefresh {
            partition_id,
            index_id,
            tablet_ids,
            partition_root_path,
        })
    }

    pub fn activate_mv_refresh_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        old_partition_id: i64,
        new_partition_id: i64,
        new_index_id: i64,
    ) -> RepositoryResult<()> {
        let partitions = self.load_versioned_partitions_for_table(txn, table_id)?;
        let mut saw_old = false;
        let mut saw_new = false;
        for (revision, mut partition) in partitions {
            if partition.partition_id == new_partition_id {
                match partition.state {
                    StarRocksPartitionState::Creating => {
                        partition.state = StarRocksPartitionState::Active;
                        partition.visible_version = 2;
                        partition.next_version = 3;
                        put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
                    }
                    StarRocksPartitionState::Active => {
                        if partition.visible_version != 2 || partition.next_version != 3 {
                            return Err(RepositoryError::conflict(format!(
                                "StarRocks partition {new_partition_id} active versions are {}/{}, expected 2/3",
                                partition.visible_version, partition.next_version
                            )));
                        }
                    }
                    _ => {
                        return Err(RepositoryError::conflict(format!(
                            "StarRocks partition {new_partition_id} is {:?}, expected Creating",
                            partition.state
                        )));
                    }
                }
                saw_new = true;
            } else if partition.partition_id == old_partition_id {
                if partition.state == StarRocksPartitionState::Active {
                    partition.state = StarRocksPartitionState::Retired;
                    put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
                }
                saw_old = true;
            }
        }
        if !saw_old {
            return Err(RepositoryError::not_found(format!(
                "StarRocks partition {old_partition_id} not found"
            )));
        }
        if !saw_new {
            return Err(RepositoryError::not_found(format!(
                "StarRocks partition {new_partition_id} not found"
            )));
        }

        let mut saw_new_index = false;
        for (revision, mut index) in self.load_versioned_indexes_for_table(txn, table_id)? {
            if index.index_id == new_index_id {
                match index.state {
                    StarRocksIndexState::Creating => {
                        index.state = StarRocksIndexState::Active;
                        saw_new_index = true;
                        put_index(txn, &index, ExpectedRevision::Exact(revision))?;
                    }
                    StarRocksIndexState::Active => {
                        saw_new_index = true;
                    }
                    _ => {
                        return Err(RepositoryError::conflict(format!(
                            "StarRocks index {new_index_id} is {:?}, expected Creating",
                            index.state
                        )));
                    }
                }
            } else if index.partition_id == old_partition_id
                && index.state == StarRocksIndexState::Active
            {
                index.state = StarRocksIndexState::Retired;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            }
        }
        if !saw_new_index {
            return Err(RepositoryError::not_found(format!(
                "StarRocks index {new_index_id} not found"
            )));
        }
        Ok(())
    }

    pub fn delete_creating_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition_id: i64,
    ) -> RepositoryResult<()> {
        let Some((partition_revision, partition)) =
            self.load_versioned_partition(txn, partition_id)?
        else {
            return Ok(());
        };
        if partition.state != StarRocksPartitionState::Creating {
            return Ok(());
        }
        for (revision, tablet) in load_versioned_tablets_for_partition(txn, partition_id)? {
            txn.delete(
                &key_tablet(tablet.tablet_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
        for (revision, index) in load_versioned_indexes_for_partition(txn, partition_id)? {
            if index.state == StarRocksIndexState::Creating {
                txn.delete(
                    &key_index(index.index_id)?,
                    ExpectedRevision::Exact(revision),
                )?;
            }
        }
        txn.delete(
            &key_partition(partition_id)?,
            ExpectedRevision::Exact(partition_revision),
        )?;
        Ok(())
    }

    pub fn fail_creating_tables(&self, txn: &mut dyn MetaWriteTxn) -> RepositoryResult<Vec<i64>> {
        let mut failed = Vec::new();
        for (revision, mut table) in load_versioned_tables(txn)? {
            if table.state == StarRocksTableState::Creating {
                table.state = StarRocksTableState::Failed;
                failed.push(table.table_id);
                put_table(txn, &table, ExpectedRevision::Exact(revision))?;
            }
        }
        Ok(failed)
    }

    pub fn delete_all_creating_partitions(
        &self,
        txn: &mut dyn MetaWriteTxn,
    ) -> RepositoryResult<Vec<i64>> {
        let partition_ids = self
            .load_snapshot(txn)?
            .partitions
            .into_iter()
            .filter(|partition| partition.state == StarRocksPartitionState::Creating)
            .map(|partition| partition.partition_id)
            .collect::<Vec<_>>();
        for partition_id in &partition_ids {
            self.delete_creating_partition(txn, *partition_id)?;
        }
        Ok(partition_ids)
    }

    pub fn drop_database_entry(
        &self,
        txn: &mut dyn MetaWriteTxn,
        database_name: &str,
    ) -> RepositoryResult<bool> {
        let lookup_key = key_database_name(database_name)?;
        let Some(record) = txn.get(&lookup_key)? else {
            return Ok(false);
        };
        let lookup: IdLookup = decode_record_payload(&record, STARROCKS_DATABASE_NAME_KIND)?;
        let Some(database_record) = txn.get(&key_database(lookup.id)?)? else {
            txn.delete(&lookup_key, ExpectedRevision::Exact(record.revision))?;
            return Ok(true);
        };
        txn.delete(
            &key_database(lookup.id)?,
            ExpectedRevision::Exact(database_record.revision),
        )?;
        txn.delete(&lookup_key, ExpectedRevision::Exact(record.revision))?;
        Ok(true)
    }

    pub fn purge_dropping_table_for_reuse(
        &self,
        txn: &mut dyn MetaWriteTxn,
        db_id: i64,
        table_name: &str,
    ) -> RepositoryResult<Vec<i64>> {
        let target = normalize_lookup_name(table_name);
        let table_ids = self
            .load_snapshot(txn)?
            .tables
            .into_iter()
            .filter(|table| {
                table.db_id == db_id
                    && table.state == StarRocksTableState::Dropping
                    && normalize_lookup_name(&table.name) == target
            })
            .map(|table| table.table_id)
            .collect::<Vec<_>>();
        for table_id in &table_ids {
            purge_table_owned_metadata(self, txn, *table_id, false)?;
        }
        Ok(table_ids)
    }

    pub fn purge_retired_table_metadata(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
    ) -> RepositoryResult<()> {
        purge_table_owned_metadata(self, txn, table_id, true)
    }

    pub fn purge_retired_partition_metadata(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition_id: i64,
    ) -> RepositoryResult<()> {
        let Some((partition_revision, partition)) =
            self.load_versioned_partition(txn, partition_id)?
        else {
            return Ok(());
        };
        if partition.state != StarRocksPartitionState::Retired {
            return Err(RepositoryError::conflict(format!(
                "cannot purge StarRocks partition {partition_id}: partition is not retired"
            )));
        }
        for (revision, tablet) in load_versioned_tablets_for_partition(txn, partition_id)? {
            txn.delete(
                &key_tablet(tablet.tablet_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
        for (revision, index) in load_versioned_indexes_for_partition(txn, partition_id)? {
            txn.delete(
                &key_index(index.index_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
        txn.delete(
            &key_partition(partition_id)?,
            ExpectedRevision::Exact(partition_revision),
        )?;
        Ok(())
    }

    pub fn load_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Option<StoredStarRocksTable>> {
        Ok(self
            .load_versioned_table(txn, table_id)?
            .map(|(_, table)| table))
    }

    pub fn load_versioned_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Option<(MetaRevision, StoredStarRocksTable)>> {
        txn.get(&key_table(table_id)?)?
            .map(|record| {
                let revision = record.revision.clone();
                let table = decode_record_payload(&record, STARROCKS_TABLE_KIND)?;
                Ok((revision, table))
            })
            .transpose()
    }

    fn load_versioned_partitions_for_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksPartition)>> {
        load_versioned_partitions(txn).map(|partitions| {
            partitions
                .into_iter()
                .filter(|(_, partition)| partition.table_id == table_id)
                .collect()
        })
    }

    fn load_versioned_indexes_for_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksIndex)>> {
        load_versioned_indexes(txn).map(|indexes| {
            indexes
                .into_iter()
                .filter(|(_, index)| index.table_id == table_id)
                .collect()
        })
    }
}

fn purge_table_owned_metadata(
    repo: &StarRocksTableMetaRepository,
    txn: &mut dyn MetaWriteTxn,
    table_id: i64,
    require_dropping: bool,
) -> RepositoryResult<()> {
    let Some((table_revision, table)) = repo.load_versioned_table(txn, table_id)? else {
        return Ok(());
    };
    if require_dropping && table.state != StarRocksTableState::Dropping {
        return Err(RepositoryError::conflict(format!(
            "cannot purge StarRocks table {table_id}: table is not dropping"
        )));
    }

    let schema_ids = load_versioned_schemas(txn)?
        .into_iter()
        .filter(|(_, schema)| schema.table_id == table_id)
        .collect::<Vec<_>>();
    for (revision, column) in load_versioned_columns(txn)? {
        if schema_ids
            .iter()
            .any(|(_, schema)| schema.schema_id == column.schema_id)
        {
            txn.delete(
                &key_column(column.schema_id, column.ordinal)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
    }
    for (revision, schema) in schema_ids {
        txn.delete(
            &key_schema(schema.schema_id)?,
            ExpectedRevision::Exact(revision),
        )?;
    }

    let partition_ids = load_versioned_partitions(txn)?
        .into_iter()
        .filter(|(_, partition)| partition.table_id == table_id)
        .collect::<Vec<_>>();
    for (revision, tablet) in load_versioned_tablets(txn)? {
        if partition_ids
            .iter()
            .any(|(_, partition)| partition.partition_id == tablet.partition_id)
        {
            txn.delete(
                &key_tablet(tablet.tablet_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
    }
    for (revision, index) in load_versioned_indexes(txn)? {
        if index.table_id == table_id {
            txn.delete(
                &key_index(index.index_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
    }
    for (revision, partition) in partition_ids {
        txn.delete(
            &key_partition(partition.partition_id)?,
            ExpectedRevision::Exact(revision),
        )?;
    }

    delete_table_name_lookup_if_matches(txn, table.db_id, &table.name, table_id)?;
    txn.delete(
        &key_table(table_id)?,
        ExpectedRevision::Exact(table_revision),
    )?;
    Ok(())
}

fn delete_table_name_lookup_if_matches(
    txn: &mut dyn MetaWriteTxn,
    db_id: i64,
    table_name: &str,
    table_id: i64,
) -> RepositoryResult<()> {
    let lookup_key = key_table_name(db_id, table_name)?;
    let Some(record) = txn.get(&lookup_key)? else {
        return Ok(());
    };
    let lookup: IdLookup = decode_record_payload(&record, STARROCKS_TABLE_NAME_KIND)?;
    if lookup.id == table_id {
        txn.delete(&lookup_key, ExpectedRevision::Exact(record.revision))?;
    }
    Ok(())
}

fn put_partition(
    txn: &mut dyn MetaWriteTxn,
    partition: &StoredStarRocksPartition,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_partition(partition.partition_id)?,
        record_kind(STARROCKS_PARTITION_KIND)?,
        expected,
        encode_record_payload(STARROCKS_PARTITION_KIND, partition)?,
    ))?;
    Ok(())
}

fn put_table(
    txn: &mut dyn MetaWriteTxn,
    table: &StoredStarRocksTable,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_table(table.table_id)?,
        record_kind(STARROCKS_TABLE_KIND)?,
        expected,
        encode_record_payload(STARROCKS_TABLE_KIND, table)?,
    ))?;
    Ok(())
}

fn put_schema(
    txn: &mut dyn MetaWriteTxn,
    schema: &StoredStarRocksSchema,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_schema(schema.schema_id)?,
        record_kind(STARROCKS_SCHEMA_KIND)?,
        expected,
        encode_record_payload(
            STARROCKS_SCHEMA_KIND,
            &StoredStarRocksSchemaAvro::from(schema),
        )?,
    ))?;
    Ok(())
}

fn put_column(
    txn: &mut dyn MetaWriteTxn,
    column: &StoredStarRocksColumn,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_column(column.schema_id, column.ordinal)?,
        record_kind(STARROCKS_COLUMN_KIND)?,
        expected,
        encode_record_payload(STARROCKS_COLUMN_KIND, column)?,
    ))?;
    Ok(())
}

fn put_index(
    txn: &mut dyn MetaWriteTxn,
    index: &StoredStarRocksIndex,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_index(index.index_id)?,
        record_kind(STARROCKS_INDEX_KIND)?,
        expected,
        encode_record_payload(STARROCKS_INDEX_KIND, index)?,
    ))?;
    Ok(())
}

fn put_tablet(
    txn: &mut dyn MetaWriteTxn,
    tablet: &StoredStarRocksTablet,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_tablet(tablet.tablet_id)?,
        record_kind(STARROCKS_TABLET_KIND)?,
        expected,
        encode_record_payload(STARROCKS_TABLET_KIND, tablet)?,
    ))?;
    Ok(())
}

fn load_versioned_tables(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksTable)>> {
    scan_versioned_values(txn, "table", STARROCKS_TABLE_KIND)
}

fn load_versioned_schemas(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksSchema)>> {
    scan_versioned_values::<StoredStarRocksSchemaAvro>(txn, "schema", STARROCKS_SCHEMA_KIND).map(
        |schemas| {
            schemas
                .into_iter()
                .map(|(revision, schema)| (revision, schema.into()))
                .collect()
        },
    )
}

fn load_versioned_schema(
    txn: &dyn MetaReadTxn,
    schema_id: i64,
) -> RepositoryResult<Option<(MetaRevision, StoredStarRocksSchema)>> {
    txn.get(&key_schema(schema_id)?)?
        .map(|record| {
            let revision = record.revision.clone();
            let schema: StoredStarRocksSchema =
                decode_record_payload::<StoredStarRocksSchemaAvro>(&record, STARROCKS_SCHEMA_KIND)?
                    .into();
            Ok((revision, schema))
        })
        .transpose()
}

fn load_versioned_columns(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksColumn)>> {
    scan_versioned_values(txn, "column", STARROCKS_COLUMN_KIND)
}

fn load_versioned_partitions(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksPartition)>> {
    scan_versioned_values(txn, "partition", STARROCKS_PARTITION_KIND)
}

fn load_versioned_indexes(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksIndex)>> {
    scan_versioned_values(txn, "index", STARROCKS_INDEX_KIND)
}

fn load_versioned_indexes_for_partition(
    txn: &dyn MetaReadTxn,
    partition_id: i64,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksIndex)>> {
    load_versioned_indexes(txn).map(|indexes| {
        indexes
            .into_iter()
            .filter(|(_, index)| index.partition_id == partition_id)
            .collect()
    })
}

fn load_versioned_tablets(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksTablet)>> {
    scan_versioned_values(txn, "tablet", STARROCKS_TABLET_KIND)
}

fn load_versioned_tablets_for_partition(
    txn: &dyn MetaReadTxn,
    partition_id: i64,
) -> RepositoryResult<Vec<(MetaRevision, StoredStarRocksTablet)>> {
    load_versioned_tablets(txn).map(|tablets| {
        tablets
            .into_iter()
            .filter(|(_, tablet)| tablet.partition_id == partition_id)
            .collect()
    })
}

fn scan_values<T>(
    txn: &dyn MetaReadTxn,
    path: &str,
    expected_kind: &str,
) -> RepositoryResult<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let prefix = MetaKeyPrefix::new(NS_STARROCKS, [path.to_string()])?;
    txn.scan(&prefix, None)?
        .into_iter()
        .map(|record| decode_record_payload(&record, expected_kind))
        .collect()
}

fn scan_versioned_values<T>(
    txn: &dyn MetaReadTxn,
    path: &str,
    expected_kind: &str,
) -> RepositoryResult<Vec<(MetaRevision, T)>>
where
    T: for<'de> Deserialize<'de>,
{
    let prefix = MetaKeyPrefix::new(NS_STARROCKS, [path.to_string()])?;
    txn.scan(&prefix, None)?
        .into_iter()
        .map(|record| {
            let revision = record.revision.clone();
            decode_record_payload(&record, expected_kind).map(|value| (revision, value))
        })
        .collect()
}

fn decode_record_payload<T>(record: &MetaRecord, expected_kind: &str) -> RepositoryResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    if record.kind.as_str() != expected_kind {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {expected_kind}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    decode_payload_for_kind(expected_kind, &record.payload).map_err(|err| {
        RepositoryError::provider(format!(
            "failed to decode metadata record {} as {expected_kind}: {err}",
            record.key.canonical_path()
        ))
    })
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
}

fn key_database(db_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["database".to_string(), db_id.to_string()],
    )?)
}

fn key_database_name(name: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["database-name".to_string(), normalize_lookup_name(name)],
    )?)
}

fn key_table(table_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["table".to_string(), table_id.to_string()],
    )?)
}

fn key_table_name(db_id: i64, name: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        [
            "table-name".to_string(),
            db_id.to_string(),
            normalize_lookup_name(name),
        ],
    )?)
}

fn key_partition(partition_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["partition".to_string(), partition_id.to_string()],
    )?)
}

fn key_schema(schema_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["schema".to_string(), schema_id.to_string()],
    )?)
}

fn key_column(schema_id: i64, ordinal: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        [
            "column".to_string(),
            schema_id.to_string(),
            ordinal.to_string(),
        ],
    )?)
}

fn key_index(index_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["index".to_string(), index_id.to_string()],
    )?)
}

fn key_tablet(tablet_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_STARROCKS,
        ["tablet".to_string(), tablet_id.to_string()],
    )?)
}

fn tablet_root_path(warehouse_uri: &str, db_id: i64, table_id: i64, partition_id: i64) -> String {
    format!(
        "{}/db_{}/table_{}/partition_{}",
        warehouse_uri.trim_end_matches('/'),
        db_id,
        table_id,
        partition_id
    )
}
