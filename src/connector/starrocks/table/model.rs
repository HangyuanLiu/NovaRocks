#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StarRocksTableSnapshot {
    pub global: StarRocksGlobalMeta,
    pub databases: Vec<StoredStarRocksDatabase>,
    pub tables: Vec<StoredStarRocksTable>,
    pub schemas: Vec<StoredStarRocksSchema>,
    pub columns: Vec<StoredStarRocksColumn>,
    pub partitions: Vec<StoredStarRocksPartition>,
    pub indexes: Vec<StoredStarRocksIndex>,
    pub tablets: Vec<StoredStarRocksTablet>,
    #[cfg(test)]
    pub txns: Vec<StoredStarRocksTxn>,
    #[cfg(test)]
    pub erase_jobs: Vec<StoredStarRocksEraseJob>,
    #[cfg(test)]
    pub materialized_views: Vec<StoredMaterializedView>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StarRocksGlobalMeta {
    pub warehouse_uri: String,
    pub next_db_id: i64,
    pub next_table_id: i64,
    pub next_partition_id: i64,
    pub next_index_id: i64,
    pub next_tablet_id: i64,
    pub next_txn_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksDatabase {
    pub db_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksTable {
    pub table_id: i64,
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
    pub state: StarRocksTableState,
    pub kind: StarRocksTableKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StarRocksTableKind {
    #[default]
    Table,
    MaterializedView,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StarRocksMvRefreshMode {
    #[default]
    DeferredManual,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct IcebergTableRef {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl IcebergTableRef {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredMaterializedView {
    pub mv_id: i64,
    pub select_sql: String,
    pub refresh_mode: StarRocksMvRefreshMode,
    pub base_table_refs: Vec<IcebergTableRef>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: std::collections::BTreeMap<String, i64>,
    pub last_refresh_table_uuids: std::collections::BTreeMap<String, String>,
    pub primary_key_columns: Vec<String>,
    pub created_at_ms: i64,
    pub storage_engine: StarRocksMvStorageEngine,
    pub iceberg_table_identifier: Option<String>,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    pub refresh_target_snapshots: std::collections::BTreeMap<String, i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StarRocksMvStorageEngine {
    StarRocks,
    Iceberg,
}

impl StarRocksMvStorageEngine {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::StarRocks => "starrocks",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn parse_sql_str(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "starrocks" => Ok(Self::StarRocks),
            "iceberg" => Ok(Self::Iceberg),
            _ => Err(format!(
                "unknown materialized view storage_engine `{value}`"
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksSchema {
    pub schema_id: i64,
    pub table_id: i64,
    pub schema_version: i64,
    pub tablet_schema_pb: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksColumn {
    pub schema_id: i64,
    pub ordinal: i64,
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksPartition {
    pub partition_id: i64,
    pub table_id: i64,
    pub name: String,
    pub visible_version: i64,
    pub next_version: i64,
    pub state: StarRocksPartitionState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksIndex {
    pub index_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub index_type: String,
    pub state: StarRocksIndexState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksTablet {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub index_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
    pub state: StarRocksTxnState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredStarRocksEraseJob {
    pub job_id: i64,
    pub job_kind: StarRocksEraseJobKind,
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub state: StarRocksEraseJobState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StarRocksTableState {
    Creating,
    #[default]
    Active,
    Dropping,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StarRocksPartitionState {
    Creating,
    #[default]
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StarRocksIndexState {
    Creating,
    #[default]
    Active,
    Retired,
    Failed,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StarRocksEraseJobKind {
    DropTable,
    DropPartition,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StarRocksEraseJobState {
    Pending,
    Running,
    Failed,
    Finished,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StarRocksTxnState {
    #[default]
    Prepared,
    Written,
    Visible,
    Aborted,
}
