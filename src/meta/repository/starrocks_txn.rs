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

use serde::{Deserialize, Serialize};

use crate::meta::keys::NS_STARROCKS_TXN;
use crate::meta::repository::starrocks_table::StarRocksTableMetaRepository;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};

const STARROCKS_TXN_KIND: &str = "starrocks.txn";

#[derive(Default)]
pub struct StarRocksTxnRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStarRocksTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
    pub state: StarRocksTxnState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StarRocksTxnState {
    Prepared,
    Written,
    Visible,
    Aborted,
}

impl StarRocksTxnState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Prepared => "PREPARED",
            Self::Written => "WRITTEN",
            Self::Visible => "VISIBLE",
            Self::Aborted => "ABORTED",
        }
    }
}

impl StarRocksTxnRepository {
    pub fn prepare(
        &self,
        meta_repo: &StarRocksTableMetaRepository,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        partition_id: i64,
    ) -> RepositoryResult<StoredStarRocksTxn> {
        let partition = meta_repo
            .load_partition(txn, partition_id)?
            .ok_or_else(|| {
                RepositoryError::not_found(format!("partition {partition_id} not found"))
            })?;
        if partition.table_id != table_id {
            return Err(RepositoryError::conflict(format!(
                "partition {partition_id} belongs to table {}, expected {table_id}",
                partition.table_id
            )));
        }

        let base_version = partition.visible_version;
        let commit_version = next_version(base_version, "commit")?;
        let stored = StoredStarRocksTxn {
            txn_id: txn.allocate_id(id_scopes::starrocks_txn())?,
            table_id,
            partition_id,
            base_version,
            commit_version,
            state: StarRocksTxnState::Prepared,
            retry_at_ms: None,
            updated_at_ms: 0,
        };
        put_txn(txn, &stored, ExpectedRevision::NotExists)?;
        Ok(stored)
    }

    pub fn load(
        &self,
        txn: &dyn MetaReadTxn,
        txn_id: i64,
    ) -> RepositoryResult<Option<StoredStarRocksTxn>> {
        Ok(load_versioned_txn(txn, txn_id)?.map(|versioned| versioned.value))
    }

    pub fn list_all(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<Vec<StoredStarRocksTxn>> {
        txn.scan(&key_prefix_txns()?, None)?
            .into_iter()
            .map(|record| decode_record_payload(&record, STARROCKS_TXN_KIND))
            .collect()
    }

    pub fn ensure_no_inflight_for_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<()> {
        if self.list_all(txn)?.into_iter().any(|stored| {
            stored.table_id == table_id
                && matches!(
                    stored.state,
                    StarRocksTxnState::Prepared | StarRocksTxnState::Written
                )
        }) {
            return Err(RepositoryError::conflict(format!(
                "cannot mutate StarRocks table {table_id}: inflight StarRocks txns exist"
            )));
        }
        Ok(())
    }

    pub fn delete_for_table(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
    ) -> RepositoryResult<()> {
        for stored in load_versioned_txns(txn)? {
            if stored.value.table_id == table_id {
                txn.delete(
                    &key_txn(stored.value.txn_id)?,
                    ExpectedRevision::Exact(stored.record_revision),
                )?;
            }
        }
        Ok(())
    }

    pub fn delete_for_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition_id: i64,
    ) -> RepositoryResult<()> {
        for stored in load_versioned_txns(txn)? {
            if stored.value.partition_id == partition_id {
                txn.delete(
                    &key_txn(stored.value.txn_id)?,
                    ExpectedRevision::Exact(stored.record_revision),
                )?;
            }
        }
        Ok(())
    }

    pub fn record_visible_bootstrap(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        partition_id: i64,
    ) -> RepositoryResult<StoredStarRocksTxn> {
        let stored = StoredStarRocksTxn {
            txn_id: txn.allocate_id(id_scopes::starrocks_txn())?,
            table_id,
            partition_id,
            base_version: 0,
            commit_version: 1,
            state: StarRocksTxnState::Visible,
            retry_at_ms: None,
            updated_at_ms: 0,
        };
        put_txn(txn, &stored, ExpectedRevision::NotExists)?;
        Ok(stored)
    }

    pub fn mark_written(&self, txn: &mut dyn MetaWriteTxn, txn_id: i64) -> RepositoryResult<()> {
        let mut stored = load_required_txn(txn, txn_id)?;
        let state = stored.value.state.clone();
        match state {
            StarRocksTxnState::Prepared => {
                stored.value.state = StarRocksTxnState::Written;
                put_txn(
                    txn,
                    &stored.value,
                    ExpectedRevision::Exact(stored.record_revision),
                )
            }
            StarRocksTxnState::Written | StarRocksTxnState::Visible => Ok(()),
            StarRocksTxnState::Aborted => Err(RepositoryError::conflict(format!(
                "StarRocks txn {txn_id} is {}, expected {}",
                state.as_str(),
                StarRocksTxnState::Prepared.as_str()
            ))),
        }
    }

    pub fn mark_visible(
        &self,
        meta_repo: &StarRocksTableMetaRepository,
        txn: &mut dyn MetaWriteTxn,
        txn_id: i64,
    ) -> RepositoryResult<()> {
        let mut stored = load_required_txn(txn, txn_id)?;
        let state = stored.value.state.clone();
        match state {
            StarRocksTxnState::Written => {
                validate_txn_versions(&stored.value)?;
                let (partition_revision, mut partition) =
                    load_checked_partition(meta_repo, txn, &stored.value)?;
                if partition.visible_version != stored.value.base_version {
                    return Err(RepositoryError::conflict(format!(
                        "partition {} visible version is {}, expected {}",
                        stored.value.partition_id,
                        partition.visible_version,
                        stored.value.base_version
                    )));
                }
                if partition.next_version != stored.value.commit_version {
                    return Err(RepositoryError::conflict(format!(
                        "partition {} next version is {}, expected {}",
                        stored.value.partition_id,
                        partition.next_version,
                        stored.value.commit_version
                    )));
                }

                partition.visible_version = stored.value.commit_version;
                partition.next_version = next_version(stored.value.commit_version, "next")?;
                meta_repo.update_partition_exact(txn, &partition, partition_revision)?;

                stored.value.state = StarRocksTxnState::Visible;
                put_txn(
                    txn,
                    &stored.value,
                    ExpectedRevision::Exact(stored.record_revision),
                )
            }
            StarRocksTxnState::Visible => {
                validate_txn_versions(&stored.value)?;
                let (_, partition) = load_checked_partition(meta_repo, txn, &stored.value)?;
                if partition.visible_version != stored.value.commit_version {
                    return Err(RepositoryError::conflict(format!(
                        "partition {} visible version is {}, expected {}",
                        stored.value.partition_id,
                        partition.visible_version,
                        stored.value.commit_version
                    )));
                }
                let expected_next_version = next_version(stored.value.commit_version, "next")?;
                if partition.next_version != expected_next_version {
                    return Err(RepositoryError::conflict(format!(
                        "partition {} next version is {}, expected {}",
                        stored.value.partition_id, partition.next_version, expected_next_version
                    )));
                }
                Ok(())
            }
            StarRocksTxnState::Prepared | StarRocksTxnState::Aborted => {
                Err(RepositoryError::conflict(format!(
                    "StarRocks txn {txn_id} is {}, expected {}",
                    state.as_str(),
                    StarRocksTxnState::Written.as_str()
                )))
            }
        }
    }

    pub fn mark_aborted(&self, txn: &mut dyn MetaWriteTxn, txn_id: i64) -> RepositoryResult<()> {
        let mut stored = load_required_txn(txn, txn_id)?;
        match stored.value.state {
            StarRocksTxnState::Prepared | StarRocksTxnState::Written => {
                stored.value.state = StarRocksTxnState::Aborted;
                put_txn(
                    txn,
                    &stored.value,
                    ExpectedRevision::Exact(stored.record_revision),
                )
            }
            StarRocksTxnState::Aborted => Ok(()),
            StarRocksTxnState::Visible => Err(RepositoryError::conflict(format!(
                "StarRocks txn {txn_id} is {}, cannot abort",
                StarRocksTxnState::Visible.as_str()
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedStarRocksTxn {
    record_revision: MetaRevision,
    value: StoredStarRocksTxn,
}

fn validate_txn_versions(stored: &StoredStarRocksTxn) -> RepositoryResult<()> {
    let expected_commit_version = next_version(stored.base_version, "commit")?;
    if stored.commit_version != expected_commit_version {
        return Err(RepositoryError::provider(format!(
            "StarRocks txn {} commit version is {}, expected {}",
            stored.txn_id, stored.commit_version, expected_commit_version
        )));
    }
    Ok(())
}

fn next_version(version: i64, label: &str) -> RepositoryResult<i64> {
    version
        .checked_add(1)
        .ok_or_else(|| RepositoryError::provider(format!("StarRocks txn {label} version overflow")))
}

fn load_checked_partition(
    meta_repo: &StarRocksTableMetaRepository,
    txn: &dyn MetaReadTxn,
    stored: &StoredStarRocksTxn,
) -> RepositoryResult<(
    MetaRevision,
    crate::meta::repository::starrocks_table::StoredStarRocksPartition,
)> {
    let (revision, partition) = meta_repo
        .load_versioned_partition(txn, stored.partition_id)?
        .ok_or_else(|| {
            RepositoryError::not_found(format!("partition {} not found", stored.partition_id))
        })?;
    if partition.table_id != stored.table_id {
        return Err(RepositoryError::conflict(format!(
            "partition {} belongs to table {}, expected {}",
            stored.partition_id, partition.table_id, stored.table_id
        )));
    }
    Ok((revision, partition))
}

fn load_required_txn(
    txn: &dyn MetaReadTxn,
    txn_id: i64,
) -> RepositoryResult<VersionedStarRocksTxn> {
    load_versioned_txn(txn, txn_id)?
        .ok_or_else(|| RepositoryError::not_found(format!("StarRocks txn {txn_id} not found")))
}

fn load_versioned_txn(
    txn: &dyn MetaReadTxn,
    txn_id: i64,
) -> RepositoryResult<Option<VersionedStarRocksTxn>> {
    txn.get(&key_txn(txn_id)?)?
        .map(|record| {
            let value = decode_record_payload(&record, STARROCKS_TXN_KIND)?;
            Ok(VersionedStarRocksTxn {
                record_revision: record.revision,
                value,
            })
        })
        .transpose()
}

fn load_versioned_txns(txn: &dyn MetaReadTxn) -> RepositoryResult<Vec<VersionedStarRocksTxn>> {
    txn.scan(&key_prefix_txns()?, None)?
        .into_iter()
        .map(|record| {
            let value = decode_record_payload(&record, STARROCKS_TXN_KIND)?;
            Ok(VersionedStarRocksTxn {
                record_revision: record.revision,
                value,
            })
        })
        .collect()
}

fn put_txn(
    txn: &mut dyn MetaWriteTxn,
    stored: &StoredStarRocksTxn,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_txn(stored.txn_id)?,
        record_kind(STARROCKS_TXN_KIND)?,
        expected,
        encode_record_payload(STARROCKS_TXN_KIND, stored)?,
    ))?;
    Ok(())
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

fn key_txn(txn_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_STARROCKS_TXN, [txn_id.to_string()])?)
}

fn key_prefix_txns() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_STARROCKS_TXN, Vec::<String>::new())?)
}
