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

use crate::meta::keys::{NS_DICTIONARY, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaWriteTxn,
};

pub const DICTIONARY_SNAPSHOT_KIND: &str = "dictionary.snapshot";
pub const DICTIONARY_LOOKUP_KIND: &str = "dictionary.lookup";

pub const DICTIONARY_STATE_ACTIVE: &str = "ACTIVE";
pub const DICTIONARY_STATE_STALE: &str = "STALE";
pub const DICTIONARY_STATE_DROPPED: &str = "DROPPED";

#[derive(Default)]
pub struct DictionaryMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDictionaryValue {
    pub id: i32,
    #[serde(with = "avro_bytes_vec")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDictionarySnapshot {
    pub dictionary_id: i64,
    pub owner_kind: String,
    pub owner_key: String,
    pub column_id: Option<i64>,
    pub column_name: String,
    pub data_type: String,
    pub version: i64,
    pub watermark: String,
    pub values: Vec<StoredDictionaryValue>,
    pub null_id: i32,
    pub state: String,
    pub order_preserving: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDictionaryLookup {
    pub owner_kind: String,
    pub owner_key: String,
    pub column_name: String,
    pub dictionary_id: i64,
}

impl DictionaryMetaRepository {
    pub fn load_active(
        &self,
        txn: &dyn MetaReadTxn,
        owner_kind: &str,
        owner_key: &str,
        column_name: &str,
    ) -> RepositoryResult<Option<StoredDictionarySnapshot>> {
        let lookup_key = dictionary_lookup_key(owner_kind, owner_key, column_name)?;
        let Some(lookup_record) = txn.get(&lookup_key)? else {
            return Ok(None);
        };
        let lookup: StoredDictionaryLookup =
            decode_record_payload(&lookup_record, DICTIONARY_LOOKUP_KIND)?;
        let snapshot_key = dictionary_snapshot_key(lookup.dictionary_id)?;
        let Some(snapshot_record) = txn.get(&snapshot_key)? else {
            return Err(RepositoryError::provider(format!(
                "dictionary lookup points to missing snapshot id {}",
                lookup.dictionary_id
            )));
        };
        let snapshot: StoredDictionarySnapshot =
            decode_record_payload(&snapshot_record, DICTIONARY_SNAPSHOT_KIND)?;
        if snapshot.state == DICTIONARY_STATE_ACTIVE {
            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_snapshot(
        &self,
        txn: &mut dyn MetaWriteTxn,
        snapshot: &StoredDictionarySnapshot,
    ) -> RepositoryResult<()> {
        let snapshot_key = dictionary_snapshot_key(snapshot.dictionary_id)?;
        let snapshot_kind = record_kind(DICTIONARY_SNAPSHOT_KIND)?;
        let snapshot_payload = encode_record_payload(DICTIONARY_SNAPSHOT_KIND, snapshot)?;
        txn.put(MetaRecordPut::new(
            snapshot_key,
            snapshot_kind,
            ExpectedRevision::Any,
            snapshot_payload,
        ))?;

        let lookup = StoredDictionaryLookup {
            owner_kind: snapshot.owner_kind.clone(),
            owner_key: snapshot.owner_key.clone(),
            column_name: snapshot.column_name.clone(),
            dictionary_id: snapshot.dictionary_id,
        };
        let lookup_key = dictionary_lookup_key(
            &snapshot.owner_kind,
            &snapshot.owner_key,
            &snapshot.column_name,
        )?;
        let lookup_kind = record_kind(DICTIONARY_LOOKUP_KIND)?;
        let lookup_payload = encode_record_payload(DICTIONARY_LOOKUP_KIND, &lookup)?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            lookup_kind,
            ExpectedRevision::Any,
            lookup_payload,
        ))?;
        Ok(())
    }

    pub fn mark_owner_stale(
        &self,
        txn: &mut dyn MetaWriteTxn,
        owner_kind: &str,
        owner_key: &str,
    ) -> RepositoryResult<()> {
        let prefix = dictionary_lookup_prefix(owner_kind, owner_key)?;
        let records = txn.scan(&prefix, None)?;
        for record in records {
            let lookup: StoredDictionaryLookup =
                decode_record_payload(&record, DICTIONARY_LOOKUP_KIND)?;
            self.mark_snapshot_state(txn, lookup.dictionary_id, DICTIONARY_STATE_STALE)?;
        }
        Ok(())
    }

    pub fn drop_owner(
        &self,
        txn: &mut dyn MetaWriteTxn,
        owner_kind: &str,
        owner_key: &str,
    ) -> RepositoryResult<()> {
        let prefix = dictionary_lookup_prefix(owner_kind, owner_key)?;
        let records = txn.scan(&prefix, None)?;
        for record in records {
            let lookup: StoredDictionaryLookup =
                decode_record_payload(&record, DICTIONARY_LOOKUP_KIND)?;
            self.mark_snapshot_state(txn, lookup.dictionary_id, DICTIONARY_STATE_DROPPED)?;
            txn.delete(&record.key, ExpectedRevision::Any)?;
        }
        Ok(())
    }

    fn mark_snapshot_state(
        &self,
        txn: &mut dyn MetaWriteTxn,
        dictionary_id: i64,
        state: &str,
    ) -> RepositoryResult<()> {
        let snapshot_key = dictionary_snapshot_key(dictionary_id)?;
        let Some(record) = txn.get(&snapshot_key)? else {
            return Ok(());
        };
        let mut snapshot: StoredDictionarySnapshot =
            decode_record_payload(&record, DICTIONARY_SNAPSHOT_KIND)?;
        if snapshot.state == state {
            return Ok(());
        }
        snapshot.state = state.to_string();
        let kind = record_kind(DICTIONARY_SNAPSHOT_KIND)?;
        let payload = encode_record_payload(DICTIONARY_SNAPSHOT_KIND, &snapshot)?;
        txn.put(MetaRecordPut::new(
            snapshot_key,
            kind,
            ExpectedRevision::Exact(record.revision),
            payload,
        ))?;
        Ok(())
    }
}

fn dictionary_snapshot_key(dictionary_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_DICTIONARY,
        ["snapshot".to_string(), dictionary_id.to_string()],
    )?)
}

fn dictionary_lookup_key(
    owner_kind: &str,
    owner_key: &str,
    column_name: &str,
) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_DICTIONARY,
        [
            "lookup".to_string(),
            normalize_lookup_name(owner_kind),
            normalize_lookup_name(owner_key),
            normalize_lookup_name(column_name),
        ],
    )?)
}

fn dictionary_lookup_prefix(owner_kind: &str, owner_key: &str) -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(
        NS_DICTIONARY,
        [
            "lookup".to_string(),
            normalize_lookup_name(owner_kind),
            normalize_lookup_name(owner_key),
        ],
    )?)
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
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
            while let Some(byte) = seq.next_element::<u8>()? {
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::SqliteMetaStoreProvider;
    use crate::meta::provider::MetaStoreProvider;

    fn open_provider() -> (tempfile::TempDir, SqliteMetaStoreProvider) {
        let dir = tempfile::tempdir().expect("create tempdir for dictionary tests");
        let provider = SqliteMetaStoreProvider::open(dir.path().join("dictionary.sqlite"))
            .expect("open sqlite metadata provider for dictionary tests");
        (dir, provider)
    }

    fn sample_snapshot(dictionary_id: i64, state: &str) -> StoredDictionarySnapshot {
        StoredDictionarySnapshot {
            dictionary_id,
            owner_kind: "starrocks_table".to_string(),
            owner_key: "db=demo;table=t1".to_string(),
            column_id: Some(101),
            column_name: "s".to_string(),
            data_type: "STRING".to_string(),
            version: 1,
            watermark: "{\"kind\":\"starrocks\"}".to_string(),
            values: vec![
                StoredDictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                },
                StoredDictionaryValue {
                    id: 2,
                    bytes: b"b".to_vec(),
                },
            ],
            null_id: 0,
            state: state.to_string(),
            order_preserving: true,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn upsert_and_load_active_snapshot() {
        let (_dir, provider) = open_provider();
        let repo = DictionaryMetaRepository;

        let snapshot = sample_snapshot(7, DICTIONARY_STATE_ACTIVE);
        let mut txn = provider.begin_write("upsert dictionary").unwrap();
        repo.upsert_snapshot(txn.as_mut(), &snapshot).unwrap();
        txn.commit().unwrap();

        let read = provider.begin_read().unwrap();
        let loaded = repo
            .load_active(
                read.as_ref(),
                &snapshot.owner_kind,
                &snapshot.owner_key,
                &snapshot.column_name,
            )
            .unwrap();
        assert_eq!(loaded.as_ref(), Some(&snapshot));
    }

    #[test]
    fn mark_owner_stale_blocks_load_active() {
        let (_dir, provider) = open_provider();
        let repo = DictionaryMetaRepository;

        let snapshot = sample_snapshot(9, DICTIONARY_STATE_ACTIVE);
        let mut txn = provider.begin_write("upsert dictionary").unwrap();
        repo.upsert_snapshot(txn.as_mut(), &snapshot).unwrap();
        txn.commit().unwrap();

        let mut txn = provider.begin_write("mark stale").unwrap();
        repo.mark_owner_stale(txn.as_mut(), &snapshot.owner_kind, &snapshot.owner_key)
            .unwrap();
        txn.commit().unwrap();

        let read = provider.begin_read().unwrap();
        let loaded = repo
            .load_active(
                read.as_ref(),
                &snapshot.owner_kind,
                &snapshot.owner_key,
                &snapshot.column_name,
            )
            .unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn drop_owner_removes_lookup_and_marks_snapshot_dropped() {
        let (_dir, provider) = open_provider();
        let repo = DictionaryMetaRepository;

        let snapshot = sample_snapshot(11, DICTIONARY_STATE_ACTIVE);
        let mut txn = provider.begin_write("upsert dictionary").unwrap();
        repo.upsert_snapshot(txn.as_mut(), &snapshot).unwrap();
        txn.commit().unwrap();

        let mut txn = provider.begin_write("drop dictionary").unwrap();
        repo.drop_owner(txn.as_mut(), &snapshot.owner_kind, &snapshot.owner_key)
            .unwrap();
        txn.commit().unwrap();

        let read = provider.begin_read().unwrap();
        let loaded = repo
            .load_active(
                read.as_ref(),
                &snapshot.owner_kind,
                &snapshot.owner_key,
                &snapshot.column_name,
            )
            .unwrap();
        assert!(loaded.is_none());

        let snapshot_key = dictionary_snapshot_key(snapshot.dictionary_id).unwrap();
        let record = read.get(&snapshot_key).unwrap().expect("snapshot retained");
        let stored: StoredDictionarySnapshot =
            decode_record_payload(&record, DICTIONARY_SNAPSHOT_KIND).unwrap();
        assert_eq!(stored.state, DICTIONARY_STATE_DROPPED);
    }
}
