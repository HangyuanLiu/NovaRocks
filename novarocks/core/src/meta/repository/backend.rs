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

use crate::meta::keys::NS_CLUSTER;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaWriteTxn,
};

pub const CLUSTER_BACKEND_KIND: &str = "cluster.backend";

#[derive(Default)]
pub struct BackendMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredBackend {
    pub be_id: i64,
    pub endpoint: String,
    pub state: String,
}

impl BackendMetaRepository {
    pub fn upsert_backend(
        &self,
        txn: &mut dyn MetaWriteTxn,
        backend: &StoredBackend,
    ) -> RepositoryResult<()> {
        txn.put(MetaRecordPut::new(
            backend_key(&backend.endpoint)?,
            record_kind(CLUSTER_BACKEND_KIND)?,
            ExpectedRevision::Any,
            encode_record_payload(CLUSTER_BACKEND_KIND, backend)?,
        ))?;
        Ok(())
    }

    pub fn list_backends(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<Vec<StoredBackend>> {
        let mut backends: Vec<StoredBackend> = txn
            .scan(&backend_prefix()?, None)?
            .into_iter()
            .map(|record| decode_record_payload(&record, CLUSTER_BACKEND_KIND))
            .collect::<RepositoryResult<Vec<_>>>()?;
        backends.sort_by_key(|backend| backend.be_id);
        Ok(backends)
    }

    pub fn delete_backend(
        &self,
        txn: &mut dyn MetaWriteTxn,
        endpoint: &str,
    ) -> RepositoryResult<()> {
        txn.delete(&backend_key(endpoint)?, ExpectedRevision::Any)?;
        Ok(())
    }
}

fn backend_key(endpoint: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_CLUSTER,
        ["backend".to_string(), endpoint.to_string()],
    )?)
}

fn backend_prefix() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_CLUSTER, ["backend".to_string()])?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::MetaStoreProvider;

    #[test]
    fn repository_round_trips_backends() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let provider = crate::meta::SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))
            .expect("open provider");
        let repo = BackendMetaRepository;

        {
            let mut txn = provider.begin_write("store backend").expect("write");
            repo.upsert_backend(
                txn.as_mut(),
                &StoredBackend {
                    be_id: 7,
                    endpoint: "127.0.0.1:19070".to_string(),
                    state: "Live".to_string(),
                },
            )
            .expect("upsert");
            txn.commit().expect("commit");
        }

        let txn = provider.begin_read().expect("read");
        let backends = repo.list_backends(txn.as_ref()).expect("list");
        assert_eq!(
            backends,
            vec![StoredBackend {
                be_id: 7,
                endpoint: "127.0.0.1:19070".to_string(),
                state: "Live".to_string(),
            }]
        );
    }
}
