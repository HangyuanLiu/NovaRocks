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

use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaWriteTxn,
};

const ATTACHMENT_NAMESPACE: &str = "iceberg_catalog";
const ATTACHMENT_KIND: &str = "iceberg.catalog";

#[derive(Default)]
pub(crate) struct CatalogAttachmentRepository;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogAttachment {
    pub(crate) catalog: String,
    pub(crate) properties: CatalogAttachmentProperties,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogAttachmentProperties {
    pub(crate) properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CatalogAttachmentPropertiesAvro {
    properties: Vec<StringPairAvro>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StringPairAvro {
    key: String,
    value: String,
}

impl From<&CatalogAttachmentProperties> for CatalogAttachmentPropertiesAvro {
    fn from(value: &CatalogAttachmentProperties) -> Self {
        Self {
            properties: value
                .properties
                .iter()
                .map(|(key, value)| StringPairAvro {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

impl From<CatalogAttachmentPropertiesAvro> for CatalogAttachmentProperties {
    fn from(value: CatalogAttachmentPropertiesAvro) -> Self {
        Self {
            properties: value
                .properties
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect(),
        }
    }
}

impl CatalogAttachmentRepository {
    pub(crate) fn upsert(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        properties: CatalogAttachmentProperties,
    ) -> RepositoryResult<()> {
        txn.put(MetaRecordPut::new(
            attachment_key(catalog)?,
            MetaRecordKind::new(ATTACHMENT_KIND)?,
            ExpectedRevision::Any,
            encode_record_payload(
                ATTACHMENT_KIND,
                &CatalogAttachmentPropertiesAvro::from(&properties),
            )?,
        ))?;
        Ok(())
    }

    pub(crate) fn exists(&self, txn: &dyn MetaReadTxn, catalog: &str) -> RepositoryResult<bool> {
        let Some(record) = txn.get(&attachment_key(catalog)?)? else {
            return Ok(false);
        };
        decode_attachment_properties(&record)?;
        Ok(true)
    }

    pub(crate) fn list(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<Vec<CatalogAttachment>> {
        txn.scan(&attachment_prefix()?, None)?
            .into_iter()
            .map(|record| {
                let catalog = record_path_component(&record, 1, "catalog attachment")?;
                let properties = decode_attachment_properties(&record)?;
                Ok(CatalogAttachment {
                    catalog,
                    properties,
                })
            })
            .collect()
    }

    pub(crate) fn delete(&self, txn: &mut dyn MetaWriteTxn, catalog: &str) -> RepositoryResult<()> {
        txn.delete(&attachment_key(catalog)?, ExpectedRevision::Any)?;
        Ok(())
    }
}

fn decode_attachment_properties(
    record: &MetaRecord,
) -> RepositoryResult<CatalogAttachmentProperties> {
    if record.kind.as_str() != ATTACHMENT_KIND {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {ATTACHMENT_KIND}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    let value: CatalogAttachmentPropertiesAvro =
        decode_payload_for_kind(ATTACHMENT_KIND, &record.payload).map_err(|err| {
            RepositoryError::provider(format!(
                "failed to decode metadata record {} as {ATTACHMENT_KIND}: {err}",
                record.key.canonical_path()
            ))
        })?;
    Ok(value.into())
}

fn record_path_component(
    record: &MetaRecord,
    index: usize,
    description: &str,
) -> RepositoryResult<String> {
    record
        .key
        .canonical_path()
        .split('/')
        .nth(index)
        .map(str::to_string)
        .ok_or_else(|| {
            RepositoryError::provider(format!(
                "metadata record {} is not a valid {description} key",
                record.key.canonical_path()
            ))
        })
}

fn attachment_key(catalog: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        ATTACHMENT_NAMESPACE,
        ["catalog".to_string(), catalog.to_ascii_lowercase()],
    )?)
}

fn attachment_prefix() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(ATTACHMENT_NAMESPACE, ["catalog"])?)
}

#[cfg(test)]
mod tests {
    use crate::meta::{
        ExpectedRevision, MetaRecordKind, MetaRecordPut, MetaStoreProvider, SqliteMetaStoreProvider,
    };

    use super::*;

    #[test]
    fn attachment_round_trips_normalizes_and_deletes() {
        let dir = tempfile::tempdir().expect("metadata dir");
        let provider =
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("open provider");
        let repository = CatalogAttachmentRepository;
        let mut write = provider
            .begin_write("upsert attachment")
            .expect("write txn");
        repository
            .upsert(
                write.as_mut(),
                "ICE",
                CatalogAttachmentProperties {
                    properties: vec![("type".into(), "rest".into())],
                },
            )
            .expect("upsert attachment");
        repository
            .upsert(
                write.as_mut(),
                "WAREHOUSE",
                CatalogAttachmentProperties {
                    properties: vec![("type".into(), "hadoop".into())],
                },
            )
            .expect("upsert second attachment");
        write.commit().expect("commit attachment");

        let read = provider.begin_read().expect("read txn");
        assert!(repository.exists(read.as_ref(), "ice").expect("exists"));
        assert_eq!(
            repository.list(read.as_ref()).expect("list"),
            vec![
                CatalogAttachment {
                    catalog: "ice".into(),
                    properties: CatalogAttachmentProperties {
                        properties: vec![("type".into(), "rest".into())],
                    },
                },
                CatalogAttachment {
                    catalog: "warehouse".into(),
                    properties: CatalogAttachmentProperties {
                        properties: vec![("type".into(), "hadoop".into())],
                    },
                },
            ]
        );
        drop(read);

        let mut write = provider
            .begin_write("delete attachment")
            .expect("write txn");
        repository
            .delete(write.as_mut(), "IcE")
            .expect("delete attachment");
        write.commit().expect("commit delete");
        let read = provider.begin_read().expect("read txn");
        assert!(!repository.exists(read.as_ref(), "ice").expect("exists"));
        assert!(
            repository
                .exists(read.as_ref(), "WAREHOUSE")
                .expect("second exists")
        );
    }

    #[test]
    fn attachment_exists_rejects_a_wrong_record_kind_before_decode() {
        let dir = tempfile::tempdir().expect("metadata dir");
        let provider =
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("open provider");
        let repository = CatalogAttachmentRepository;
        let mut write = provider.begin_write("seed attachment").expect("write txn");
        repository
            .upsert(
                write.as_mut(),
                "ice",
                CatalogAttachmentProperties {
                    properties: vec![("type".into(), "rest".into())],
                },
            )
            .expect("upsert attachment");
        write.commit().expect("commit attachment");

        let read = provider.begin_read().expect("read txn");
        let payload = read
            .get(&attachment_key("ice").expect("attachment key"))
            .expect("get attachment")
            .expect("attachment record")
            .payload;
        drop(read);

        let mut write = provider
            .begin_write("replace attachment kind")
            .expect("write txn");
        write
            .put(MetaRecordPut::new(
                attachment_key("ice").expect("attachment key"),
                MetaRecordKind::new("job.erase").expect("record kind"),
                ExpectedRevision::Any,
                payload,
            ))
            .expect("replace record kind");
        write.commit().expect("commit wrong kind");

        let read = provider.begin_read().expect("read txn");
        assert_eq!(
            repository
                .exists(read.as_ref(), "ice")
                .expect_err("wrong kind must fail")
                .to_string(),
            "metadata repository provider error: metadata record catalog/ice has kind job.erase, expected iceberg.catalog"
        );
    }
}
