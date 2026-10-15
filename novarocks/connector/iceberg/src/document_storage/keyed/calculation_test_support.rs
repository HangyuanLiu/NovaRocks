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

use super::{
    envelope::encode_version,
    keys::{record_key, version_key},
    record::encode_record,
    view::project_full_view,
};
use crate::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadata,
    TableMetadataBuilder, Type,
};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::HashMap;

pub(super) fn document(
    name: &str,
    references: Vec<(String, DocumentIdentity)>,
) -> ConnectorDocumentVersion {
    ConnectorDocumentVersion::try_new(
        "novarocks.test",
        name,
        ConnectorDocumentFormat::try_new("novarocks.test", "document", 1).unwrap(),
        references,
        Bytes::from_static(b"stable-fact"),
    )
    .unwrap()
}
pub(super) fn properties(versions: &[ConnectorDocumentVersion]) -> HashMap<String, String> {
    versions
        .iter()
        .map(|v| (version_key(v.identity()), encode_version(v).unwrap()))
        .collect()
}
pub(super) fn record(
    payload: &'static [u8],
    binding: Option<DocumentIdentity>,
) -> ConnectorSnapshotRecord {
    ConnectorSnapshotRecord {
        owner: "novarocks.test".into(),
        name: "publication".into(),
        binding,
        payload: Bytes::from_static(payload),
    }
}
pub(super) struct Snapshot {
    pub id: i64,
    pub parent: Option<i64>,
    pub records: Vec<ConnectorSnapshotRecord>,
}
impl Snapshot {
    pub fn new(id: i64, parent: Option<i64>) -> Self {
        Self {
            id,
            parent,
            records: vec![],
        }
    }
    pub fn record(mut self, record: ConnectorSnapshotRecord) -> Self {
        self.records.push(record);
        self
    }
}

/// Independent physical fixtures, including deliberately expired parent links.
/// No production outcome or retention helper constructs the expected answer.
pub(super) fn full(
    current: Option<i64>,
    snapshots: Vec<Snapshot>,
    properties: HashMap<String, String>,
    extra_refs: Vec<(&str, i64)>,
) -> FullKeyedDocumentView {
    let schema = Schema::builder()
        .with_fields(vec![std::sync::Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let metadata = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://warehouse/table".into(),
        FormatVersion::V2,
        properties,
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let mut json = serde_json::to_value(metadata).unwrap();
    json["table-uuid"] = serde_json::json!("00000000-0000-0000-0000-000000000001");
    let length = snapshots.len();
    let serialized:Vec<_>=snapshots.into_iter().enumerate().map(|(i,s)| {
        let mut summary=serde_json::json!({"operation":"append"});
        for record in s.records {summary[record_key(&record.owner,&record.name)]=serde_json::json!(encode_record(&record).unwrap());}
        let mut snapshot=serde_json::json!({"snapshot-id":s.id,"sequence-number":(i+1) as i64,"timestamp-ms":(i+1) as i64,"manifest-list":"memory://manifest-list","summary":summary});
        if let Some(parent)=s.parent {snapshot["parent-snapshot-id"]=serde_json::json!(parent);}
        snapshot
    }).collect();
    json["snapshots"] = serde_json::json!(serialized);
    json["last-sequence-number"] = serde_json::json!(length as i64);
    json["refs"] = serde_json::json!({});
    if let Some(current) = current {
        json["current-snapshot-id"] = serde_json::json!(current);
        json["refs"]["main"] = serde_json::json!({"snapshot-id":current,"type":"branch"});
    } else {
        json.as_object_mut().unwrap().remove("current-snapshot-id");
    }
    for (name, id) in extra_refs {
        json["refs"][name] = serde_json::json!({"snapshot-id":id,"type":"tag"});
    }
    let metadata: TableMetadata = serde_json::from_value(json).unwrap();
    let binding = ConnectorProviderBinding::iceberg("test", [1; 16], "test").unwrap();
    let request = KeyedDocumentObservationRequest {
        owner: binding.binding_key().clone(),
        catalog_handle: CatalogHandle::new(
            binding.binding_key().instance_id.clone(),
            CatalogVersion::from_bytes([2; 32]),
        ),
        target: ConnectorTableIdentity {
            instance_id: binding.binding_key().instance_id.clone(),
            namespace: "ns".into(),
            table: "table".into(),
        },
        object_id: ConnectorTableObjectId::try_new(Bytes::from(metadata.uuid().to_string()))
            .unwrap(),
        context: ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            ConnectorStopOwner::default().view(),
            1024,
            4096,
        )
        .unwrap(),
    };
    let version = ConnectorCommittedVersion::try_new(
        Bytes::from_static(b"metadata-version"),
        metadata.current_snapshot_id(),
    )
    .unwrap();
    project_full_view(&metadata, &request, version).unwrap()
}
