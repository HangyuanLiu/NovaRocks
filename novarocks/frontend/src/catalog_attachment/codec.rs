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

use std::io::Cursor;
use std::sync::LazyLock;

use apache_avro::{Schema, from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"NRCA";
const ENVELOPE_VERSION: u8 = 1;

static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::parse_str(
        r#"{
          "type":"record", "name":"CatalogAttachmentV1", "namespace":"novarocks.frontend",
          "fields":[
            {"name":"attachment_id","type":"string"},
            {"name":"instance_id","type":"string"},
            {"name":"provider_id","type":"string"},
            {"name":"display_name","type":"string"},
            {"name":"durable_properties","type":{"type":"array","items":{"type":"record","name":"CatalogPropertyV1","fields":[{"name":"key","type":"string"},{"name":"value","type":"string"}]}}},
            {"name":"created_at_ms","type":"long"}
          ]
        }"#,
    )
    .expect("catalog attachment v1 schema is valid")
});

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredCatalogAttachment {
    pub(crate) attachment_id: String,
    pub(crate) instance_id: String,
    pub(crate) provider_id: String,
    pub(crate) display_name: String,
    pub(crate) durable_properties: Vec<StoredProperty>,
    pub(crate) created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredProperty {
    pub(crate) key: String,
    pub(crate) value: String,
}

pub(crate) fn encode(value: &StoredCatalogAttachment) -> Result<Bytes, String> {
    let encoded = to_avro_datum(&SCHEMA, to_value(value).map_err(|error| error.to_string())?)
        .map_err(|error| format!("encode catalog attachment v1 Avro payload: {error}"))?;
    let mut envelope = Vec::with_capacity(MAGIC.len() + 1 + encoded.len());
    envelope.extend_from_slice(MAGIC);
    envelope.push(ENVELOPE_VERSION);
    envelope.extend_from_slice(&encoded);
    Ok(Bytes::from(envelope))
}

pub(crate) fn decode(value: &[u8]) -> Result<StoredCatalogAttachment, String> {
    if value.len() < MAGIC.len() + 1 || &value[..MAGIC.len()] != MAGIC {
        return Err("catalog attachment has an invalid envelope magic".to_string());
    }
    if value[MAGIC.len()] != ENVELOPE_VERSION {
        return Err(format!(
            "catalog attachment has unsupported envelope version {}",
            value[MAGIC.len()]
        ));
    }
    let datum = from_avro_datum(&SCHEMA, &mut Cursor::new(&value[MAGIC.len() + 1..]), None)
        .map_err(|error| format!("decode catalog attachment v1 Avro payload: {error}"))?;
    from_value(&datum).map_err(|error| format!("decode catalog attachment v1 fields: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_round_trips_v1() {
        let value = StoredCatalogAttachment {
            attachment_id: Uuid::now_v7().to_string(),
            instance_id: "warehouse".to_string(),
            provider_id: "iceberg".to_string(),
            display_name: "Warehouse".to_string(),
            durable_properties: vec![StoredProperty {
                key: "type".to_string(),
                value: "iceberg".to_string(),
            }],
            created_at_ms: 42,
        };
        assert_eq!(decode(&encode(&value).expect("encode")).expect("decode"), value);
    }
}
