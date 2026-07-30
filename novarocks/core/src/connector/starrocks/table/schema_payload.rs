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

//! Versioned core-domain payload for standalone StarRocks table schemas.
//!
//! This is intentionally not a StarRocks protobuf wire format. Repository
//! writes use this self-identifying domain representation; the catalog keeps
//! a legacy protobuf read fallback until existing repositories are migrated.

use crate::connector::starrocks::schema::StarRocksTabletSchema;

const MAGIC: &[u8] = b"NOVAROCKS_SCHEMA_JSON_V1\n";

pub(crate) fn encode(schema: &StarRocksTabletSchema) -> Result<Vec<u8>, String> {
    schema.validate()?;
    let mut bytes = MAGIC.to_vec();
    serde_json::to_writer(&mut bytes, schema)
        .map_err(|error| format!("encode domain tablet schema payload failed: {error}"))?;
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Option<StarRocksTabletSchema>, String> {
    let Some(payload) = bytes.strip_prefix(MAGIC) else {
        return Ok(None);
    };
    let schema: StarRocksTabletSchema = serde_json::from_slice(payload)
        .map_err(|error| format!("decode domain tablet schema payload failed: {error}"))?;
    schema.validate()?;
    Ok(Some(schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::schema::{
        StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
    };

    #[test]
    fn round_trip_is_self_identifying_and_domain_only() {
        let schema = StarRocksTabletSchema {
            id: Some(7),
            keys_type: Some(StarRocksKeysType::Duplicate),
            column: vec![StarRocksColumnSchema {
                unique_id: 1,
                name: Some("k1".to_string()),
                r#type: "BIGINT".to_string(),
                ..StarRocksColumnSchema::default()
            }],
            ..StarRocksTabletSchema::default()
        };
        let encoded = encode(&schema).expect("encode schema");
        assert!(encoded.starts_with(MAGIC));
        assert_eq!(decode(&encoded).expect("decode schema"), Some(schema));
        assert_eq!(
            decode(b"legacy protobuf bytes").expect("legacy marker"),
            None
        );
    }
}
