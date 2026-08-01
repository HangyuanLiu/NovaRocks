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

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::domain::{StarRocksFreezeDigest, invalid};

pub(crate) const CODEC_VERSION: u16 = 1;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Base64Bytes(pub Bytes);

impl Serialize for Base64Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD_NO_PAD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        STANDARD_NO_PAD
            .decode(text)
            .map(Bytes::from)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

pub(crate) fn encode_v1<T: Serialize>(
    value: &T,
    subject: &str,
    max_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("serialize StarRocks {subject}: {error}"),
        )
    })?;
    if bytes.len() > max_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("StarRocks {subject} exceeds the request payload budget"),
        ));
    }
    Ok(Bytes::from(bytes))
}

pub(crate) fn decode_v1<T: for<'de> Deserialize<'de>>(
    bytes: &Bytes,
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(bytes).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode StarRocks {subject}: {error}"),
        )
    })
}

pub(crate) fn encode_schema_ipc(schema: &Schema) -> Result<Base64Bytes, ConnectorError> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema)
        .map_err(|error| invalid(format!("encode StarRocks Arrow schema: {error}")))?;
    writer
        .finish()
        .map_err(|error| invalid(format!("finish StarRocks Arrow schema: {error}")))?;
    Ok(Base64Bytes(Bytes::from(writer.get_ref().clone())))
}

pub(crate) fn decode_schema_ipc(schema: &Base64Bytes) -> Result<SchemaRef, ConnectorError> {
    if schema.0.is_empty() {
        return Err(invalid("StarRocks Arrow schema payload must not be empty"));
    }
    StreamReader::try_new(Cursor::new(&schema.0), None)
        .map(|reader| reader.schema())
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode StarRocks Arrow schema: {error}"),
            )
        })
}

pub(crate) fn freeze_digest<T: Serialize>(
    facts: &T,
) -> Result<StarRocksFreezeDigest, ConnectorError> {
    let encoded = serde_json::to_vec(facts).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("serialize StarRocks frozen read facts: {error}"),
        )
    })?;
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.starrocks.connector.freeze.v1");
    hasher.update([0]);
    hasher.update(encoded);
    Ok(StarRocksFreezeDigest(hasher.finalize().into()))
}

pub(crate) fn schema_digest(schema: &Base64Bytes) -> [u8; 32] {
    Sha256::digest(&schema.0).into()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Envelope {
        version: u16,
        payload: Base64Bytes,
    }

    #[test]
    fn json_codec_is_bounded_and_rejects_unknown_fields() {
        let encoded = encode_v1(
            &Envelope {
                version: CODEC_VERSION,
                payload: Base64Bytes(Bytes::from_static(b"secret")),
            },
            "fixture",
            1024,
        )
        .unwrap();
        assert!(String::from_utf8_lossy(&encoded).contains("c2VjcmV0"));
        assert!(
            decode_v1::<Envelope>(
                &Bytes::from_static(br#"{"version":1,"payload":"c2VjcmV0","extra":true}"#),
                "fixture"
            )
            .is_err()
        );
        assert_eq!(
            encode_v1(
                &Envelope {
                    version: 1,
                    payload: Base64Bytes(Bytes::from_static(b"x"))
                },
                "fixture",
                1
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn ipc_schema_and_digest_are_stable() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let encoded = encode_schema_ipc(&schema).unwrap();
        assert_eq!(decode_schema_ipc(&encoded).unwrap().as_ref(), &schema);
        assert_eq!(
            freeze_digest(&vec!["frozen"]).unwrap(),
            freeze_digest(&vec!["frozen"]).unwrap()
        );
    }
}
