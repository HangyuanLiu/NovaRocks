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

//! The provider-private codec of the StarRocks table handle.
//!
//! Only the encoding half remains: the table handle is the last StarRocks
//! payload the connector mints, and nothing decodes it now that no read
//! consumes a handle.

use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use serde::{Serialize, Serializer};

pub(crate) const CODEC_VERSION: u16 = 1;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Base64Bytes(pub Bytes);

impl Serialize for Base64Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD_NO_PAD.encode(&self.0))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Envelope {
        version: u16,
        payload: Base64Bytes,
    }

    #[test]
    fn json_codec_base64_encodes_binary_facts_and_honours_the_budget() {
        let envelope = Envelope {
            version: CODEC_VERSION,
            payload: Base64Bytes(Bytes::from_static(b"secret")),
        };
        let encoded = encode_v1(&envelope, "fixture", 1024).expect("encoded fixture");
        assert!(String::from_utf8_lossy(&encoded).contains("c2VjcmV0"));

        assert_eq!(
            encode_v1(&envelope, "fixture", 1)
                .expect_err("a payload over budget is not encodable")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }
}
