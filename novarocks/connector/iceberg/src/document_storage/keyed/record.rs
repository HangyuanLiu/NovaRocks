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

use super::keys::{DocumentKey, corrupt, parse_key};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorSnapshotRecord, DocumentIdentity};

pub(crate) fn encode_record(record: &ConnectorSnapshotRecord) -> Result<String, ConnectorError> {
    record.validate()?;
    Ok(format!(
        "2.{}.{}",
        record
            .binding
            .map(|id| id.to_hex())
            .unwrap_or_else(|| "-".into()),
        URL_SAFE_NO_PAD.encode(&record.payload)
    ))
}
pub(crate) fn decode_record(
    key: &str,
    value: &str,
) -> Result<ConnectorSnapshotRecord, ConnectorError> {
    let Some(DocumentKey::Record { owner, name }) =
        parse_key(key, true).map_err(|e| corrupt(key, e))?
    else {
        return Err(corrupt(key, "expected record key"));
    };
    let parts: Vec<_> = value.split('.').collect();
    let ["2", binding, payload] = parts.as_slice() else {
        return Err(corrupt(key, "invalid record frame"));
    };
    let record = ConnectorSnapshotRecord {
        owner,
        name,
        binding: if *binding == "-" {
            None
        } else {
            Some(DocumentIdentity::parse_hex(binding).map_err(|e| corrupt(key, e))?)
        },
        payload: Bytes::from(
            URL_SAFE_NO_PAD
                .decode(payload)
                .map_err(|e| corrupt(key, e))?,
        ),
    };
    if encode_record(&record)? != value {
        return Err(corrupt(key, "noncanonical record frame"));
    }
    Ok(record)
}
