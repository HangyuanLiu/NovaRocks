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

use std::sync::Arc;

use bytes::Bytes;

use super::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId};

pub const MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableHandle {
    owner: ConnectorInstanceId,
    payload: Bytes,
}

impl ConnectorTableHandle {
    pub fn try_new(owner: ConnectorInstanceId, payload: Bytes) -> Result<Self, ConnectorError> {
        validate_payload(&payload)?;
        Ok(Self { owner, payload })
    }

    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorScanHandle {
    owner: ConnectorInstanceId,
    payload: Bytes,
}

impl ConnectorScanHandle {
    pub fn try_new(owner: ConnectorInstanceId, payload: Bytes) -> Result<Self, ConnectorError> {
        validate_payload(&payload)?;
        Ok(Self { owner, payload })
    }

    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorSplit {
    owner: ConnectorInstanceId,
    split_id: Arc<str>,
    payload: Bytes,
    estimated_bytes: Option<u64>,
}

impl ConnectorSplit {
    pub fn try_new(
        owner: ConnectorInstanceId,
        split_id: impl Into<Arc<str>>,
        payload: Bytes,
        estimated_bytes: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        let split_id = split_id.into();
        if split_id.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector split ID must not be empty",
            ));
        }
        validate_payload(&payload)?;
        Ok(Self {
            owner,
            split_id,
            payload,
            estimated_bytes,
        })
    }

    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }

    pub fn split_id(&self) -> &str {
        &self.split_id
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn estimated_bytes(&self) -> Option<u64> {
        self.estimated_bytes
    }
}

fn validate_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector handle payload exceeds the hard limit",
        ));
    }
    Ok(())
}
