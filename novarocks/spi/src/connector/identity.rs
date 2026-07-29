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

use super::{ConnectorError, ConnectorErrorKind};

const MAX_PROVIDER_ID_BYTES: usize = 64;
const MAX_INSTANCE_ID_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorProviderId(Arc<str>);

impl ConnectorProviderId {
    pub fn parse(value: &str) -> Result<Self, ConnectorError> {
        if !is_provider_id(value) {
            return Err(invalid_id("connector provider ID"));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorInstanceId(Arc<str>);

impl ConnectorInstanceId {
    pub fn parse(value: &str) -> Result<Self, ConnectorError> {
        if !value.is_ascii() {
            return Err(invalid_id("connector instance ID"));
        }
        let normalized = value.to_ascii_lowercase();
        if !is_instance_id(&normalized) {
            return Err(invalid_id("connector instance ID"));
        }
        Ok(Self(Arc::from(normalized)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorInstanceDescriptor {
    pub provider_id: ConnectorProviderId,
    pub instance_id: ConnectorInstanceId,
}

fn is_provider_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_PROVIDER_ID_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn is_instance_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_INSTANCE_ID_BYTES
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

fn invalid_id(subject: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}
