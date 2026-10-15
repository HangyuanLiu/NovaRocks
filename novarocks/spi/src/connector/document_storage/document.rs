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

use crate::connector::{ConnectorError, ConnectorErrorKind};
use std::sync::Arc;
pub(crate) const MAX_CONNECTOR_DOCUMENT_NAME_BYTES: usize = 128;
pub(crate) const MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorDocumentFormat {
    owner: Arc<str>,
    name: Arc<str>,
    version: u32,
}

impl ConnectorDocumentFormat {
    pub fn try_new(
        owner: impl AsRef<str>,
        name: impl AsRef<str>,
        version: u32,
    ) -> Result<Self, ConnectorError> {
        validate_token(
            owner.as_ref(),
            MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES,
            "document format owner",
        )?;
        validate_token(
            name.as_ref(),
            MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES,
            "document format name",
        )?;
        if version == 0 {
            return Err(invalid("document format version must be non-zero"));
        }
        Ok(Self {
            owner: Arc::from(owner.as_ref()),
            name: Arc::from(name.as_ref()),
            version,
        })
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
}

pub(crate) fn validate_token(
    value: &str,
    max_bytes: usize,
    field: &str,
) -> Result<(), ConnectorError> {
    if value.is_empty()
        || value.len() > max_bytes
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == 0)
    {
        return Err(invalid(format!(
            "{field} must be bounded non-whitespace ASCII"
        )));
    }
    Ok(())
}

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

pub(crate) fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}
