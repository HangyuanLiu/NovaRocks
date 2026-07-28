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
use std::time::Instant;

use super::{
    ConnectorError, ConnectorErrorKind, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

pub trait ConnectorCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Clone)]
pub struct ConnectorRequestContext {
    deadline: Instant,
    cancellation: Arc<dyn ConnectorCancellation>,
    max_handle_payload_bytes: usize,
    max_total_payload_bytes: usize,
}

impl ConnectorRequestContext {
    pub fn try_new(
        deadline: Instant,
        cancellation: Arc<dyn ConnectorCancellation>,
        max_handle_payload_bytes: usize,
        max_total_payload_bytes: usize,
    ) -> Result<Self, ConnectorError> {
        if max_handle_payload_bytes == 0
            || max_total_payload_bytes == 0
            || max_handle_payload_bytes > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
            || max_total_payload_bytes > MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES
            || max_total_payload_bytes < max_handle_payload_bytes
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid connector payload budget",
            ));
        }
        Ok(Self {
            deadline,
            cancellation,
            max_handle_payload_bytes,
            max_total_payload_bytes,
        })
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancellation(&self) -> &Arc<dyn ConnectorCancellation> {
        &self.cancellation
    }

    pub const fn max_handle_payload_bytes(&self) -> usize {
        self.max_handle_payload_bytes
    }

    pub const fn max_total_payload_bytes(&self) -> usize {
        self.max_total_payload_bytes
    }
}
