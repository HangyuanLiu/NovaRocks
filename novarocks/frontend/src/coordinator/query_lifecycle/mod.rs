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

mod barrier;
mod lease;
mod manifest;

#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use novarocks::query_execution::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand, QueryControlEvent, QueryInitAck,
    QueryInitRequest, QueryTerminationAck,
};

pub trait QueryLifecycleTransport: Send + Sync + 'static {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError>;

    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError>;

    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError>;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryLifecycleTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    start_epoch: u64,
}

impl QueryLifecycleTarget {
    pub const fn new(backend_idx: usize, endpoint: SocketAddr, start_epoch: u64) -> Self {
        Self {
            backend_idx,
            endpoint,
            start_epoch,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(self) -> SocketAddr {
        self.endpoint
    }

    pub const fn start_epoch(self) -> u64 {
        self.start_epoch
    }
}

pub trait QueryControlSession: Send + Sync + 'static {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError>;

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLifecycleTransportErrorKind {
    DeadlineExceeded,
    StreamClosed,
    Backpressure,
    InvalidResponse,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLifecycleTransportError {
    kind: QueryLifecycleTransportErrorKind,
    detail: String,
}

impl QueryLifecycleTransportError {
    pub fn new(kind: QueryLifecycleTransportErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> QueryLifecycleTransportErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub const fn is_unknown_init_outcome(&self) -> bool {
        matches!(
            self.kind,
            QueryLifecycleTransportErrorKind::DeadlineExceeded
                | QueryLifecycleTransportErrorKind::StreamClosed
        )
    }
}

impl std::fmt::Display for QueryLifecycleTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for QueryLifecycleTransportError {}
