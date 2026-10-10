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

//! A split source keeps its original admitted responsibility through close.

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
    ConnectorSplitBatch, SplitSourceProfile,
};

use crate::task_execution::blocking_io::{
    ConnectorBlockingIoJoinPin, ConnectorBlockingIoResponsibility, ConnectorBlockingIoSupervisor,
};

pub(crate) struct SupervisedSplitSource {
    source: Option<Box<dyn ConnectorReadSplitSource>>,
    closed: bool,
    plan_node_id: i32,
    owner: SourceOwner,
}

enum SourceOwner {
    Production {
        supervisor: ConnectorBlockingIoSupervisor,
        responsibility: ConnectorBlockingIoResponsibility,
    },
    #[cfg(test)]
    Fixture,
}

impl SupervisedSplitSource {
    pub(crate) fn new(
        source: Box<dyn ConnectorReadSplitSource>,
        plan_node_id: i32,
        supervisor: ConnectorBlockingIoSupervisor,
        responsibility: ConnectorBlockingIoResponsibility,
    ) -> Self {
        Self {
            source: Some(source),
            closed: false,
            plan_node_id,
            owner: SourceOwner::Production {
                supervisor,
                responsibility,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn fixture(source: Box<dyn ConnectorReadSplitSource>) -> Self {
        Self {
            source: Some(source),
            closed: false,
            plan_node_id: 0,
            owner: SourceOwner::Fixture,
        }
    }

    pub(crate) fn join_pin(&self) -> Option<ConnectorBlockingIoJoinPin> {
        match &self.owner {
            SourceOwner::Production { responsibility, .. } => Some(responsibility.join_pin()),
            #[cfg(test)]
            SourceOwner::Fixture => None,
        }
    }
}

impl ConnectorReadSplitSource for SupervisedSplitSource {
    fn profile_snapshot(&self) -> SplitSourceProfile {
        self.source
            .as_ref()
            .expect("source retained until actual backing exit")
            .profile_snapshot()
    }

    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        self.source
            .as_ref()
            .expect("open split source")
            .initial_dynamic_filter_wait_request()
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
        self.source
            .as_mut()
            .expect("open split source")
            .next_batch(max_size, dynamic_filter)
    }

    fn is_finished(&self) -> bool {
        self.source
            .as_ref()
            .is_none_or(|source| source.is_finished())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // Cleanup never requires a successful provider profile call. Keep the
        // real source backing until its enclosing original job retires.
        self.source
            .as_mut()
            .expect("source retained through close")
            .close()
    }
}

impl Drop for SupervisedSplitSource {
    fn drop(&mut self) {
        let Some(mut source) = self.source.take() else {
            return;
        };
        if self.closed {
            drop(source);
            return;
        }
        match &self.owner {
            SourceOwner::Production {
                supervisor,
                responsibility,
            } => {
                let pin = responsibility.join_pin();
                let plan_node_id = self.plan_node_id;
                // Mint the original join pin before this owner's fields retire.
                // This also reaps remaining sources after a batch-close panic.
                let _ = supervisor.spawn_pinned(vec![pin], move || {
                    if let Err(error) = source.close() {
                        tracing::warn!(plan_node_id, error = %error, "closing an abandoned split source failed");
                    }
                    super::round::emit_split_source_close_marker(plan_node_id);
                    drop(source);
                });
            }
            #[cfg(test)]
            SourceOwner::Fixture => {
                let _ = source.close();
            }
        }
    }
}
