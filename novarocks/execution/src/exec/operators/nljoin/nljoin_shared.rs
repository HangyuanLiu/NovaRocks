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
//! Shared artifact/state for nested-loop join execution.
//!
//! Responsibilities:
//! - Stores build-side chunks and synchronization flags shared by NL-join operators.
//! - Coordinates visibility and completion between NL-join build and probe stages.
//!
//! Key exported interfaces:
//! - Types: `NlJoinBuildArtifact`, `NlJoinSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};

/// Materialized build-side chunk storage used by nested-loop join probing.
pub(crate) struct NlJoinBuildArtifact {
    pub(crate) build_batches: Arc<Vec<Chunk>>,
}

impl NlJoinBuildArtifact {
    pub(crate) fn new(build_batches: Vec<Chunk>) -> Self {
        Self {
            build_batches: Arc::new(build_batches),
        }
    }
}

/// Shared state coordinating nested-loop join build publication and probe visibility.
pub(crate) struct NlJoinSharedState {
    node_id: i32,
    build_dep: DependencyHandle,
    build_rows: AtomicUsize,
    build: Mutex<Option<Arc<NlJoinBuildArtifact>>>,
}

impl NlJoinSharedState {
    pub(crate) fn new(node_id: i32, dep_manager: DependencyManager) -> Self {
        let build_dep = dep_manager.get_or_create(format!("nljoin_build:{}", node_id));
        Self {
            node_id,
            build_dep,
            build_rows: AtomicUsize::new(0),
            build: Mutex::new(None),
        }
    }

    pub(crate) fn build_dep(&self) -> DependencyHandle {
        self.build_dep.clone()
    }

    pub(crate) fn node_id(&self) -> i32 {
        self.node_id
    }

    pub(crate) fn set_build(
        &self,
        artifact: Arc<NlJoinBuildArtifact>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let mut guard = self.build.lock().expect("nljoin build lock");
        if guard.is_some() {
            return Err(super::failure::contract("nljoin build already set"));
        }
        let total_rows = artifact
            .build_batches
            .iter()
            .map(|b| b.len())
            .sum::<usize>();
        self.build_rows.store(total_rows, Ordering::Release);
        *guard = Some(artifact);
        self.build_dep.set_ready();
        Ok(())
    }

    pub(crate) fn get_build(&self) -> Option<Arc<NlJoinBuildArtifact>> {
        let guard = self.build.lock().expect("nljoin build lock");
        guard.clone()
    }

    pub(crate) fn has_build(&self) -> bool {
        let guard = self.build.lock().expect("nljoin build lock");
        guard.is_some()
    }
}
