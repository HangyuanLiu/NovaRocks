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

//! Role-local effects driven by the Worker task-lifecycle owner.
//!
//! The Worker decides when a task is created, retired, or discarded.  A role
//! may render those facts as metrics, markers, or result-buffer effects, but
//! it must not decide a second lifecycle verdict while doing so.  These ports
//! make that one-way relationship explicit without giving the Worker a
//! dependency on a Backend runtime or Native adapter.

use std::sync::Arc;

use novarocks_execution_contract::task_execution::identity::TaskIdentity;

use crate::TaskProtocolEvent;

/// Renders one already-classified Worker lifecycle fact.
pub trait TaskProtocolObserver: Send + Sync {
    fn observe(&self, event: TaskProtocolEvent);
}

/// Reclaims role-local result material for an exact Worker task identity.
///
/// `discard_task` is idempotent and may be called before a task owns a root
/// result. `retire_task_result` preserves the distinction between a terminal
/// task record and its result material: the role decides what to retain, while
/// the Worker decides when retirement is legal.
pub trait TaskResultLifecycle: Send + Sync {
    fn discard_task(&self, identity: TaskIdentity);

    fn retire_task_result(&self, identity: TaskIdentity);
}

/// Records a role-local observation that one task passed the Worker creation
/// gate.
///
/// The Worker owns that gate and invokes this only after it accepted the
/// creation transaction. A role may expose the fact as a metric, but cannot
/// infer a second creation verdict from it.
pub trait TaskExecutionMetrics: Send + Sync {
    fn record_task_created(&self);
}

/// The complete role-local effect set one Worker task owner drives.
#[derive(Clone)]
pub struct TaskExecutionPorts {
    observer: Arc<dyn TaskProtocolObserver>,
    result_lifecycle: Arc<dyn TaskResultLifecycle>,
    metrics: Arc<dyn TaskExecutionMetrics>,
}

impl TaskExecutionPorts {
    pub fn new(
        observer: Arc<dyn TaskProtocolObserver>,
        result_lifecycle: Arc<dyn TaskResultLifecycle>,
        metrics: Arc<dyn TaskExecutionMetrics>,
    ) -> Self {
        Self {
            observer,
            result_lifecycle,
            metrics,
        }
    }

    pub fn observe(&self, event: TaskProtocolEvent) {
        self.observer.observe(event);
    }

    pub fn discard_task(&self, identity: TaskIdentity) {
        self.result_lifecycle.discard_task(identity);
    }

    pub fn retire_task_result(&self, identity: TaskIdentity) {
        self.result_lifecycle.retire_task_result(identity);
    }

    pub fn record_task_created(&self) {
        self.metrics.record_task_created();
    }
}
