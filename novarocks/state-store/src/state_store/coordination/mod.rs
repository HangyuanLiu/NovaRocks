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

mod clock;
#[allow(dead_code)]
mod codec;
#[allow(dead_code)]
mod error;
mod gate;
mod lease;
mod metrics;
#[allow(dead_code)]
mod model;
mod operation;

pub use clock::{ClockHealth, LeaseClock, LeaseSettings};
pub use error::{CoordinationError, CoordinationErrorKind};
pub use gate::{IncarnationGate, WriteAdmission};
pub use lease::{AcquireOutcome, LeaseGuard, LeaseManager};
pub use metrics::{
    COORDINATION_OPERATION_COUNT, COORDINATION_OUTCOME_COUNT, CoordinationMetrics,
    CoordinationMetricsSnapshot, CoordinationOperation, CoordinationOutcome,
};
pub use model::{
    AttemptId, ControlPlaneIncarnation, ControlPlaneMode, ControlPlaneSnapshot, FencingToken,
    HolderId, LeaseCancellationReason, LeaseObservation, ResourceEpoch, ResourceKey,
};
