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

//! Worker-owned observation of the current local admission epoch.

use novarocks_execution_contract::AdmissionEpochCapability;

/// The exact admission epoch a new native task-context acquisition must freeze.
///
/// A Native transport adapter may observe this opaque capability for a
/// heartbeat response, but it cannot mint, rotate, or admit against it.
pub trait WorkerAdmissionEpochAuthority: Send + Sync + 'static {
    fn admission_epoch_capability(&self) -> AdmissionEpochCapability;
}
