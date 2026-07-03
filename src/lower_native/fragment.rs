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

//! Proto fragment lowering placeholder.

use std::sync::Arc;

use crate::lower::fragment::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;

#[allow(unused)]
pub(crate) fn execute_fragment_native(
    fragment: &crate::proto::plan::PlanFragment,
    instance_params: &crate::proto::novarocks::InstanceParams,
    session_time_zone: Option<&str>,
    pipeline_dop: i32,
    db_name: Option<&str>,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
) -> Result<FragmentOutput, String> {
    todo!("native proto fragment lowering is implemented in NIDL-4 M2.5")
}
