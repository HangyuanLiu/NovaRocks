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

use crate::common::types::UniqueId;
use crate::exec::fragment::program::FragmentSinkSpec;
use crate::proto;
use crate::protocol::native::decode;
use crate::runtime::fragment::sink::materialize_fragment_sink_components;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::result_buffer;

pub(super) fn prepare_result_buffer_for_native_sink(
    sink: &proto::plan::DataSink,
    finst_id: UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) -> Result<(), String> {
    let uses_fetch_result_buffer = matches!(
        sink.kind.as_ref(),
        Some(proto::plan::data_sink::Kind::Result(true))
    );
    if !uses_fetch_result_buffer {
        return Ok(());
    }
    if typed_result_sink {
        result_buffer::create_typed_sender(finst_id);
    } else {
        result_buffer::create_sender(finst_id);
    }
    if let Some(root) = mem_tracker {
        let label = format!("ResultBuffer: finst={}", finst_id);
        let tracker = MemTracker::new_child(label, root);
        result_buffer::set_mem_tracker(finst_id, tracker);
    }
    Ok(())
}

pub(super) fn sink_factory_from_native(
    fragment: &proto::plan::PlanFragment,
    sink: &proto::plan::DataSink,
    instance_params: &proto::novarocks::InstanceParams,
    typed_result_sink: bool,
    layout: &decode::Layout,
) -> Result<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>, String> {
    let program = decode::decode_fragment_sink_program(fragment, layout)
        .map_err(|error| error.to_string())?;
    let program = FragmentSinkSpec::try_new(program).map_err(|error| error.to_string())?;
    let assignment = decode::decode_fragment_sink_assignment(sink, instance_params)
        .map_err(|error| error.to_string())?;
    let fragment_instance_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())
        .map(|id| UniqueId {
            hi: id.hi,
            lo: id.lo,
        })?;
    let root_plan_node_id = fragment
        .root
        .as_ref()
        .map(|node| node.node_id)
        .unwrap_or(-1);

    materialize_fragment_sink_components(
        &program,
        &assignment,
        fragment_instance_id,
        typed_result_sink,
        root_plan_node_id,
    )
    .map_err(|error| error.to_string())
}
