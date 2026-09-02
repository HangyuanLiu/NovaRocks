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

//! Frontend-local adapters for terminal wire leaves.
//!
//! Protocol owns validation and the generated terminal values. These adapters
//! only bridge Frontend-owned execution profile views.

use novarocks_execution::runtime::profile::{
    ProfileCounter, ProfileNode, ProfileUnit, RuntimeProfileTree, default_counter_strategy,
};
use novarocks_proto_models::novarocks;

pub(crate) fn decode_runtime_profile_tree(
    tree: &novarocks::RuntimeProfileTree,
) -> Result<RuntimeProfileTree, String> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| "RuntimeProfileTree missing root".to_string())?;
    Ok(RuntimeProfileTree {
        root: decode_profile_node(root)?,
    })
}

fn decode_profile_node(node: &novarocks::ProfileNode) -> Result<ProfileNode, String> {
    Ok(ProfileNode {
        name: node.name.clone(),
        node_id: node.node_id,
        counters: node
            .counters
            .iter()
            .map(decode_profile_counter)
            .collect::<Result<Vec<_>, _>>()?,
        info_strings: node
            .info_strings
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        children: node
            .children
            .iter()
            .map(decode_profile_node)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn decode_profile_counter(counter: &novarocks::Counter) -> Result<ProfileCounter, String> {
    let unit = match novarocks::ProfileUnit::try_from(counter.unit) {
        Ok(novarocks::ProfileUnit::Unit) => ProfileUnit::Unit,
        Ok(novarocks::ProfileUnit::CpuTicks) => ProfileUnit::CpuTicks,
        Ok(novarocks::ProfileUnit::Bytes) => ProfileUnit::Bytes,
        Ok(novarocks::ProfileUnit::TimeNs) => ProfileUnit::TimeNs,
        Ok(novarocks::ProfileUnit::TimeMs) => ProfileUnit::TimeMs,
        Ok(novarocks::ProfileUnit::TimeS) => ProfileUnit::TimeS,
        Ok(novarocks::ProfileUnit::None) => ProfileUnit::None,
        Ok(novarocks::ProfileUnit::Unspecified) => {
            return Err("ProfileUnit is unspecified in native runtime profile".to_string());
        }
        Err(_) => {
            return Err(format!(
                "unknown ProfileUnit value {} in native runtime profile",
                counter.unit
            ));
        }
    };
    Ok(ProfileCounter {
        name: counter.name.clone(),
        parent_name: counter.parent_name.clone(),
        unit,
        strategy: default_counter_strategy(unit),
        value: counter.value,
        min_value: counter.min_value,
        max_value: counter.max_value,
    })
}
