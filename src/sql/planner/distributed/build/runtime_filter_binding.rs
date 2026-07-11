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

use std::collections::HashMap;

use crate::sql::planner::distributed::runtime_filter::{
    BoundRuntimeFilterBuild, BoundRuntimeFilterProbe,
};
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, FragmentId, PlanFragment,
};
use crate::sql::planner::physical::runtime_filter::{
    RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};

pub(super) struct RuntimeFilterBuildBinding {
    pub(super) node_id: i32,
    pub(super) fragment_id: FragmentId,
    pub(super) intent: RuntimeFilterBuildIntent,
}

pub(super) struct RuntimeFilterProbeBinding {
    pub(super) node_id: i32,
    pub(super) fragment_id: FragmentId,
    pub(super) intent: RuntimeFilterProbeIntent,
}

pub(super) struct RuntimeFilterBindings {
    pub(super) builds: Vec<RuntimeFilterBuildBinding>,
    pub(super) probes: Vec<RuntimeFilterProbeBinding>,
}

impl RuntimeFilterBindings {
    pub(super) fn new() -> Self {
        Self {
            builds: Vec::new(),
            probes: Vec::new(),
        }
    }

    pub(super) fn record(
        &mut self,
        node_id: i32,
        fragment_id: FragmentId,
        physical: &PhysicalPlanNode,
        distributed_payload: &DistributedNodeKind,
    ) {
        for intent in &physical.probe_runtime_filters {
            self.probes.push(RuntimeFilterProbeBinding {
                node_id,
                fragment_id,
                intent: intent.clone(),
            });
        }
        if matches!(distributed_payload, DistributedNodeKind::HashJoin(_))
            && let PhysicalPlanKind::HashJoin(join) = &physical.kind
        {
            for intent in &join.build_runtime_filters {
                self.builds.push(RuntimeFilterBuildBinding {
                    node_id,
                    fragment_id,
                    intent: intent.clone(),
                });
            }
        }
    }
}

pub(super) fn bind_runtime_filters(
    fragments: &mut [PlanFragment],
    bindings: RuntimeFilterBindings,
) {
    let mut source_fragment_by_filter = HashMap::new();
    for build in &bindings.builds {
        source_fragment_by_filter
            .entry(build.intent.filter_id)
            .or_insert(build.fragment_id);
    }

    let mut target_fragments_by_filter: HashMap<i32, Vec<FragmentId>> = HashMap::new();
    for probe in &bindings.probes {
        if !source_fragment_by_filter.contains_key(&probe.intent.filter_id) {
            continue;
        }
        let targets = target_fragments_by_filter
            .entry(probe.intent.filter_id)
            .or_default();
        if !targets.contains(&probe.fragment_id) {
            targets.push(probe.fragment_id);
        }
    }

    let mut builds_by_node: HashMap<i32, Vec<BoundRuntimeFilterBuild>> = HashMap::new();
    for build in bindings.builds {
        let target_fragment_ids = target_fragments_by_filter
            .get(&build.intent.filter_id)
            .cloned()
            .unwrap_or_default();
        builds_by_node
            .entry(build.node_id)
            .or_default()
            .push(BoundRuntimeFilterBuild {
                intent: build.intent,
                source_fragment_id: build.fragment_id,
                target_fragment_ids,
            });
    }

    let mut probes_by_node: HashMap<i32, Vec<BoundRuntimeFilterProbe>> = HashMap::new();
    for probe in bindings.probes {
        let filter_id = probe.intent.filter_id;
        let Some(&source_fragment_id) = source_fragment_by_filter.get(&filter_id) else {
            continue;
        };
        let probes = probes_by_node.entry(probe.node_id).or_default();
        if probes
            .iter()
            .any(|bound| bound.intent.filter_id == filter_id)
        {
            continue;
        }
        probes.push(BoundRuntimeFilterProbe {
            intent: probe.intent,
            source_fragment_id,
        });
    }

    for fragment in fragments {
        attach_runtime_filters(&mut fragment.root, &mut builds_by_node, &mut probes_by_node);
    }
}

fn attach_runtime_filters(
    node: &mut DistributedNode,
    builds_by_node: &mut HashMap<i32, Vec<BoundRuntimeFilterBuild>>,
    probes_by_node: &mut HashMap<i32, Vec<BoundRuntimeFilterProbe>>,
) {
    if let Some(mut builds) = builds_by_node.remove(&node.node_id) {
        node.build_runtime_filters.append(&mut builds);
    }
    if let Some(mut probes) = probes_by_node.remove(&node.node_id) {
        node.probe_runtime_filters.append(&mut probes);
    }
    for child in &mut node.children {
        attach_runtime_filters(child, builds_by_node, probes_by_node);
    }
}
