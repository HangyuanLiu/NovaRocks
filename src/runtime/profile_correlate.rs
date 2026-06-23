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

use crate::runtime::profile::Profiler;
use crate::runtime_profile;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActualMetrics {
    pub(crate) output_rows: i64,
    pub(crate) total_time_ns: i64,
    pub(crate) peak_mem_bytes: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DistributedProfileSummary {
    pub(crate) fragment_instance_count: usize,
    pub(crate) fragment_wall_max_ns: i64,
    pub(crate) fragment_wall_sum_ns: i64,
    pub(crate) driver_total_time_ns: i64,
    pub(crate) operator_active_time_ns: i64,
    pub(crate) driver_blocked_time_ns: i64,
    pub(crate) source_wait_time_ns: i64,
    pub(crate) sink_wait_time_ns: i64,
    pub(crate) dependency_wait_time_ns: i64,
    pub(crate) exchange_wait_time_ns: i64,
    pub(crate) exchange_process_time_ns: i64,
    pub(crate) network_time_ns: i64,
    pub(crate) scan_io_time_ns: i64,
}

pub(crate) fn collect_actuals_by_plan_node_id(profiler: &Profiler) -> HashMap<i32, ActualMetrics> {
    collect_actuals_by_plan_node_id_multi(std::slice::from_ref(profiler))
}

pub(crate) fn collect_actuals_by_plan_node_id_multi(
    profilers: &[Profiler],
) -> HashMap<i32, ActualMetrics> {
    let mut actuals = HashMap::new();
    for profiler in profilers {
        let merged = crate::service::fe_report::merge_pipeline_profiles_for_fe(profiler);
        collect_rec(&merged, &mut actuals);
    }
    actuals
}

pub(crate) fn collect_actuals_by_plan_node_id_from_profile_trees(
    trees: &[runtime_profile::TRuntimeProfileTree],
) -> HashMap<i32, ActualMetrics> {
    let mut actuals = HashMap::new();
    for tree in trees {
        if !tree.nodes.is_empty() {
            collect_thrift_rec(&tree.nodes, 0, &mut actuals);
        }
    }
    actuals
}

pub(crate) fn collect_distributed_profile_summary_from_profile_trees(
    trees: &[runtime_profile::TRuntimeProfileTree],
) -> DistributedProfileSummary {
    let mut summary = DistributedProfileSummary::default();
    for tree in trees {
        if tree.nodes.is_empty() {
            continue;
        }
        summary.fragment_instance_count += 1;
        let fragment_wall_ns = thrift_counter_in_tree(tree, "FragmentWallTime");
        summary.fragment_wall_sum_ns = summary
            .fragment_wall_sum_ns
            .saturating_add(fragment_wall_ns);
        summary.fragment_wall_max_ns = summary.fragment_wall_max_ns.max(fragment_wall_ns);
        summary.driver_total_time_ns = summary
            .driver_total_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "DriverTotalTime"));
        summary.operator_active_time_ns = summary
            .operator_active_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "OperatorTotalTime"));
        summary.driver_blocked_time_ns = summary
            .driver_blocked_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "DriverBlockedTime"));
        summary.source_wait_time_ns = summary
            .source_wait_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "DriverInputEmptyTime"));
        summary.sink_wait_time_ns = summary
            .sink_wait_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "DriverOutputFullTime"));
        summary.dependency_wait_time_ns = summary
            .dependency_wait_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "DriverDependencyWaitTime"));
        summary.exchange_wait_time_ns = summary
            .exchange_wait_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "WaitTime"));
        summary.exchange_process_time_ns = summary
            .exchange_process_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "ReceiverProcessTotalTime"));
        summary.network_time_ns = summary
            .network_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "NetworkTime"));
        summary.scan_io_time_ns = summary
            .scan_io_time_ns
            .saturating_add(thrift_counter_in_tree(tree, "IOTaskExecTime"));
    }
    summary
}

fn collect_rec(node: &Profiler, actuals: &mut HashMap<i32, ActualMetrics>) {
    if let Some(node_id) = parse_plan_node_id(&node.name()) {
        if let Some(common) = node.get_child("CommonMetrics") {
            merge_actual_metrics(
                actuals,
                node_id,
                ActualMetrics {
                    output_rows: counter(&common, "PullRowNum"),
                    total_time_ns: counter(&common, "OperatorTotalTime"),
                    peak_mem_bytes: counter(&common, "OperatorPeakMemoryUsage"),
                },
            );
        }
    }

    for child in node.children() {
        collect_rec(&child, actuals);
    }
}

fn collect_thrift_rec(
    nodes: &[runtime_profile::TRuntimeProfileNode],
    idx: usize,
    actuals: &mut HashMap<i32, ActualMetrics>,
) -> usize {
    let Some(node) = nodes.get(idx) else {
        return idx;
    };
    let mut next = idx + 1;
    let mut child_ranges = Vec::new();
    for _ in 0..node.num_children.max(0) {
        let child_start = next;
        next = collect_thrift_rec(nodes, child_start, actuals);
        child_ranges.push(child_start..next);
    }

    if let Some(node_id) = parse_plan_node_id(&node.name) {
        for range in child_ranges {
            if nodes
                .get(range.start)
                .is_some_and(|child| child.name == "CommonMetrics")
            {
                let metrics = ActualMetrics {
                    output_rows: thrift_counter(nodes, range.clone(), "PullRowNum"),
                    total_time_ns: thrift_counter(nodes, range.clone(), "OperatorTotalTime"),
                    peak_mem_bytes: thrift_counter(nodes, range, "OperatorPeakMemoryUsage"),
                };
                merge_actual_metrics(actuals, node_id, metrics);
                break;
            }
        }
    }

    next
}

fn merge_actual_metrics(
    actuals: &mut HashMap<i32, ActualMetrics>,
    node_id: i32,
    metrics: ActualMetrics,
) {
    let entry = actuals.entry(node_id).or_default();
    entry.output_rows = entry.output_rows.saturating_add(metrics.output_rows);
    entry.total_time_ns = entry.total_time_ns.max(metrics.total_time_ns);
    entry.peak_mem_bytes = entry.peak_mem_bytes.saturating_add(metrics.peak_mem_bytes);
}

fn counter(common: &Profiler, name: &str) -> i64 {
    common.counter_value(name).unwrap_or(0)
}

fn thrift_counter(
    nodes: &[runtime_profile::TRuntimeProfileNode],
    range: std::ops::Range<usize>,
    name: &str,
) -> i64 {
    nodes[range]
        .iter()
        .flat_map(|node| node.counters.iter())
        .filter(|counter| counter.name == name)
        .map(|counter| counter.value)
        .sum()
}

fn thrift_counter_in_tree(tree: &runtime_profile::TRuntimeProfileTree, name: &str) -> i64 {
    tree.nodes
        .iter()
        .flat_map(|node| node.counters.iter())
        .filter(|counter| counter.name == name)
        .map(|counter| counter.value)
        .fold(0_i64, i64::saturating_add)
}

fn parse_plan_node_id(name: &str) -> Option<i32> {
    let key = if name.contains("plan_node_id=") {
        "plan_node_id="
    } else {
        "(id="
    };
    let start = name.find(key)? + key.len();
    let rest = &name[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{
        ActualMetrics, collect_actuals_by_plan_node_id,
        collect_actuals_by_plan_node_id_from_profile_trees, collect_actuals_by_plan_node_id_multi,
        collect_distributed_profile_summary_from_profile_trees,
    };
    use crate::metrics;
    use crate::runtime::profile::Profiler;

    fn add_operator_metrics(
        parent: &Profiler,
        name: &str,
        output_rows: i64,
        total_time_ns: i64,
        peak_mem_bytes: i64,
    ) {
        let common = parent.child(name).child("CommonMetrics");
        common.counter_set("PullRowNum", metrics::TUnit::UNIT, output_rows);
        common.counter_set("OperatorTotalTime", metrics::TUnit::TIME_NS, total_time_ns);
        common.counter_set(
            "OperatorPeakMemoryUsage",
            metrics::TUnit::BYTES,
            peak_mem_bytes,
        );
    }

    #[test]
    fn collects_single_operator_profile() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "SCAN (plan_node_id=2)", 10, 5, 64);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&2).copied(),
            Some(ActualMetrics {
                output_rows: 10,
                total_time_ns: 5,
                peak_mem_bytes: 64,
            })
        );
    }

    #[test]
    fn merges_driver_instances_before_collecting() {
        let profiler = Profiler::new("query");
        let pipeline = profiler.child("Pipeline (id=0)");
        let driver0 = pipeline.child("PipelineDriver (id=0)");
        add_operator_metrics(&driver0, "SCAN (plan_node_id=2)", 10, 5, 64);
        let driver1 = pipeline.child("PipelineDriver (id=1)");
        add_operator_metrics(&driver1, "SCAN (plan_node_id=2)", 20, 5, 32);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&2).copied(),
            Some(ActualMetrics {
                output_rows: 30,
                total_time_ns: 5,
                peak_mem_bytes: 96,
            })
        );
    }

    #[test]
    fn collects_runtime_operator_id_names_before_fe_normalization() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "AGGREGATE (id=3)", 1, 12_000, 128);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&3).copied(),
            Some(ActualMetrics {
                output_rows: 1,
                total_time_ns: 12_000,
                peak_mem_bytes: 128,
            })
        );
    }

    #[test]
    fn merges_profiles_across_fragments() {
        let scan_fragment = Profiler::new("fragment-0");
        let scan_driver = scan_fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&scan_driver, "SCAN (plan_node_id=2)", 100, 20, 256);

        let agg_fragment = Profiler::new("fragment-1");
        let agg_driver = agg_fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&agg_driver, "AGGREGATE (plan_node_id=5)", 1, 30, 512);

        let actuals = collect_actuals_by_plan_node_id_multi(&[scan_fragment, agg_fragment]);

        assert_eq!(
            actuals.get(&2).copied(),
            Some(ActualMetrics {
                output_rows: 100,
                total_time_ns: 20,
                peak_mem_bytes: 256,
            })
        );
        assert_eq!(
            actuals.get(&5).copied(),
            Some(ActualMetrics {
                output_rows: 1,
                total_time_ns: 30,
                peak_mem_bytes: 512,
            })
        );
    }

    #[test]
    fn collects_actuals_from_thrift_profile_trees() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "HASH JOIN (plan_node_id=4)", 8, 900_000, 4096);
        let tree =
            crate::service::fe_report::merge_pipeline_profiles_for_fe(&profiler).to_thrift_tree();

        let actuals = collect_actuals_by_plan_node_id_from_profile_trees(&[tree]);

        assert_eq!(
            actuals.get(&4).copied(),
            Some(ActualMetrics {
                output_rows: 8,
                total_time_ns: 900_000,
                peak_mem_bytes: 4096,
            })
        );
    }

    #[test]
    fn summarizes_distributed_profile_attribution_from_thrift_trees() {
        let profiler = Profiler::new("fragment");
        profiler.counter_set("FragmentWallTime", metrics::TUnit::TIME_NS, 20_000);
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        driver.counter_set("DriverTotalTime", metrics::TUnit::TIME_NS, 10_000);
        driver.counter_set("DriverBlockedTime", metrics::TUnit::TIME_NS, 4_500);
        driver.counter_set("DriverInputEmptyTime", metrics::TUnit::TIME_NS, 2_000);
        driver.counter_set("DriverOutputFullTime", metrics::TUnit::TIME_NS, 1_500);
        driver.counter_set("DriverDependencyWaitTime", metrics::TUnit::TIME_NS, 1_000);
        add_operator_metrics(&driver, "EXCHANGE_SOURCE (plan_node_id=2)", 8, 3_000, 512);
        let exchange_unique = driver
            .child("EXCHANGE_SOURCE (plan_node_id=2)")
            .child("UniqueMetrics");
        exchange_unique.counter_set("WaitTime", metrics::TUnit::TIME_NS, 700);
        exchange_unique.counter_set("ReceiverProcessTotalTime", metrics::TUnit::TIME_NS, 300);
        add_operator_metrics(&driver, "DATA_STREAM_SINK (plan_node_id=3)", 8, 2_000, 256);
        let sink_unique = driver
            .child("DATA_STREAM_SINK (plan_node_id=3)")
            .child("UniqueMetrics");
        sink_unique.counter_set("NetworkTime", metrics::TUnit::TIME_NS, 900);
        let scan_unique = driver.child("SCAN (plan_node_id=4)").child("UniqueMetrics");
        scan_unique.counter_set("IOTaskExecTime", metrics::TUnit::TIME_NS, 1_100);

        let summary =
            collect_distributed_profile_summary_from_profile_trees(&[profiler.to_thrift_tree()]);

        assert_eq!(summary.fragment_instance_count, 1);
        assert_eq!(summary.fragment_wall_max_ns, 20_000);
        assert_eq!(summary.fragment_wall_sum_ns, 20_000);
        assert_eq!(summary.driver_total_time_ns, 10_000);
        assert_eq!(summary.operator_active_time_ns, 5_000);
        assert_eq!(summary.driver_blocked_time_ns, 4_500);
        assert_eq!(summary.source_wait_time_ns, 2_000);
        assert_eq!(summary.sink_wait_time_ns, 1_500);
        assert_eq!(summary.dependency_wait_time_ns, 1_000);
        assert_eq!(summary.exchange_wait_time_ns, 700);
        assert_eq!(summary.exchange_process_time_ns, 300);
        assert_eq!(summary.network_time_ns, 900);
        assert_eq!(summary.scan_io_time_ns, 1_100);
    }
}
