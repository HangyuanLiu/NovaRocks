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

use std::sync::OnceLock;

use datasketches::hash::value::raw_bytes;
use datasketches::hll::{HllSketch, HllType, HllUnion};
use datasketches::theta::{CompactThetaSketch, ThetaSketchBuilder, ThetaUnionBuilder};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const WORKLOAD_MANIFEST: &str = include_str!("../workloads.toml");

#[derive(Clone, Debug, Deserialize)]
pub struct Workloads {
    pub schema_version: u32,
    pub seed: u64,
    pub theta: ThetaWorkload,
    pub hll: HllWorkload,
    pub measurement: MeasurementWorkload,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ThetaWorkload {
    pub lg_k: u8,
    pub updates: usize,
    pub duplicate_percent: usize,
    pub partitions: usize,
    pub partition_size: usize,
    pub overlaps_percent: Vec<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HllWorkload {
    pub lg_k: u8,
    pub downsample_lg_k: u8,
    pub updates: usize,
    pub sparse_updates: usize,
    pub dense_updates: usize,
    pub aux_scan_limit: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MeasurementWorkload {
    pub profile: String,
    pub warmup_seconds: u64,
    pub sample_count: usize,
}

pub fn workloads() -> &'static Workloads {
    static WORKLOADS: OnceLock<Workloads> = OnceLock::new();
    WORKLOADS.get_or_init(|| {
        let parsed: Workloads =
            toml::from_str(WORKLOAD_MANIFEST).expect("embedded workloads.toml must be valid");
        assert_eq!(parsed.schema_version, 1, "unsupported workload schema");
        assert!(parsed.theta.duplicate_percent <= 100);
        assert!(!parsed.theta.overlaps_percent.is_empty());
        assert!(
            parsed
                .theta
                .overlaps_percent
                .iter()
                .all(|percent| *percent <= 100)
        );
        assert!(parsed.theta.partitions >= 2);
        assert!(parsed.hll.downsample_lg_k < parsed.hll.lg_k);
        parsed
    })
}

pub fn workload_sha256() -> String {
    format!("{:x}", Sha256::digest(WORKLOAD_MANIFEST.as_bytes()))
}

#[inline]
pub fn deterministic_value(index: usize, seed: u64) -> u64 {
    let mut z = (index as u64)
        .wrapping_add(seed)
        .wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

pub fn unique_values(count: usize, seed: u64) -> Vec<u64> {
    (0..count)
        .map(|index| deterministic_value(index, seed))
        .collect()
}

pub fn duplicate_values(count: usize, duplicate_percent: usize, seed: u64) -> Vec<u64> {
    let distinct = count
        .saturating_mul(100usize.saturating_sub(duplicate_percent))
        .div_ceil(100)
        .max(1);
    (0..count)
        .map(|index| deterministic_value(index % distinct, seed))
        .collect()
}

pub fn byte_values(values: &[u64]) -> Vec<[u8; 8]> {
    values.iter().map(|value| value.to_le_bytes()).collect()
}

pub fn theta_from_u64(values: &[u64], lg_k: u8) -> CompactThetaSketch {
    let mut sketch = ThetaSketchBuilder::default().lg_k(lg_k).build().unwrap();
    for value in values {
        sketch.update(*value);
    }
    sketch.compact(true)
}

pub fn theta_from_bytes(values: &[[u8; 8]], lg_k: u8) -> CompactThetaSketch {
    let mut sketch = ThetaSketchBuilder::default().lg_k(lg_k).build().unwrap();
    for value in values {
        sketch.update(raw_bytes::from_slice(value));
    }
    sketch.compact(true)
}

pub fn theta_partitions(overlap_percent: usize) -> Vec<CompactThetaSketch> {
    let workload = workloads();
    let shared = workload.theta.partition_size * overlap_percent / 100;
    let unique = workload.theta.partition_size - shared;
    let mut result = Vec::with_capacity(workload.theta.partitions);

    for partition in 0..workload.theta.partitions {
        let mut sketch = ThetaSketchBuilder::default()
            .lg_k(workload.theta.lg_k)
            .build()
            .unwrap();
        for index in 0..shared {
            sketch.update(deterministic_value(index, workload.seed));
        }
        for index in 0..unique {
            let global_index = shared + partition * unique + index;
            sketch.update(deterministic_value(global_index, workload.seed));
        }
        result.push(sketch.compact(true));
    }
    result
}

pub fn theta_flat_union(sketches: &[CompactThetaSketch], lg_k: u8) -> CompactThetaSketch {
    let mut union = ThetaUnionBuilder::default().lg_k(lg_k).build().unwrap();
    for sketch in sketches {
        union.update(sketch).unwrap();
    }
    union.to_sketch(true)
}

pub fn theta_tree_union(sketches: &[CompactThetaSketch], lg_k: u8) -> CompactThetaSketch {
    assert!(!sketches.is_empty());
    let mut current = sketches.to_vec();
    while current.len() > 1 {
        let mut next = Vec::with_capacity(current.len().div_ceil(2));
        for pair in current.chunks(2) {
            let mut union = ThetaUnionBuilder::default().lg_k(lg_k).build().unwrap();
            for sketch in pair {
                union.update(sketch).unwrap();
            }
            next.push(union.to_sketch(true));
        }
        current = next;
    }
    current.pop().unwrap()
}

pub fn hll_from_values(values: &[u64], lg_k: u8, target: HllType) -> HllSketch {
    let mut sketch = HllSketch::new(lg_k, target).unwrap();
    for value in values {
        sketch.update(*value);
    }
    sketch
}

pub fn hll_union(sketches: &[HllSketch], lg_k: u8, target: HllType) -> HllSketch {
    let mut union = HllUnion::new(lg_k).unwrap();
    for sketch in sketches {
        union.update(sketch);
    }
    union.to_sketch(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_manifest_is_stable_and_valid() {
        let workload = workloads();
        assert_eq!(workload.schema_version, 1);
        assert_eq!(workload.theta.overlaps_percent, [0, 50, 95]);
        assert_eq!(
            workload_sha256(),
            "7d837245ac6f2ce080f472082f5af72b43a9eee45958a19905dcb31f58b6965b"
        );
    }

    #[test]
    fn deterministic_values_are_repeatable() {
        let workload = workloads();
        assert_eq!(
            unique_values(128, workload.seed),
            unique_values(128, workload.seed)
        );
        assert_ne!(
            unique_values(128, workload.seed),
            unique_values(128, workload.seed + 1)
        );
    }

    #[test]
    fn flat_and_tree_theta_union_cover_the_same_inputs() {
        let workload = workloads();
        for overlap in &workload.theta.overlaps_percent {
            let inputs = theta_partitions(*overlap);
            let flat = theta_flat_union(&inputs, workload.theta.lg_k);
            let tree = theta_tree_union(&inputs, workload.theta.lg_k);
            let relative_difference = (flat.estimate() - tree.estimate()).abs()
                / flat.estimate().max(tree.estimate()).max(1.0);
            assert!(relative_difference < 0.10);
        }
    }
}
