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

use datasketches::hash::value::raw_bytes;
use datasketches::hll::{HllSketch, HllType, HllUnion};
use datasketches::theta::{ThetaANotB, ThetaIntersection, ThetaSketchBuilder};
use divan::counter::ItemsCount;
use divan::{AllocProfiler, Bencher, Divan, black_box};
use novarocks_datasketches_bench::{
    byte_values, duplicate_values, hll_from_values, theta_flat_union, theta_from_u64,
    theta_partitions, theta_tree_union, unique_values, workloads,
};

#[global_allocator]
static ALLOCATOR: AllocProfiler = AllocProfiler::system();

fn main() {
    Divan::from_args()
        .sample_count(workloads().measurement.sample_count as u32)
        .main();
}

#[divan::bench]
fn theta_update_unique_u64(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.theta.updates, workload.seed);
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut sketch = ThetaSketchBuilder::default()
                .lg_k(workload.theta.lg_k)
                .build()
                .unwrap();
            for value in &values {
                sketch.update(black_box(*value));
            }
            black_box(sketch)
        });
}

#[divan::bench]
fn theta_update_unique_bytes(bencher: Bencher) {
    let workload = workloads();
    let values = byte_values(&unique_values(workload.theta.updates, workload.seed));
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut sketch = ThetaSketchBuilder::default()
                .lg_k(workload.theta.lg_k)
                .build()
                .unwrap();
            for value in &values {
                sketch.update(raw_bytes::from_slice(black_box(value)));
            }
            black_box(sketch)
        });
}

#[divan::bench]
fn theta_update_ninety_percent_duplicates(bencher: Bencher) {
    let workload = workloads();
    let values = duplicate_values(
        workload.theta.updates,
        workload.theta.duplicate_percent,
        workload.seed,
    );
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut sketch = ThetaSketchBuilder::default()
                .lg_k(workload.theta.lg_k)
                .build()
                .unwrap();
            for value in &values {
                sketch.update(black_box(*value));
            }
            black_box(sketch)
        });
}

#[divan::bench]
fn theta_exact_to_estimation_transition(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(1usize << (workload.theta.lg_k + 2), workload.seed);
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut sketch = ThetaSketchBuilder::default()
                .lg_k(workload.theta.lg_k)
                .build()
                .unwrap();
            for value in &values {
                sketch.update(black_box(*value));
            }
            assert!(sketch.is_estimation_mode());
            black_box(sketch)
        });
}

#[divan::bench(args = [false, true])]
fn theta_compact(bencher: Bencher, ordered: bool) {
    let workload = workloads();
    let values = unique_values(workload.theta.updates, workload.seed);
    let mut sketch = ThetaSketchBuilder::default()
        .lg_k(workload.theta.lg_k)
        .build()
        .unwrap();
    for value in values {
        sketch.update(value);
    }
    bencher.bench_local(|| black_box(&sketch).compact(black_box(ordered)));
}

#[divan::bench(args = [false, true])]
fn theta_serialize(bencher: Bencher, compressed: bool) {
    let workload = workloads();
    let values = unique_values(workload.theta.updates, workload.seed);
    let compact = theta_from_u64(&values, workload.theta.lg_k);
    bencher.bench_local(|| {
        if compressed {
            black_box(&compact).serialize_compressed()
        } else {
            black_box(&compact).serialize()
        }
    });
}

#[divan::bench(args = [false, true])]
fn theta_deserialize(bencher: Bencher, compressed: bool) {
    let workload = workloads();
    let values = unique_values(workload.theta.updates, workload.seed);
    let compact = theta_from_u64(&values, workload.theta.lg_k);
    let bytes = if compressed {
        compact.serialize_compressed()
    } else {
        compact.serialize()
    };
    bencher.bench_local(|| {
        datasketches::theta::CompactThetaSketch::deserialize(black_box(&bytes)).unwrap()
    });
}

#[divan::bench(args = [0, 50, 95])]
fn theta_union_flat(bencher: Bencher, overlap_percent: usize) {
    let workload = workloads();
    let inputs = theta_partitions(overlap_percent);
    bencher
        .counter(ItemsCount::new(
            workload.theta.partitions * workload.theta.partition_size,
        ))
        .bench_local(|| theta_flat_union(black_box(&inputs), workload.theta.lg_k));
}

#[divan::bench(args = [0, 50, 95])]
fn theta_union_tree(bencher: Bencher, overlap_percent: usize) {
    let workload = workloads();
    let inputs = theta_partitions(overlap_percent);
    bencher
        .counter(ItemsCount::new(
            workload.theta.partitions * workload.theta.partition_size,
        ))
        .bench_local(|| theta_tree_union(black_box(&inputs), workload.theta.lg_k));
}

#[divan::bench]
fn theta_intersection(bencher: Bencher) {
    let inputs = theta_partitions(50);
    bencher.bench_local(|| {
        let mut intersection = ThetaIntersection::default();
        for input in black_box(&inputs) {
            intersection.update(input).unwrap();
        }
        black_box(intersection.to_sketch(true).unwrap())
    });
}

#[divan::bench]
fn theta_a_not_b(bencher: Bencher) {
    let inputs = theta_partitions(50);
    let operation = ThetaANotB::default();
    bencher.bench_local(|| {
        operation
            .compute(black_box(&inputs[0]), black_box(&inputs[1]), true)
            .unwrap()
    });
}

fn bench_hll_update(bencher: Bencher, target: HllType) {
    let workload = workloads();
    let values = unique_values(workload.hll.updates, workload.seed);
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut sketch = HllSketch::new(workload.hll.lg_k, target).unwrap();
            for value in &values {
                sketch.update(black_box(*value));
            }
            black_box(sketch)
        });
}

#[divan::bench]
fn hll4_update(bencher: Bencher) {
    bench_hll_update(bencher, HllType::Hll4);
}

#[divan::bench]
fn hll6_update(bencher: Bencher) {
    bench_hll_update(bencher, HllType::Hll6);
}

#[divan::bench]
fn hll8_update(bencher: Bencher) {
    bench_hll_update(bencher, HllType::Hll8);
}

#[divan::bench]
fn hll_serialize(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.hll.updates, workload.seed);
    let sketch = hll_from_values(&values, workload.hll.lg_k, HllType::Hll4);
    bencher.bench_local(|| black_box(&sketch).serialize());
}

#[divan::bench]
fn hll_deserialize(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.hll.updates, workload.seed);
    let bytes = hll_from_values(&values, workload.hll.lg_k, HllType::Hll4).serialize();
    bencher.bench_local(|| HllSketch::deserialize(black_box(&bytes)).unwrap());
}

#[divan::bench]
fn hll_same_type_union(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.hll.updates, workload.seed);
    let mid = values.len() / 2;
    let inputs = [
        hll_from_values(&values[..mid], workload.hll.lg_k, HllType::Hll4),
        hll_from_values(&values[mid..], workload.hll.lg_k, HllType::Hll4),
    ];
    bencher.bench_local(|| {
        let mut union = HllUnion::new(workload.hll.lg_k).unwrap();
        for input in black_box(&inputs) {
            union.update(input);
        }
        black_box(union.to_sketch(HllType::Hll4))
    });
}

#[divan::bench]
fn hll_mixed_type_union(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.hll.updates, workload.seed);
    let third = values.len() / 3;
    let inputs = [
        hll_from_values(&values[..third], workload.hll.lg_k, HllType::Hll4),
        hll_from_values(&values[third..2 * third], workload.hll.lg_k, HllType::Hll6),
        hll_from_values(&values[2 * third..], workload.hll.lg_k, HllType::Hll8),
    ];
    bencher.bench_local(|| {
        let mut union = HllUnion::new(workload.hll.lg_k).unwrap();
        for input in black_box(&inputs) {
            union.update(input);
        }
        black_box(union.to_sketch(HllType::Hll4))
    });
}

#[divan::bench]
fn hll_sparse_to_dense(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.hll.dense_updates, workload.seed);
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut sketch = HllSketch::new(workload.hll.lg_k, HllType::Hll8).unwrap();
            for value in &values {
                sketch.update(black_box(*value));
            }
            black_box(sketch)
        });
}

#[divan::bench]
fn hll_downsample_union(bencher: Bencher) {
    let workload = workloads();
    let values = unique_values(workload.hll.updates, workload.seed);
    let high = hll_from_values(&values, workload.hll.lg_k, HllType::Hll8);
    let low = hll_from_values(&values, workload.hll.downsample_lg_k, HllType::Hll6);
    bencher.bench_local(|| {
        let mut union = HllUnion::new(workload.hll.lg_k).unwrap();
        union.update(black_box(&high));
        union.update(black_box(&low));
        black_box(union.to_sketch(HllType::Hll8))
    });
}

#[divan::bench(args = [7, 8, 25, 512, 4096, 16384])]
fn hll_estimated_size_is_constant_work(bencher: Bencher, input_count: usize) {
    let workload = workloads();
    let values = unique_values(input_count, workload.seed);
    let sketch = hll_from_values(&values, workload.hll.lg_k, HllType::Hll4);
    bencher.bench_local(|| black_box(&sketch).estimated_size());
}

#[divan::bench(args = [7, 8, 25, 512, 4096, 16384])]
fn hll_union_estimated_size_is_constant_work(bencher: Bencher, input_count: usize) {
    let workload = workloads();
    let values = unique_values(input_count, workload.seed);
    let mut union = HllUnion::new(workload.hll.lg_k).unwrap();
    for value in values {
        union.update_value(value);
    }
    bencher.bench_local(|| black_box(&union).estimated_size());
}
