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

use datasketches::hash::value::natural_extend;
use datasketches::theta::{
    CompactThetaSketch, ThetaANotB, ThetaIntersection, ThetaSketchBuilder, ThetaUnionBuilder,
};

use super::common::fixture;

fn compact(path: &str) -> CompactThetaSketch {
    CompactThetaSketch::deserialize(&fixture(path)).expect("external compact fixture")
}

fn mutable(start: u32, end: u32) -> datasketches::theta::ThetaSketch {
    let mut sketch = ThetaSketchBuilder::default()
        .lg_k(12)
        .build()
        .expect("theta builder");
    for value in start..end {
        sketch.update(natural_extend::from_u32(value));
    }
    sketch
}

#[test]
fn external_compacts_cover_union_intersection_and_a_not_b() {
    let left = compact("theta/java62_quickselect_overlap_left_ordered_v3.sk");
    let right = compact("theta/java62_quickselect_overlap_right_ordered_v3.sk");
    let disjoint = compact("theta/java62_quickselect_disjoint_right_ordered_v3.sk");
    let left_before = left.serialize();
    let right_before = right.serialize();

    let mut union = ThetaUnionBuilder::default()
        .lg_k(12)
        .build()
        .expect("union");
    union.update(&left).expect("left");
    union.update(&right).expect("right");
    assert_eq!(union.to_sketch(true).estimate(), 1100.0);

    let mut intersection = ThetaIntersection::default();
    intersection.update(&left).expect("left");
    intersection.update(&right).expect("right");
    assert_eq!(
        intersection.to_sketch(true).expect("result").estimate(),
        900.0
    );

    let a_not_b = ThetaANotB::default();
    assert_eq!(
        a_not_b
            .compute(&left, &right, true)
            .expect("a-not-b")
            .estimate(),
        100.0
    );
    assert_eq!(
        a_not_b
            .compute(&left, &disjoint, true)
            .expect("disjoint")
            .estimate(),
        1000.0
    );
    assert_eq!(
        left.serialize(),
        left_before,
        "set operations must not mutate left input"
    );
    assert_eq!(
        right.serialize(),
        right_before,
        "set operations must not mutate right input"
    );
}

#[test]
fn mutable_and_compact_combinations_have_the_same_set_semantics() {
    let left_mutable = mutable(0, 1000);
    let right_mutable = mutable(100, 1100);
    let right_compact = compact("theta/java62_quickselect_overlap_right_ordered_v3.sk");
    let disjoint_compact = compact("theta/java62_quickselect_disjoint_right_ordered_v3.sk");

    let mut flat = ThetaUnionBuilder::default()
        .lg_k(12)
        .build()
        .expect("flat union");
    flat.update(&left_mutable).expect("mutable");
    flat.update(&right_compact).expect("compact");
    let flat = flat.to_sketch(true);
    assert_eq!(flat.estimate(), 1100.0);

    let mut tree = ThetaUnionBuilder::default()
        .lg_k(12)
        .build()
        .expect("tree union");
    tree.update(&flat).expect("compact intermediate");
    tree.update(&disjoint_compact).expect("compact disjoint");
    assert_eq!(tree.to_sketch(true).estimate(), 2100.0);

    let mut intersection = ThetaIntersection::default();
    intersection.update(&left_mutable).expect("mutable");
    intersection.update(&right_compact).expect("compact");
    assert_eq!(
        intersection.to_sketch(true).expect("result").estimate(),
        900.0
    );

    let a_not_b = ThetaANotB::default();
    assert_eq!(
        a_not_b
            .compute(&left_mutable, &right_compact, true)
            .expect("mixed")
            .estimate(),
        100.0
    );
    assert_eq!(
        a_not_b
            .compute(&right_compact, &left_mutable, true)
            .expect("reverse")
            .estimate(),
        100.0
    );
    assert_eq!(
        a_not_b
            .compute(&left_mutable, &right_mutable, true)
            .expect("mutable")
            .estimate(),
        100.0
    );
}

#[test]
fn alpha_compact_is_an_opaque_valid_input_not_a_rust_producer_claim() {
    let alpha = compact("theta/java62_alpha_n100000_ordered_v3.sk");
    let quickselect = compact("theta/java62_quickselect_n100000_ordered_v3.sk");
    assert!(alpha.is_estimation_mode());
    assert!(quickselect.is_estimation_mode());
    assert_ne!(
        alpha.theta64(),
        quickselect.theta64(),
        "producer trajectories may differ"
    );

    let mut union = ThetaUnionBuilder::default()
        .lg_k(12)
        .build()
        .expect("union");
    union.update(&alpha).expect("alpha compact is readable");
    union
        .update(&quickselect)
        .expect("quickselect compact is readable");
    let estimate = union.to_sketch(true).estimate();
    assert!(
        (90_000.0..=110_000.0).contains(&estimate),
        "union estimate {estimate}"
    );
}
