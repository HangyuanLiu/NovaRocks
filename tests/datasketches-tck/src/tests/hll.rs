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

use datasketches::hll::{HllSketch, HllType, HllUnion};

use super::common::fixture;

#[test]
fn known_legacy_payload_remains_readable() {
    let bytes = [
        0x02, 0x01, 0x07, 0x11, 0x03, 0x08, 0x01, 0x04, 0x3d, 0x9c, 0xf5, 0x1c,
    ];
    let sketch = HllSketch::deserialize(&bytes).expect("known HLL payload");
    assert_eq!(sketch.estimate().round() as u64, 1);
}

#[test]
fn java_hll4_hll6_hll8_union_and_target_round_trip() {
    for (name, hll_type) in [
        ("hll-4", HllType::Hll4),
        ("hll-6", HllType::Hll6),
        ("hll-8", HllType::Hll8),
    ] {
        let left = HllSketch::deserialize(&fixture(&format!("hll/java62_{name}_lgk12_n10000.sk")))
            .expect("left Java HLL");
        let right = HllSketch::deserialize(&fixture(&format!(
            "hll/{name_without_dash}_n100000_cpp.sk",
            name_without_dash = name.replace('-', "")
        )))
        .expect("right C++ HLL");
        let mut union = HllUnion::new(12).expect("HLL union");
        union.update(&left);
        union.update(&right);
        let result = union.to_sketch(hll_type);
        assert_eq!(result.target_type(), hll_type);
        assert_eq!(
            HllSketch::deserialize(&result.serialize())
                .expect("round trip")
                .target_type(),
            hll_type
        );
        assert!((90_000.0..=115_000.0).contains(&result.estimate()));
    }
}

#[test]
fn mixed_lg_k_union_downsamples_to_the_smaller_configuration() {
    let lg12 = HllSketch::deserialize(&fixture("hll/java62_hll-8_lgk12_n10000.sk")).expect("lg12");
    let lg10 = HllSketch::deserialize(&fixture("hll/java62_hll-8_lgk10_n10000.sk")).expect("lg10");
    let mut union = HllUnion::new(12).expect("union");
    union.update(&lg12);
    union.update(&lg10);
    let result = union.to_sketch(HllType::Hll8);
    assert_eq!(result.lg_config_k(), 10);
    assert!((8_500.0..=12_000.0).contains(&result.estimate()));
}
