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

use std::panic::{AssertUnwindSafe, catch_unwind};

use datasketches::hll::HllSketch;
use datasketches::theta::CompactThetaSketch;

use super::common::{CUSTOM_SEED, fixture};

fn rejects_theta(name: &str, bytes: &[u8]) {
    let outcome = catch_unwind(AssertUnwindSafe(|| CompactThetaSketch::deserialize(bytes)));
    assert!(outcome.is_ok(), "{name} panicked");
    assert!(outcome.unwrap().is_err(), "{name} was accepted");
}

fn rejects_hll(name: &str, bytes: &[u8]) {
    let outcome = catch_unwind(AssertUnwindSafe(|| HllSketch::deserialize(bytes)));
    assert!(outcome.is_ok(), "{name} panicked");
    assert!(outcome.unwrap().is_err(), "{name} was accepted");
}

#[test]
fn theta_malformed_corpus_fails_closed_without_panicking() {
    let valid = fixture("theta/java62_quickselect_n100000_ordered_v3.sk");
    for length in [0, 1, 2, 3, 7, 15, valid.len() - 1] {
        rejects_theta(&format!("truncated-{length}"), &valid[..length]);
    }

    let mut invalid_preamble = valid.clone();
    invalid_preamble[0] = 0;
    rejects_theta("invalid-preamble", &invalid_preamble);

    let mut invalid_family = valid.clone();
    invalid_family[2] = 7;
    rejects_theta("invalid-family", &invalid_family);

    let mut invalid_version = valid.clone();
    invalid_version[1] = 99;
    rejects_theta("invalid-version", &invalid_version);

    let custom_seed = fixture("theta/java62_quickselect_n1000_custom_seed_ordered_v3.sk");
    rejects_theta("seed-mismatch", &custom_seed);
    assert!(CompactThetaSketch::deserialize_with_seed(&custom_seed, CUSTOM_SEED).is_ok());

    let mut oversized_count = fixture("theta/java62_quickselect_n1000_unordered_v3.sk");
    oversized_count[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    rejects_theta("oversized-count", &oversized_count);

    let mut compressed_width_overflow = fixture("theta/java62_quickselect_n100000_ordered_v4.sk");
    compressed_width_overflow[3] = 64;
    rejects_theta("compressed-entry-width", &compressed_width_overflow);

    let mut compressed_count_overflow = fixture("theta/java62_quickselect_n100000_ordered_v4.sk");
    compressed_count_overflow[4] = 5;
    rejects_theta("compressed-count-width", &compressed_count_overflow);
}

#[test]
fn hll_malformed_corpus_fails_closed_without_panicking() {
    let valid = fixture("hll/java62_hll-8_lgk12_n10000.sk");
    for length in [0, 1, 2, 3, 7, 39, valid.len() - 1] {
        rejects_hll(&format!("truncated-{length}"), &valid[..length]);
    }

    let mut invalid_preamble = valid.clone();
    invalid_preamble[0] = 0;
    rejects_hll("invalid-preamble", &invalid_preamble);

    let mut invalid_family = valid.clone();
    invalid_family[2] = 3;
    rejects_hll("invalid-family", &invalid_family);

    let mut invalid_version = valid.clone();
    invalid_version[1] = 99;
    rejects_hll("invalid-version", &invalid_version);

    let mut contradictory_empty = fixture("hll/java62_hll-8_lgk12_n1.sk");
    contradictory_empty[5] |= 4;
    rejects_hll("empty-flag-with-coupon", &contradictory_empty);

    let mut oversized_count = fixture("hll/java62_hll-8_lgk12_n10.sk");
    oversized_count[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    rejects_hll("oversized-set-count", &oversized_count);
}

#[test]
fn trailing_backing_capacity_and_reserved_flags_follow_official_tolerant_semantics() {
    let mut theta = fixture("theta/java62_quickselect_n1000_unordered_v3.sk");
    theta.extend_from_slice(&[0xa5; 16]);
    theta[5] |= 0x80;
    assert!(CompactThetaSketch::deserialize(&theta).is_ok());

    let mut hll = fixture("hll/java62_hll-8_lgk12_n10000.sk");
    hll.extend_from_slice(&[0xa5; 16]);
    hll[5] |= 0x80;
    assert!(HllSketch::deserialize(&hll).is_ok());
}
