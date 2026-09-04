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

use std::collections::BTreeSet;
use std::fs;

use datasketches::hash::value::natural_extend;
use datasketches::hll::{HllSketch, HllType};
use datasketches::theta::{CompactThetaSketch, ThetaSketchBuilder};

use super::common::{
    CUSTOM_SEED, cases, digest, expected_cardinality, fact, fixture, fixture_root,
};

#[test]
fn manifest_covers_every_checked_in_fixture_and_records_exact_provenance() {
    let cases = cases();
    assert_eq!(cases.len(), 50);

    let manifest_paths: BTreeSet<_> = cases.iter().map(|case| case.path.to_owned()).collect();
    let mut disk_paths = BTreeSet::new();
    for family in ["theta", "hll"] {
        for entry in fs::read_dir(fixture_root().join(family)).expect("fixture family") {
            let path = entry.expect("fixture entry").path();
            if path.extension().is_some_and(|extension| extension == "sk") {
                disk_paths.insert(format!(
                    "{family}/{}",
                    path.file_name().unwrap().to_string_lossy()
                ));
            }
        }
    }
    assert_eq!(manifest_paths, disk_paths);

    for case in cases {
        match case.producer {
            "java" => {
                assert_eq!(case.producer_version, "6.2.0");
                assert_eq!(
                    case.producer_commit,
                    "9ca65f12b7bdde9b424f27be1d16f2f9dc365a7a"
                );
            }
            "cpp" => {
                assert_eq!(case.producer_version, "fe0261a");
                assert_eq!(
                    case.producer_commit,
                    "fe0261aa043c1d3af9a92a62fa286caabbf6fa84"
                );
            }
            "rust" => {
                assert_eq!(case.producer_version, "0.5.0-rc.1");
                assert_eq!(
                    case.producer_commit,
                    "77f5652016b3859c23b60c5b8b9e94578ef484f0"
                );
            }
            producer => panic!("{} unknown producer {producer}", case.path),
        }
        assert!(!case.profile.is_empty());
        assert!(!case.input.is_empty());
        assert!(!case.serialization.is_empty());
        assert!(case.seed == "-" || case.seed.parse::<u64>().is_ok());
        assert_eq!(case.bytes_sha256.len(), 64);
        assert_eq!(
            digest(&fixture(case.path)),
            case.bytes_sha256,
            "{} digest",
            case.path
        );
    }
}

#[test]
fn canonical_outputs_are_byte_exact_only_where_the_format_promises_it() {
    let java_v4 = fixture("theta/java62_quickselect_n100000_ordered_v4.sk");
    let cpp_v4 = fixture("theta/theta_compressed_n100000_cpp.sk");
    let rust_v4 = fixture("theta/rust_quickselect_n100000_ordered_v4.sk");
    assert_eq!(java_v4, cpp_v4);
    assert_eq!(java_v4, rust_v4);

    for hll_type in ["hll4", "hll6", "hll8"] {
        let java_name = hll_type.replace("hll", "hll-");
        let java = fixture(&format!("hll/java62_{java_name}_lgk12_n10.sk"));
        let cpp = fixture(&format!("hll/{hll_type}_n10_cpp.sk"));
        assert_eq!(java, cpp, "canonical sparse {hll_type}");
    }
}

#[test]
fn checked_in_rust_fixtures_match_rc1_generation() {
    let mut theta = ThetaSketchBuilder::default()
        .lg_k(12)
        .build()
        .expect("theta");
    for value in 0_u32..1000 {
        theta.update(natural_extend::from_u32(value));
    }
    assert_eq!(
        theta.compact(true).serialize(),
        fixture("theta/rust_quickselect_n1000_ordered_v3.sk")
    );

    for (hll_type, name) in [
        (HllType::Hll4, "hll4"),
        (HllType::Hll6, "hll6"),
        (HllType::Hll8, "hll8"),
    ] {
        let mut hll = HllSketch::new(12, hll_type).expect("HLL");
        for value in 0_u32..10_000 {
            hll.update(natural_extend::from_u32(value));
        }
        assert_eq!(
            hll.serialize(),
            fixture(&format!("hll/rust_{name}_n10000.sk"))
        );
    }
}

#[test]
fn theta_fixtures_match_recorded_cross_language_facts() {
    for case in cases()
        .into_iter()
        .filter(|case| case.path.starts_with("theta/"))
    {
        let bytes = fixture(case.path);
        let sketch = if case.seed == CUSTOM_SEED.to_string() {
            CompactThetaSketch::deserialize_with_seed(&bytes, CUSTOM_SEED)
        } else {
            CompactThetaSketch::deserialize(&bytes)
        }
        .unwrap_or_else(|error| panic!("deserialize {}: {error}", case.path));

        assert_eq!(
            sketch.is_empty().to_string(),
            fact(&case, "empty"),
            "{} empty",
            case.path
        );
        assert_eq!(
            sketch.is_estimation_mode().to_string(),
            fact(&case, "estimation"),
            "{} estimation",
            case.path
        );
        assert_eq!(
            sketch.is_ordered().to_string(),
            fact(&case, "ordered"),
            "{} ordered",
            case.path
        );
        assert_eq!(
            sketch.seed_hash().to_string(),
            fact(&case, "seed_hash"),
            "{} seed",
            case.path
        );
        assert_eq!(
            sketch.num_retained().to_string(),
            fact(&case, "retained"),
            "{} retained",
            case.path
        );
        assert_eq!(
            sketch.theta64().to_string(),
            fact(&case, "theta64"),
            "{} theta",
            case.path
        );
        assert_eq!(
            sketch.estimate().to_bits(),
            fact(&case, "estimate").parse::<f64>().unwrap().to_bits()
        );

        let mut hashes: Vec<_> = sketch.iter().map(|entry| entry.hash()).collect();
        hashes.sort_unstable();
        let encoded: Vec<_> = hashes.iter().flat_map(|hash| hash.to_le_bytes()).collect();
        assert_eq!(
            digest(&encoded),
            fact(&case, "hashes_sha256"),
            "{} hashes",
            case.path
        );

        let expected = expected_cardinality(case.input).expect("known deterministic input") as f64;
        if expected <= 4097.0 {
            assert_eq!(sketch.estimate(), expected, "{} exact", case.path);
        } else {
            let relative_error = (sketch.estimate() - expected).abs() / expected;
            assert!(
                relative_error < 0.06,
                "{} relative error {relative_error}",
                case.path
            );
        }
    }
}

#[test]
fn hll_fixtures_match_recorded_cross_language_facts() {
    for case in cases()
        .into_iter()
        .filter(|case| case.path.starts_with("hll/"))
    {
        let bytes = fixture(case.path);
        let sketch = HllSketch::deserialize(&bytes)
            .unwrap_or_else(|error| panic!("deserialize {}: {error}", case.path));
        assert_eq!(
            format!("{:?}", sketch.target_type()),
            fact(&case, "type"),
            "{} type",
            case.path
        );
        assert_eq!(sketch.lg_config_k(), case.lg_k, "{} lg_k", case.path);
        assert_eq!(sketch.lg_config_k().to_string(), fact(&case, "lg_k"));
        assert_eq!(
            sketch.estimate().to_bits(),
            fact(&case, "estimate").parse::<f64>().unwrap().to_bits()
        );
        let expected = expected_cardinality(case.input).expect("known deterministic input") as f64;
        let relative_error = (sketch.estimate() - expected).abs() / expected.max(1.0);
        assert!(
            relative_error < 0.06,
            "{} relative error {relative_error}",
            case.path
        );
    }
}
