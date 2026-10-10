// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
#[test]
fn caller_property_known_five_axes_and_arithmetic_use_original_layouts() {
    let input = hash();
    let raw = expected_hash();
    let encoded = properties_encode_numerical_facts_in(&input, SOURCE).unwrap();
    let decoded = properties_decode_numerical_facts_in(&raw, SOURCE).unwrap();
    assert_eq!(
        encoded,
        properties_encode_numerical_facts(&input, SOURCE).unwrap()
    );
    assert_eq!(
        decoded,
        properties_decode_numerical_facts(&raw, SOURCE).unwrap()
    );
    for (f, copies, element) in [
        (
            encoded,
            1,
            Layout::array::<u32>(3).unwrap().size()
                + Layout::array::<wire::OrderingKey>(3).unwrap().size()
                + 64,
        ),
        (
            decoded,
            2,
            Layout::array::<physical::ValueId>(3).unwrap().size()
                + Layout::array::<physical::OrderingKey>(3).unwrap().size(),
        ),
    ] {
        assert_eq!(f.value_reference_count, 6);
        assert_eq!(f.allocation_request_bytes_upper_bound, copies * element);
        let exact = exact_limits(f);
        check_properties_numerical_facts(f, exact).unwrap();
        for axis in 0..5 {
            let mut under = exact;
            reduce(&mut under, axis);
            assert!(matches!(
                check_properties_numerical_facts(f, under),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    }
    assert!(matches!(
        properties_encode_numerical_facts_in(&input, usize::MAX),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert!(matches!(
        properties_decode_numerical_facts_in(&raw, usize::MAX),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert!(matches!(
        properties_encode_numerical_facts(&input, usize::MAX),
        Err(Error::InvalidShape(_))
    ));
    assert!(matches!(
        properties_decode_numerical_facts(&raw, usize::MAX),
        Err(Error::InvalidShape(_))
    ));
}
#[test]
fn caller_property_source_presence_errors_stay_ordinary_and_distribution_has_same_author() {
    let input = hash();
    let raw = expected_hash();
    assert!(matches!(
        properties_encode_numerical_facts_in(&input, 0),
        Err(Error::InvalidShape(
            "physical property source invoice omits original backing"
        ))
    ));
    assert!(matches!(
        properties_decode_numerical_facts_in(&raw, 0),
        Err(Error::InvalidShape(
            "physical property source invoice omits original backing"
        ))
    ));
    let mut missing = raw.clone();
    missing.distribution = None;
    assert!(matches!(
        properties_decode_numerical_facts_in(&missing, SOURCE),
        Err(Error::InvalidShape(
            "physical property distribution is absent"
        ))
    ));
    let distribution = raw.distribution.as_ref().unwrap();
    assert_eq!(
        distribution_encode_numerical_facts_in(&input.distribution, SOURCE).unwrap(),
        distribution_encode_numerical_facts(&input.distribution, SOURCE).unwrap()
    );
    assert_eq!(
        distribution_decode_numerical_facts_in(distribution, SOURCE).unwrap(),
        distribution_decode_numerical_facts(distribution, SOURCE).unwrap()
    );
    assert!(matches!(
        distribution_encode_numerical_facts_in(&input.distribution, usize::MAX),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert!(matches!(
        distribution_decode_numerical_facts_in(distribution, usize::MAX),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
}
