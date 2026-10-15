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

//! The resident length/range author is shared; bounded validation changes
//! neither canonical UTF8 acceptance nor original codec refusal categories.
use super::*;

struct Running;
impl KernelEvaluationControl for Running {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("membership probing never waits")
    }
}

#[test]
fn membership_controlled_leaf_utf8_window_carry_accepts_complete_multibyte_values() {
    for prefix in [253, 254, 255, 256] {
        for suffix in ["é", "海", "🦀", "é海🦀tail"] {
            let text = format!("{}{}", "x".repeat(prefix), suffix);
            assert_eq!(validate_utf8_controlled(text.as_bytes(), &Running), Ok(()));
        }
    }
    assert_eq!(validate_utf8_controlled(b"", &Running), Ok(()));
}

#[test]
fn membership_controlled_leaf_preserves_malformed_truncated_and_wrong_probe_errors() {
    for malformed in [
        vec![0xc0, 0xaf],
        vec![0xed, 0xa0, 0x80],
        vec![0xf0, 0x9f],
        vec![0xff],
    ] {
        for prefix in [0, 255, 256] {
            let mut value = vec![b'x'; prefix];
            value.extend_from_slice(&malformed);
            let mut encoded = u64::try_from(value.len()).unwrap().to_be_bytes().to_vec();
            encoded.extend_from_slice(&value);
            let index = ResidentMembershipIndex::Utf8 {
                payload: 0..encoded.len(),
                length_offsets: Box::from([0]),
            };
            assert_eq!(
                indexed_membership_contains(&encoded, &index, MembershipProbe::Utf8("x")),
                Err(ArtifactCodecError::NonCanonicalPayload)
            );
            assert_eq!(
                indexed_membership_contains_controlled(
                    &encoded,
                    &index,
                    MembershipProbe::Utf8("x"),
                    &Running
                ),
                Err(ControlledMembershipProbeError::Codec(
                    ArtifactCodecError::NonCanonicalPayload
                ))
            );
        }
    }
    let index = ResidentMembershipIndex::Utf8 {
        payload: 0..9,
        length_offsets: Box::from([0]),
    };
    let mut truncated = 2_u64.to_be_bytes().to_vec();
    truncated.push(b'x');
    assert_eq!(
        indexed_membership_contains(&truncated, &index, MembershipProbe::Utf8("x")),
        Err(ArtifactCodecError::Truncated)
    );
    assert_eq!(
        indexed_membership_contains_controlled(
            &truncated,
            &index,
            MembershipProbe::Utf8("x"),
            &Running
        ),
        Err(ControlledMembershipProbeError::Codec(
            ArtifactCodecError::Truncated
        ))
    );
    assert_eq!(
        indexed_membership_contains(&truncated, &index, MembershipProbe::Int64(1)),
        Err(ArtifactCodecError::ContractViolation)
    );
    assert_eq!(
        indexed_membership_contains_controlled(
            &truncated,
            &index,
            MembershipProbe::Int64(1),
            &Running
        ),
        Err(ControlledMembershipProbeError::Codec(
            ArtifactCodecError::ContractViolation
        ))
    );
}
