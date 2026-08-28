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

//! Frozen valid pre-LNP-9 protobuf fixtures.  Neither carrier has field 5/11.

use novarocks_proto_codec::ProtocolErrorKind;
use novarocks_proto_codec::lifecycle::ParticipantManifest;
use novarocks_proto_codec::membership::BackendProcessDescriptor;
use novarocks_proto_models::novarocks;
use prost::Message;

// BackendProcessDescriptor{process_id, endpoint, deployment_id, build_identity}.
const PRE_LNP9_DESCRIPTOR: &[u8] = &[
    0x0a, 0x12, 0x0a, 0x10, 0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97, 0x7b, 0x33, 0xd1,
    0x88, 0xad, 0x1f, 0x06, 0x12, 0x05, 0x0a, 0x01, 0x62, 0x10, 0x01, 0x1a, 0x01, 0x64, 0x22, 0x01,
    0x76,
];

// ParticipantManifest{execution_id, backend, expected ids, options, deadline,
// pre-start timeout, report endpoint}; the then-valid carrier lacks field 11.
const PRE_LNP9_MANIFEST: &[u8] = &[
    0x0a, 0x08, 0x0a, 0x04, 0x08, 0x05, 0x10, 0x06, 0x10, 0x01, 0x12, 0x1b, 0x12, 0x05, 0x0a, 0x01,
    0x62, 0x10, 0x01, 0x22, 0x12, 0x0a, 0x10, 0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97,
    0x7b, 0x33, 0xd1, 0x88, 0xad, 0x1f, 0x06, 0x22, 0x04, 0x08, 0x0b, 0x10, 0x0c, 0x2a, 0x00, 0x30,
    0xe8, 0x07, 0x48, 0xb0, 0xea, 0x01, 0x52, 0x05, 0x0a, 0x01, 0x62, 0x10, 0x01,
];

#[test]
fn pre_lnp9_descriptor_bytes_are_a_hard_cut() {
    let raw = novarocks::BackendProcessDescriptor::decode(PRE_LNP9_DESCRIPTOR)
        .expect("frozen pre-LNP-9 descriptor decodes");
    let error = BackendProcessDescriptor::parse(raw).expect_err("old descriptor must reject");
    assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
    assert_eq!(
        error.path().to_string(),
        "backend_process_descriptor.native_compatibility_id"
    );
}

#[test]
fn pre_lnp9_manifest_bytes_are_a_hard_cut() {
    let raw = novarocks::ParticipantManifest::decode(PRE_LNP9_MANIFEST)
        .expect("frozen pre-LNP-9 manifest decodes");
    let error = ParticipantManifest::parse(raw).expect_err("old manifest must reject");
    assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
    assert_eq!(
        error.path().to_string(),
        "participant_manifest.native_compatibility_id"
    );
}
