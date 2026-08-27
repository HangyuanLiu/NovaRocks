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

use std::time::Duration;

use novarocks_proto::lifecycle::{
    AttemptId, ParticipantBackendIdentity, ParticipantManifest, QueryControlEndpoint,
    QueryExecutionId, QueryInitRequest, QueryOptions, RuntimeFilterContribution,
};
use novarocks_proto_models::novarocks;
use novarocks_types::{BackendProcessId, QueryId};

fn request_with_runtime_filter() -> QueryInitRequest {
    let execution_id = QueryExecutionId::new(
        QueryId::new(41, 42),
        AttemptId::new(7).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let contribution = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
        participant_id: 3,
        ..Default::default()
    })
    .expect("valid contribution");
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            BackendProcessId::new_v7(),
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
        )
        .expect("valid backend"),
        [],
        QueryOptions::parse(novarocks::QueryOptions::default()).expect("valid query options"),
        10_000,
        [],
        Some(contribution),
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid manifest");
    QueryInitRequest::from_manifest(manifest)
}

#[test]
fn sender_and_receiver_derive_the_same_manifest_identity() {
    let request = request_with_runtime_filter();
    let expected = request
        .manifest()
        .expect("request manifest")
        .digest()
        .expect("request digest");

    let decoded =
        QueryInitRequest::parse(request.as_proto().clone()).expect("canonical request decodes");

    assert_eq!(
        decoded.manifest().expect("decoded manifest"),
        request.manifest().expect("request manifest")
    );
    assert_eq!(
        decoded
            .manifest()
            .expect("decoded manifest")
            .digest()
            .expect("decoded digest"),
        expected,
        "the identity is derived from content, not carried by the request"
    );
}

#[test]
fn participant_manifest_digest_covers_the_nested_runtime_filter_contribution() {
    let request = request_with_runtime_filter();
    let baseline = request
        .manifest()
        .expect("manifest")
        .digest()
        .expect("baseline digest");

    let mut wire = request.as_proto().clone();
    wire.manifest
        .as_mut()
        .expect("manifest")
        .runtime_filter
        .as_mut()
        .expect("runtime filter contribution")
        .participant_id += 1;

    let mutated = QueryInitRequest::parse(wire)
        .expect("a structurally valid request still decodes")
        .manifest()
        .expect("mutated manifest")
        .digest()
        .expect("mutated digest");

    assert_ne!(
        baseline, mutated,
        "descriptor traversal must reach the nested contribution"
    );
}
