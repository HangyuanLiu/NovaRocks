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

use std::fs;
use std::path::Path;

#[test]
fn rfd5b_native_encoder_preserves_project_filter_aggregate_union_exchange_scan_values_attachments()
{
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/protocol/native/encode/plan.rs"),
    )
    .expect("read native plan encoder");
    let body = source
        .split("pub(super) fn encode_node_with_context")
        .nth(1)
        .expect("node encoder")
        .split("fn apply_sealed_node_output_columns")
        .next()
        .expect("node encoder body");
    assert!(
        body.contains(
            "runtime_filter_binding_ids: src\n            .runtime_filter_binding_ids\n            .iter()"
        ),
        "generic DistributedNode encoding must copy sealed binding ids directly for Project, Filter, Aggregate, Union, Exchange, Scan, and Values payloads"
    );
    assert_eq!(
        body.matches("runtime_filter_binding_ids").count(),
        2,
        "generic node encoding must neither derive nor filter Project, Filter, Aggregate, Union, Exchange, Scan, or Values attachments by node kind"
    );
}
