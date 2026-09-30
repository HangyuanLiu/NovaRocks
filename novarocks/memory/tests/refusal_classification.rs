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

mod common;
use common::*;
use novarocks_memory::*;
#[test]
fn query_impossible_closed_and_invalid_never_become_shared_shortage() {
    let a = authority(32_768);
    let q = work(&a);
    q.install_policy(2_048, LimitDimension::Work);
    let _existing = q.create_domain(1_024).unwrap();
    assert!(matches!(
        a.request_domain(&q, 900, 0),
        RequestOutcome::Refused(CapacityError::QueryLimit(_))
    ));
    assert!(matches!(
        a.request_domain(&q, 4_096, 0),
        RequestOutcome::Refused(CapacityError::ImpossibleRequest(_))
    ));
    q.close_to_growth();
    assert!(matches!(
        a.request_domain(&q, 1, 0),
        RequestOutcome::Refused(CapacityError::Closed { .. })
    ));
}
#[test]
fn metadata_backing_must_be_acquired_during_assembly() {
    let c = AuthorityConfig::new(1_024, 0, 0);
    assert!(matches!(
        MemoryAuthority::new(c),
        Err(ConfigError::CoreMetadataExceedsCapacity { .. })
    ));
}
