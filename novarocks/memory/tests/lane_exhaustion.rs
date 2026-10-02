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
use novarocks_memory::lane::{CoverageError, ResponsibilityClass, StoreHandle, global_store};

#[test]
fn real_process_record_exhaustion_is_a_coverage_error_without_funding_change() {
    let a = authority(65_536);
    let query = work(&a);
    let before = a.pressure_projection();
    let mut held = Vec::new();
    loop {
        match StoreHandle::Global.acquire(1, ResponsibilityClass::Service) {
            Ok(owner) => held.push(owner),
            Err(CoverageError::RecordStoreExhausted) => break,
            Err(error) => panic!("unexpected coverage failure: {error:?}"),
        }
    }
    assert_eq!(
        query.create_lane().unwrap_err(),
        CoverageError::RecordStoreExhausted
    );
    assert_eq!(a.pressure_projection(), before);
    drop(held);
    global_store().reclaim(usize::MAX);
    assert!(query.create_lane().is_ok());
}
