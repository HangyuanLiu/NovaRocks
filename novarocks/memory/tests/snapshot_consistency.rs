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
#[test]
fn hierarchy_snapshot_decomposes_each_byte_once() {
    let a = authority(32_768);
    let q = work(&a);
    q.prefund(8_192).unwrap();
    let d = q.create_domain(2_048).unwrap();
    let mut l = d.activate(2_048, 0).unwrap();
    let p = l.record_allocation(1_024);
    l.finish();
    assert!(q.snapshot().is_internally_consistent());
    assert!(a.snapshot().root.is_internally_consistent());
    free(p, 1_024);
}
