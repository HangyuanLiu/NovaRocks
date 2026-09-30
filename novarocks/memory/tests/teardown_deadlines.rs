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
fn deadline_and_terminal_do_not_fake_actual_io_exit() {
    let a = authority(32_768);
    let q = work(&a);
    let d = q.create_domain(1_024).unwrap();
    let mut l = d.activate(1_024, 0).unwrap();
    let o = l.record_allocation(512);
    l.finish();
    let io = [IoExitEvidence {
        owner: 7,
        cancel_started_ns: 10,
        exit_bound_ns: 10,
        max_inflight: 2,
        actually_exited: false,
    }];
    let e = TeardownEvidence {
        tasks_exited: true,
        operators_destroyed: true,
        io: &io,
        now_ns: 20,
    };
    assert_eq!(
        q.retire(&e),
        Err(TeardownError::TeardownDeadlineExceeded { owner: 7 })
    );
    assert!(!q.is_retired());
    assert!(!d.snapshot().residual);
    free(o, 512);
    assert!(q.retire(&exited()).is_ok());
}
