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

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use novarocks::common::types::UniqueId;
use novarocks::novarocks_logging::error;

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_submit_exec_batch_plan_fragments(ptr: *const u8, len: usize) -> i32 {
    if ptr.is_null() {
        return 2;
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match super::service::submit_exec_batch_plan_fragments(bytes) {
        Ok(_) => 0,
        Err(error) => {
            error!(
                target: "novarocks::ffi",
                error = %error,
                "submit_exec_batch_plan_fragments failed"
            );
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_submit_exec_plan_fragment(ptr: *const u8, len: usize) -> i32 {
    if ptr.is_null() {
        return 2;
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match super::service::submit_exec_plan_fragment(bytes) {
        Ok(()) => 0,
        Err(error) => {
            error!(
                target: "novarocks::ffi",
                error = %error,
                "submit_exec_plan_fragment failed"
            );
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_cancel(finst_id_hi: i64, finst_id_lo: i64) -> i32 {
    let finst_id = UniqueId {
        hi: finst_id_hi,
        lo: finst_id_lo,
    };
    super::service::cancel_fragment(finst_id);
    novarocks::cancel(finst_id);
    0
}

#[cfg(test)]
mod tests {
    use super::{
        novarocks_rs_cancel, novarocks_rs_submit_exec_batch_plan_fragments,
        novarocks_rs_submit_exec_plan_fragment,
    };

    #[test]
    fn null_fragment_payloads_keep_the_existing_invalid_argument_status() {
        assert_eq!(
            novarocks_rs_submit_exec_plan_fragment(std::ptr::null(), 0),
            2
        );
        assert_eq!(
            novarocks_rs_submit_exec_batch_plan_fragments(std::ptr::null(), 0),
            2
        );
        assert_eq!(novarocks_rs_cancel(0, 0), 0);
    }
}
