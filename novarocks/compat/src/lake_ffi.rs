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

//! C ABI adapters for compat-owned lake storage operations.
//!
//! These symbols intentionally keep their historic names.  The C++ BRPC
//! bridge passes an opaque, host-owned service context as their first
//! parameter; it remains responsible for freeing both returned buffers using
//! the compat-owned buffer free function.

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::c_void;

use crate::proto::starrocks::{
    AbortCompactionRequest, AbortTxnRequest, CompactRequest, DeleteDataRequest,
    DeleteTabletRequest, DropTableRequest, PublishLogVersionBatchRequest, PublishLogVersionRequest,
    PublishVersionRequest, TabletStatRequest, VacuumRequest,
};
use novarocks::novarocks_logging::error;
use prost::Message;

use crate::ffi_support::NovaRocksRustBuf;
use crate::lake_storage::{CompatLakeStorageService, LakeWireResult, decode_lake_request};

fn reset_output(buffer: *mut NovaRocksRustBuf) {
    if buffer.is_null() {
        return;
    }
    // SAFETY: the C ABI requires non-null output buffers to point to writable
    // NovaRocksRustBuf storage for the duration of this call.
    unsafe {
        (*buffer).ptr = std::ptr::null_mut();
        (*buffer).len = 0;
    }
}

fn write_bytes(buffer: *mut NovaRocksRustBuf, bytes: Vec<u8>) {
    if buffer.is_null() {
        return;
    }
    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()).cast::<u8>();
    // SAFETY: the caller supplied writable output storage, checked above.
    unsafe {
        (*buffer).ptr = ptr;
        (*buffer).len = len;
    }
}

fn write_error(buffer: *mut NovaRocksRustBuf, message: String) {
    write_bytes(buffer, message.into_bytes());
}

fn execute_lake_ffi<Request, Response>(
    context: *const c_void,
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
    operation: &str,
    execute: impl FnOnce(&CompatLakeStorageService, &Request) -> Result<Response, String>,
) -> i32
where
    Request: Message + Default,
    Response: Message,
{
    reset_output(out_resp);
    reset_output(out_err);

    if context.is_null() {
        write_error(out_err, format!("lake {operation} service context is null"));
        return 2;
    }
    if ptr.is_null() {
        write_error(out_err, format!("lake {operation} request ptr is null"));
        return 2;
    }

    // SAFETY: the C++ bridge owns the service for the entire BRPC request and
    // passes `len` readable request bytes.  T7 installs this context only
    // after the compat application host has created the service.
    let service = unsafe { &*context.cast::<CompatLakeStorageService>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match decode_lake_request(operation, bytes, |request| execute(service, request)) {
        LakeWireResult::Response(response) => {
            write_bytes(out_resp, response);
            0
        }
        LakeWireResult::Error { code, message } => {
            if code == 2 {
                error!(target: "novarocks::ffi", error = %message, "lake {operation} decode failed");
            } else {
                error!(target: "novarocks::ffi", error = %message, "lake {operation} failed");
            }
            write_error(out_err, message);
            code
        }
    }
}

macro_rules! lake_ffi_export {
    ($symbol:ident, $operation:literal, $request:ty, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $symbol(
            context: *const c_void,
            ptr: *const u8,
            len: usize,
            out_resp: *mut NovaRocksRustBuf,
            out_err: *mut NovaRocksRustBuf,
        ) -> i32 {
            execute_lake_ffi::<$request, _>(
                context,
                ptr,
                len,
                out_resp,
                out_err,
                $operation,
                |service, request| service.$method(request),
            )
        }
    };
}

lake_ffi_export!(
    novarocks_rs_lake_publish_version,
    "publish_version",
    PublishVersionRequest,
    publish_version
);
lake_ffi_export!(
    novarocks_rs_lake_publish_log_version,
    "publish_log_version",
    PublishLogVersionRequest,
    publish_log_version
);
lake_ffi_export!(
    novarocks_rs_lake_publish_log_version_batch,
    "publish_log_version_batch",
    PublishLogVersionBatchRequest,
    publish_log_version_batch
);
lake_ffi_export!(
    novarocks_rs_lake_abort_txn,
    "abort_txn",
    AbortTxnRequest,
    abort_txn
);
lake_ffi_export!(
    novarocks_rs_lake_drop_table,
    "drop_table",
    DropTableRequest,
    drop_table
);
lake_ffi_export!(
    novarocks_rs_lake_delete_tablet,
    "delete_tablet",
    DeleteTabletRequest,
    delete_tablet
);
lake_ffi_export!(
    novarocks_rs_lake_delete_data,
    "delete_data",
    DeleteDataRequest,
    delete_data
);
lake_ffi_export!(
    novarocks_rs_lake_get_tablet_stats,
    "get_tablet_stats",
    TabletStatRequest,
    get_tablet_stats
);
lake_ffi_export!(
    novarocks_rs_lake_compact,
    "compact",
    CompactRequest,
    compact
);
lake_ffi_export!(
    novarocks_rs_lake_abort_compaction,
    "abort_compaction",
    AbortCompactionRequest,
    abort_compaction
);
lake_ffi_export!(novarocks_rs_lake_vacuum, "vacuum", VacuumRequest, vacuum);

#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use super::{NovaRocksRustBuf, novarocks_rs_lake_publish_version};

    fn take_buffer(buffer: NovaRocksRustBuf) -> Vec<u8> {
        if buffer.ptr.is_null() {
            return Vec::new();
        }
        // SAFETY: test-owned buffer was allocated by `write_bytes` above.
        unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.len) }
    }

    #[test]
    fn lake_ffi_rejects_a_null_context_without_dereferencing_request_memory() {
        let request = [0_u8];
        let mut response = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let mut error = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };

        assert_eq!(
            novarocks_rs_lake_publish_version(
                std::ptr::null::<c_void>(),
                request.as_ptr(),
                request.len(),
                &mut response,
                &mut error,
            ),
            2
        );
        assert!(response.ptr.is_null());
        assert_eq!(
            String::from_utf8(take_buffer(error)).expect("valid error text"),
            "lake publish_version service context is null"
        );
    }

    #[test]
    fn lake_ffi_preserves_null_request_text() {
        let mut response = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let mut error = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let service = crate::lake_storage::CompatLakeStorageService::new(Default::default());

        assert_eq!(
            novarocks_rs_lake_publish_version(
                std::ptr::from_ref(&service).cast(),
                std::ptr::null(),
                0,
                &mut response,
                &mut error,
            ),
            2
        );
        assert!(response.ptr.is_null());
        assert_eq!(
            String::from_utf8(take_buffer(error)).expect("valid error text"),
            "lake publish_version request ptr is null"
        );
    }
}
