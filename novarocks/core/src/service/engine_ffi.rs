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

use crate::common::result_batch::ResultBatch;
use crate::novarocks_logging::error;
use crate::service::grpc_client::proto::starrocks::{
    PLookUpCloseRequest, PLookUpCloseResponse, PLookUpRequest, PLookUpResponse,
    PTransmitChunkParams, PTransmitChunkResult, PUpdateFailPointStatusRequest,
    PUpdateFailPointStatusResponse,
};
use crate::service::internal_rpc;
use crate::{FetchResult, UniqueId};
use prost::Message;

#[repr(C)]
pub struct NovaRocksRustBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub struct NovaRocksUniqueId {
    pub hi: i64,
    pub lo: i64,
}

const FETCH_OK: i32 = 0;
const FETCH_NOT_FOUND: i32 = 1;
const FETCH_CANCELLED: i32 = 2;
const FETCH_FAILED: i32 = 3;
const FETCH_TIMEOUT: i32 = 4;
const FETCH_NOT_READY: i32 = 4;

fn unique_id(hi: i64, lo: i64) -> UniqueId {
    UniqueId { hi, lo }
}

fn result_batch_to_thrift(
    batch: &ResultBatch,
    packet_seq: i64,
) -> crate::thrift::data::TResultBatch {
    crate::thrift::data::TResultBatch::new(
        batch.rows.clone(),
        batch.is_compressed,
        packet_seq,
        batch.statistic_version,
    )
}

fn estimate_result_batch_bytes(batch: &ResultBatch) -> usize {
    let mut rows_bytes = 0usize;
    for row in &batch.rows {
        rows_bytes = rows_bytes.saturating_add(4);
        rows_bytes = rows_bytes.saturating_add(row.len());
    }

    let mut total = 24usize.saturating_add(rows_bytes);
    if batch.statistic_version.is_some() {
        total = total.saturating_add(7);
    }

    total.saturating_add(64)
}

fn thrift_serialize_result_batch(batch: &ResultBatch, packet_seq: i64) -> Vec<u8> {
    use thrift::protocol::{TBinaryOutputProtocol, TSerializable};
    use thrift::transport::{TBufferChannel, TIoChannel};

    let wire_batch = result_batch_to_thrift(batch, packet_seq);
    let capacity = estimate_result_batch_bytes(batch);
    let channel = TBufferChannel::with_capacity(0, capacity);
    let (_, w) = channel.split().expect("split TBufferChannel");
    let mut protocol = TBinaryOutputProtocol::new(w, true);
    wire_batch
        .write_to_out_protocol(&mut protocol)
        .expect("write TResultBatch");
    protocol.transport.write_bytes()
}

fn write_fetch_result(
    result: FetchResult,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
) {
    unsafe {
        if !out_packet_seq.is_null() {
            *out_packet_seq = result.packet_seq;
        }
        if !out_eos.is_null() {
            *out_eos = result.eos;
        }
        if !out_batch.is_null() {
            *out_batch = NovaRocksRustBuf {
                ptr: std::ptr::null_mut(),
                len: 0,
            };
            // Align with StarRocks BE: EOS closes the stream with packet_seq/eos only
            // and does not send an empty TResultBatch attachment.
            if !(result.eos && result.result_batch.rows.is_empty()) {
                let bytes = thrift_serialize_result_batch(&result.result_batch, result.packet_seq);
                let boxed = bytes.into_boxed_slice();
                let len = boxed.len();
                let ptr = Box::into_raw(boxed) as *mut u8;
                *out_batch = NovaRocksRustBuf { ptr, len };
            }
        }
    }
}

fn write_string_buf(message: String, out: *mut NovaRocksRustBuf) {
    unsafe {
        if out.is_null() {
            return;
        }
        let boxed = message.into_bytes().into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        *out = NovaRocksRustBuf { ptr, len };
    }
}

fn write_bytes_buf(bytes: Vec<u8>, out: *mut NovaRocksRustBuf) {
    unsafe {
        if out.is_null() {
            return;
        }
        let boxed = bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        *out = NovaRocksRustBuf { ptr, len };
    }
}

fn init_out_buf(out: *mut NovaRocksRustBuf) {
    unsafe {
        if !out.is_null() {
            (*out).ptr = std::ptr::null_mut();
            (*out).len = 0;
        }
    }
}

fn handle_unary_proto_rpc<Request, Response, F>(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
    rpc_name: &str,
    handler: F,
) -> i32
where
    Request: Message + Default,
    Response: Message,
    F: FnOnce(Request) -> Response,
{
    init_out_buf(out_resp);
    init_out_buf(out_err);
    if ptr.is_null() {
        write_string_buf(format!("{rpc_name} request ptr is null"), out_err);
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match Request::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode {rpc_name} request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, rpc = rpc_name, "decode failed");
            return 2;
        }
    };

    write_bytes_buf(handler(request).encode_to_vec(), out_resp);
    0
}

/// Returns:
/// - 0: OK (a result batch is returned; may be EOS)
/// - 1: NOT_FOUND
/// - 2: CANCELLED
/// - 3: FAILED
/// - 4: TIMEOUT
#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_fetch_result_batch(
    finst_id_hi: i64,
    finst_id_lo: i64,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }

    let finst_id = unique_id(finst_id_hi, finst_id_lo);
    let timeout = crate::runtime::result_buffer::fetch_wait_timeout(finst_id);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match crate::runtime::result_buffer::try_fetch(finst_id) {
            crate::runtime::result_buffer::TryFetchResult::Ready(result) => {
                write_fetch_result(result, out_packet_seq, out_eos, out_batch);
                return FETCH_OK;
            }
            crate::runtime::result_buffer::TryFetchResult::NotReady => {
                if std::time::Instant::now() >= deadline {
                    write_string_buf(
                        format!("timeout waiting for result after {:?}", timeout),
                        out_err,
                    );
                    return FETCH_TIMEOUT;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            crate::runtime::result_buffer::TryFetchResult::Error(err) => {
                write_string_buf(err.message, out_err);
                return match err.kind {
                    crate::runtime::result_buffer::FetchErrorKind::NotFound => FETCH_NOT_FOUND,
                    crate::runtime::result_buffer::FetchErrorKind::Cancelled => FETCH_CANCELLED,
                    crate::runtime::result_buffer::FetchErrorKind::Failed => FETCH_FAILED,
                };
            }
        }
    }
}

/// Returns:
/// - 0: OK (a result batch is returned; may be EOS)
/// - 1: NOT_FOUND
/// - 2: CANCELLED
/// - 3: FAILED
/// - 4: NOT_READY
#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_try_fetch_result_batch(
    finst_id_hi: i64,
    finst_id_lo: i64,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }

    let finst_id = unique_id(finst_id_hi, finst_id_lo);
    match crate::runtime::result_buffer::try_fetch(finst_id) {
        crate::runtime::result_buffer::TryFetchResult::Ready(result) => {
            write_fetch_result(result, out_packet_seq, out_eos, out_batch);
            FETCH_OK
        }
        crate::runtime::result_buffer::TryFetchResult::NotReady => FETCH_NOT_READY,
        crate::runtime::result_buffer::TryFetchResult::Error(err) => {
            write_string_buf(err.message, out_err);
            match err.kind {
                crate::runtime::result_buffer::FetchErrorKind::NotFound => FETCH_NOT_FOUND,
                crate::runtime::result_buffer::FetchErrorKind::Cancelled => FETCH_CANCELLED,
                crate::runtime::result_buffer::FetchErrorKind::Failed => FETCH_FAILED,
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_fetch_wait_timeout_ms(finst_id_hi: i64, finst_id_lo: i64) -> i64 {
    crate::runtime::result_buffer::fetch_wait_timeout_ms(unique_id(finst_id_hi, finst_id_lo))
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_transmit_chunk(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<PTransmitChunkParams, PTransmitChunkResult, _>(
        ptr,
        len,
        out_resp,
        out_err,
        "transmit_chunk",
        internal_rpc::handle_transmit_chunk_compat,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lookup(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<PLookUpRequest, PLookUpResponse, _>(
        ptr,
        len,
        out_resp,
        out_err,
        "lookup",
        internal_rpc::handle_lookup_compat,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lookup_close(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<PLookUpCloseRequest, PLookUpCloseResponse, _>(
        ptr,
        len,
        out_resp,
        out_err,
        "lookup_close",
        internal_rpc::handle_lookup_close_compat,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_update_fail_point_status(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<PUpdateFailPointStatusRequest, PUpdateFailPointStatusResponse, _>(
        ptr,
        len,
        out_resp,
        out_err,
        "update_fail_point_status",
        internal_rpc::handle_update_fail_point_status,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_free_buf(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(slice));
    }
}
