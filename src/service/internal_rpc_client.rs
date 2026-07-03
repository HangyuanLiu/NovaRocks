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
use std::ffi::CString;

use prost::Message;

use crate::common::types::UniqueId;
use crate::service::engine_ffi::NovaRocksRustBuf;

pub use crate::proto;

unsafe extern "C" {
    fn novarocks_compat_transmit_chunk(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_transmit_runtime_filter(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_lookup(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_free_buf(ptr: *mut u8, len: usize);
}

type UnaryClientFn = unsafe extern "C" fn(
    host: *const std::os::raw::c_char,
    port: u16,
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32;

fn take_compat_buf(buf: &mut NovaRocksRustBuf) -> Vec<u8> {
    let bytes = if buf.ptr.is_null() || buf.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(buf.ptr, buf.len) }.to_vec()
    };
    if !buf.ptr.is_null() {
        unsafe { novarocks_compat_free_buf(buf.ptr, buf.len) };
        buf.ptr = std::ptr::null_mut();
        buf.len = 0;
    }
    bytes
}

fn status_error(status: Option<&proto::starrocks::StatusPb>, rpc: &str) -> Result<(), String> {
    let Some(status) = status else {
        return Ok(());
    };
    if status.status_code == 0 {
        return Ok(());
    }
    if status.error_msgs.is_empty() {
        return Err(format!("{rpc} returned status_code={}", status.status_code));
    }
    Err(format!("{rpc} failed: {}", status.error_msgs.join("; ")))
}

fn runtime_filter_request_to_compat(
    params: proto::filter::TransmitRuntimeFilterRequest,
) -> proto::starrocks::PTransmitRuntimeFilterParams {
    let column_type = params.column_type.and_then(|desc| {
        crate::exec::runtime_filter::arrow_type_from_common_type_desc(&desc).and_then(|data_type| {
            crate::exec::runtime_filter::arrow_type_to_proto_type_desc(&data_type)
        })
    });

    proto::starrocks::PTransmitRuntimeFilterParams {
        is_partial: Some(params.is_partial),
        query_id: params
            .query_id
            .as_ref()
            .map(|query_id| proto::starrocks::PUniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
        filter_id: Some(params.filter_id),
        data: Some(params.data),
        build_be_number: params.is_partial.then_some(params.build_be_number),
        column_type,
        ..Default::default()
    }
}

fn call_unary<Request, Response>(
    dest_host: &str,
    dest_port: u16,
    request: Request,
    rpc_name: &str,
    func: UnaryClientFn,
) -> Result<Response, String>
where
    Request: Message,
    Response: Message + Default,
{
    let host = CString::new(dest_host)
        .map_err(|e| format!("{rpc_name} invalid destination host {dest_host:?}: {e}"))?;
    let req_bytes = request.encode_to_vec();
    let mut resp_buf = NovaRocksRustBuf {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    let mut err_buf = NovaRocksRustBuf {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    let rc = unsafe {
        func(
            host.as_ptr(),
            dest_port,
            req_bytes.as_ptr(),
            req_bytes.len(),
            &mut resp_buf,
            &mut err_buf,
        )
    };
    let err_bytes = take_compat_buf(&mut err_buf);
    let resp_bytes = take_compat_buf(&mut resp_buf);
    if rc != 0 {
        let err = String::from_utf8(err_bytes)
            .unwrap_or_else(|_| format!("{rpc_name} returned non-utf8 error"));
        return Err(if err.is_empty() {
            format!("{rpc_name} failed with rc={rc}")
        } else {
            err
        });
    }
    Response::decode(resp_bytes.as_slice())
        .map_err(|e| format!("{rpc_name} decode response failed: {e}"))
}

pub fn send_chunks(
    dest_host: &str,
    dest_port: u16,
    finst_id: UniqueId,
    node_id: i32,
    sender_id: i32,
    be_number: i32,
    eos: bool,
    sequence: i64,
    payload: Vec<u8>,
) -> Result<(), String> {
    let params = proto::starrocks::PTransmitChunkParams {
        finst_id: Some(proto::starrocks::PUniqueId {
            hi: finst_id.hi,
            lo: finst_id.lo,
        }),
        node_id: Some(node_id),
        sender_id: Some(sender_id),
        be_number: Some(be_number),
        eos: Some(eos),
        sequence: Some(sequence),
        chunks: vec![proto::starrocks::ChunkPb {
            data: Some(payload),
            data_size: Some(0),
            ..Default::default()
        }],
        ..Default::default()
    };
    #[cfg(test)]
    if let Some(result) = maybe_transmit_chunk_hook(dest_host, dest_port, params.clone()) {
        return result.and_then(|resp| status_error(resp.status.as_ref(), "transmit_chunk"));
    }

    let response: proto::starrocks::PTransmitChunkResult = call_unary(
        dest_host,
        dest_port,
        params,
        "transmit_chunk",
        novarocks_compat_transmit_chunk,
    )?;
    status_error(response.status.as_ref(), "transmit_chunk")
}

pub fn transmit_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: proto::filter::TransmitRuntimeFilterRequest,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) = maybe_transmit_runtime_filter_hook(dest_host, dest_port, params.clone()) {
        return result;
    }

    let compat_request = runtime_filter_request_to_compat(params);
    #[cfg(test)]
    let response: proto::starrocks::PTransmitRuntimeFilterResult = if let Some(result) =
        maybe_transmit_runtime_filter_wire_hook(dest_host, dest_port, compat_request.clone())
    {
        result?
    } else {
        call_unary(
            dest_host,
            dest_port,
            compat_request,
            "transmit_runtime_filter",
            novarocks_compat_transmit_runtime_filter,
        )?
    };
    #[cfg(not(test))]
    let response: proto::starrocks::PTransmitRuntimeFilterResult = call_unary(
        dest_host,
        dest_port,
        compat_request,
        "transmit_runtime_filter",
        novarocks_compat_transmit_runtime_filter,
    )?;
    status_error(response.status.as_ref(), "transmit_runtime_filter")
}

pub fn lookup(
    dest_host: &str,
    dest_port: u16,
    params: proto::filter::LookupRequest,
) -> Result<proto::filter::LookupResponse, String> {
    #[cfg(test)]
    if let Some(result) = maybe_lookup_hook(dest_host, dest_port, params.clone()) {
        return result;
    }

    let compat_request = proto::starrocks::PLookUpRequest {
        query_id: params
            .query_id
            .as_ref()
            .map(|query_id| proto::starrocks::PUniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
        lookup_node_id: Some(params.lookup_node_id),
        request_tuple_id: Some(params.request_tuple_id),
        request_columns: params
            .request_columns
            .into_iter()
            .map(|col| proto::starrocks::PColumn {
                slot_id: Some(col.slot_id),
                data_size: Some(col.data_size),
                data: Some(col.data),
            })
            .collect(),
        lookup_slots: Vec::new(),
    };

    #[cfg(test)]
    let response: proto::starrocks::PLookUpResponse = if let Some(result) =
        maybe_lookup_wire_hook(dest_host, dest_port, compat_request.clone())
    {
        result?
    } else {
        call_unary(
            dest_host,
            dest_port,
            compat_request,
            "lookup",
            novarocks_compat_lookup,
        )?
    };
    #[cfg(not(test))]
    let response: proto::starrocks::PLookUpResponse = call_unary(
        dest_host,
        dest_port,
        compat_request,
        "lookup",
        novarocks_compat_lookup,
    )?;

    let status = response.status.map(|status| proto::common::Status {
        code: status.status_code,
        message: status.error_msgs.join("; "),
    });
    let mut columns = Vec::with_capacity(response.columns.len());
    for col in response.columns {
        let slot_id = col
            .slot_id
            .ok_or_else(|| "lookup response column missing slot_id".to_string())?;
        let data = col
            .data
            .ok_or_else(|| "lookup response column missing data".to_string())?;
        columns.push(proto::filter::Column {
            slot_id,
            data_size: col.data_size.unwrap_or(data.len() as i64),
            data,
        });
    }
    Ok(proto::filter::LookupResponse { status, columns })
}

#[cfg(test)]
type TransmitChunkHook = std::sync::Arc<
    dyn Fn(
            &str,
            u16,
            proto::starrocks::PTransmitChunkParams,
        ) -> Result<proto::starrocks::PTransmitChunkResult, String>
        + Send
        + Sync,
>;

#[cfg(test)]
type TransmitRuntimeFilterHook = std::sync::Arc<
    dyn Fn(&str, u16, proto::filter::TransmitRuntimeFilterRequest) -> Result<(), String>
        + Send
        + Sync,
>;

#[cfg(test)]
type TransmitRuntimeFilterWireHook = std::sync::Arc<
    dyn Fn(
            &str,
            u16,
            proto::starrocks::PTransmitRuntimeFilterParams,
        ) -> Result<proto::starrocks::PTransmitRuntimeFilterResult, String>
        + Send
        + Sync,
>;

#[cfg(test)]
type LookupHook = std::sync::Arc<
    dyn Fn(&str, u16, proto::filter::LookupRequest) -> Result<proto::filter::LookupResponse, String>
        + Send
        + Sync,
>;

#[cfg(test)]
type LookupWireHook = std::sync::Arc<
    dyn Fn(
            &str,
            u16,
            proto::starrocks::PLookUpRequest,
        ) -> Result<proto::starrocks::PLookUpResponse, String>
        + Send
        + Sync,
>;

#[cfg(test)]
fn transmit_chunk_hook() -> &'static std::sync::Mutex<Option<TransmitChunkHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<TransmitChunkHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn transmit_runtime_filter_hook() -> &'static std::sync::Mutex<Option<TransmitRuntimeFilterHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<TransmitRuntimeFilterHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn transmit_runtime_filter_wire_hook()
-> &'static std::sync::Mutex<Option<TransmitRuntimeFilterWireHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<TransmitRuntimeFilterWireHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn lookup_hook() -> &'static std::sync::Mutex<Option<LookupHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<LookupHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn lookup_wire_hook() -> &'static std::sync::Mutex<Option<LookupWireHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<LookupWireHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn test_hook_mutex() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn maybe_transmit_chunk_hook(
    host: &str,
    port: u16,
    params: proto::starrocks::PTransmitChunkParams,
) -> Option<Result<proto::starrocks::PTransmitChunkResult, String>> {
    let hook = transmit_chunk_hook()
        .lock()
        .expect("transmit_chunk hook lock")
        .clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
fn maybe_transmit_runtime_filter_hook(
    host: &str,
    port: u16,
    params: proto::filter::TransmitRuntimeFilterRequest,
) -> Option<Result<(), String>> {
    let hook = transmit_runtime_filter_hook()
        .lock()
        .expect("transmit_runtime_filter hook lock")
        .clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
fn maybe_transmit_runtime_filter_wire_hook(
    host: &str,
    port: u16,
    params: proto::starrocks::PTransmitRuntimeFilterParams,
) -> Option<Result<proto::starrocks::PTransmitRuntimeFilterResult, String>> {
    let hook = transmit_runtime_filter_wire_hook()
        .lock()
        .expect("transmit_runtime_filter wire hook lock")
        .clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
fn maybe_lookup_hook(
    host: &str,
    port: u16,
    params: proto::filter::LookupRequest,
) -> Option<Result<proto::filter::LookupResponse, String>> {
    let hook = lookup_hook().lock().expect("lookup hook lock").clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
fn maybe_lookup_wire_hook(
    host: &str,
    port: u16,
    params: proto::starrocks::PLookUpRequest,
) -> Option<Result<proto::starrocks::PLookUpResponse, String>> {
    let hook = lookup_wire_hook()
        .lock()
        .expect("lookup wire hook lock")
        .clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
pub(crate) fn test_hook_lock() -> std::sync::MutexGuard<'static, ()> {
    test_hook_mutex().lock().expect("test hook global lock")
}

#[cfg(test)]
pub(crate) fn clear_test_hooks() {
    *transmit_chunk_hook()
        .lock()
        .expect("transmit_chunk hook lock") = None;
    *transmit_runtime_filter_hook()
        .lock()
        .expect("transmit_runtime_filter hook lock") = None;
    *transmit_runtime_filter_wire_hook()
        .lock()
        .expect("transmit_runtime_filter wire hook lock") = None;
    *lookup_hook().lock().expect("lookup hook lock") = None;
    *lookup_wire_hook().lock().expect("lookup wire hook lock") = None;
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_transmit_chunk_hook<F>(hook: F)
where
    F: Fn(
            &str,
            u16,
            proto::starrocks::PTransmitChunkParams,
        ) -> Result<proto::starrocks::PTransmitChunkResult, String>
        + Send
        + Sync
        + 'static,
{
    *transmit_chunk_hook()
        .lock()
        .expect("transmit_chunk hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
pub(crate) fn set_transmit_runtime_filter_hook<F>(hook: F)
where
    F: Fn(&str, u16, proto::filter::TransmitRuntimeFilterRequest) -> Result<(), String>
        + Send
        + Sync
        + 'static,
{
    *transmit_runtime_filter_hook()
        .lock()
        .expect("transmit_runtime_filter hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
fn set_transmit_runtime_filter_wire_hook<F>(hook: F)
where
    F: Fn(
            &str,
            u16,
            proto::starrocks::PTransmitRuntimeFilterParams,
        ) -> Result<proto::starrocks::PTransmitRuntimeFilterResult, String>
        + Send
        + Sync
        + 'static,
{
    *transmit_runtime_filter_wire_hook()
        .lock()
        .expect("transmit_runtime_filter wire hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
pub(crate) fn set_lookup_hook<F>(hook: F)
where
    F: Fn(&str, u16, proto::filter::LookupRequest) -> Result<proto::filter::LookupResponse, String>
        + Send
        + Sync
        + 'static,
{
    *lookup_hook().lock().expect("lookup hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
fn set_lookup_wire_hook<F>(hook: F)
where
    F: Fn(
            &str,
            u16,
            proto::starrocks::PLookUpRequest,
        ) -> Result<proto::starrocks::PLookUpResponse, String>
        + Send
        + Sync
        + 'static,
{
    *lookup_wire_hook().lock().expect("lookup wire hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn test_transmit_runtime_filter_accepts_native_request_and_maps_starrocks_wire_request() {
        let _hook_guard = test_hook_lock();
        clear_test_hooks();

        let captured = Arc::new(Mutex::new(None));
        let captured_hook = Arc::clone(&captured);
        set_transmit_runtime_filter_wire_hook(move |host, port, req| {
            *captured_hook.lock().expect("captured lock") = Some((host.to_string(), port, req));
            Ok(proto::starrocks::PTransmitRuntimeFilterResult {
                status: Some(proto::starrocks::StatusPb {
                    status_code: 0,
                    error_msgs: Vec::new(),
                }),
                filter_id: Some(7),
            })
        });

        transmit_runtime_filter(
            "compat-host",
            9910,
            proto::filter::TransmitRuntimeFilterRequest {
                is_partial: true,
                query_id: Some(proto::common::UniqueId { hi: 10, lo: 20 }),
                filter_id: 7,
                data: vec![1, 2, 3],
                build_be_number: 3,
                column_type: Some(proto::common::TypeDesc {
                    kind: Some(proto::common::type_desc::Kind::Scalar(
                        proto::common::ScalarType {
                            r#type: proto::common::PrimitiveType::Int as i32,
                            len: None,
                            precision: None,
                            scale: None,
                            time_unit: None,
                        },
                    )),
                }),
            },
        )
        .expect("runtime filter should map through compat wire hook");

        let captured = captured.lock().expect("captured lock");
        let (host, port, request) = captured.as_ref().expect("captured request");
        assert_eq!(host, "compat-host");
        assert_eq!(*port, 9910);
        assert_eq!(request.is_partial, Some(true));
        assert_eq!(
            request.query_id,
            Some(proto::starrocks::PUniqueId { hi: 10, lo: 20 })
        );
        assert_eq!(request.filter_id, Some(7));
        assert_eq!(request.data, Some(vec![1, 2, 3]));
        assert_eq!(request.build_be_number, Some(3));
        let column_type = request.column_type.as_ref().expect("column type");
        let scalar = column_type
            .types
            .first()
            .and_then(|node| node.scalar_type.as_ref())
            .expect("scalar column type");
        assert_eq!(scalar.r#type, crate::thrift::types::TPrimitiveType::INT.0);

        clear_test_hooks();
    }

    #[test]
    fn test_lookup_accepts_native_request_and_maps_starrocks_wire_response() {
        let _hook_guard = test_hook_lock();
        clear_test_hooks();

        let captured = Arc::new(Mutex::new(None));
        let captured_hook = Arc::clone(&captured);
        set_lookup_wire_hook(move |host, port, req| {
            *captured_hook.lock().expect("captured lock") = Some((host.to_string(), port, req));
            Ok(proto::starrocks::PLookUpResponse {
                status: Some(proto::starrocks::StatusPb {
                    status_code: 0,
                    error_msgs: Vec::new(),
                }),
                columns: vec![proto::starrocks::PColumn {
                    slot_id: Some(4),
                    data_size: Some(3),
                    data: Some(vec![7, 8, 9]),
                }],
            })
        });

        let response = lookup(
            "compat-host",
            9911,
            proto::filter::LookupRequest {
                query_id: Some(proto::common::UniqueId { hi: 10, lo: 20 }),
                lookup_node_id: 77,
                request_tuple_id: 3,
                request_columns: vec![proto::filter::Column {
                    slot_id: 2,
                    data_size: 3,
                    data: vec![1, 2, 3],
                }],
            },
        )
        .expect("lookup should map through compat wire hook");

        assert_eq!(
            response.status,
            Some(proto::common::Status {
                code: 0,
                message: String::new(),
            })
        );
        assert_eq!(response.columns.len(), 1);
        assert_eq!(response.columns[0].slot_id, 4);
        assert_eq!(response.columns[0].data_size, 3);
        assert_eq!(response.columns[0].data, vec![7, 8, 9]);

        let captured = captured.lock().expect("captured lock");
        let (host, port, request) = captured.as_ref().expect("captured request");
        assert_eq!(host, "compat-host");
        assert_eq!(*port, 9911);
        assert_eq!(
            request.query_id,
            Some(proto::starrocks::PUniqueId { hi: 10, lo: 20 })
        );
        assert_eq!(request.lookup_node_id, Some(77));
        assert_eq!(request.request_tuple_id, Some(3));
        assert_eq!(request.request_columns.len(), 1);
        assert_eq!(request.request_columns[0].slot_id, Some(2));
        assert_eq!(request.request_columns[0].data, Some(vec![1, 2, 3]));
        assert!(request.lookup_slots.is_empty());

        clear_test_hooks();
    }
}
