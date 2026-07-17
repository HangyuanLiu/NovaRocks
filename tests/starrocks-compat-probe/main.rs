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

// Cargo auto-discovers directories under tests/ as integration targets. The
// production probe is the explicit test=false binary declared in Cargo.toml.
#![cfg(not(test))]

use anyhow::{Context, Result, bail};
use novarocks::proto::starrocks::{
    PExecBatchPlanFragmentsRequest, PExecBatchPlanFragmentsResult, PExecPlanFragmentRequest,
    PExecPlanFragmentResult, PFetchDataRequest, PFetchDataResult, PLookUpRequest, PLookUpResponse,
    PTransmitChunkParams, PTransmitChunkResult, PTransmitRuntimeFilterParams,
    PTransmitRuntimeFilterResult, PUniqueId, StatusPb,
};
use prost::Message;
use std::ffi::{CString, c_char};
use std::ptr;

const PROBES: &[&str] = &[
    "malformed-plan",
    "malformed-batch-plan",
    "malformed-chunk",
    "malformed-runtime-filter",
    "malformed-lookup",
    "terminal-fetch",
];

#[repr(C)]
struct NovaRocksRustBuf {
    ptr: *mut u8,
    len: usize,
}

type NetworkClient = unsafe extern "C" fn(
    *const c_char,
    u16,
    *const u8,
    usize,
    *const u8,
    usize,
    *mut NovaRocksRustBuf,
    *mut NovaRocksRustBuf,
) -> i32;

unsafe extern "C" {
    fn novarocks_compat_exec_plan_fragment(
        host: *const c_char,
        port: u16,
        request_ptr: *const u8,
        request_len: usize,
        attachment_ptr: *const u8,
        attachment_len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_exec_batch_plan_fragments(
        host: *const c_char,
        port: u16,
        request_ptr: *const u8,
        request_len: usize,
        attachment_ptr: *const u8,
        attachment_len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_fetch_data(
        host: *const c_char,
        port: u16,
        request_ptr: *const u8,
        request_len: usize,
        attachment_ptr: *const u8,
        attachment_len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_transmit_chunk(
        host: *const c_char,
        port: u16,
        request_ptr: *const u8,
        request_len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_transmit_runtime_filter(
        host: *const c_char,
        port: u16,
        request_ptr: *const u8,
        request_len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_lookup(
        host: *const c_char,
        port: u16,
        request_ptr: *const u8,
        request_len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_free_buf(ptr: *mut u8, len: usize);
}

struct Args {
    host: String,
    brpc_port: u16,
    probe: String,
}

fn parse_args() -> Result<Args> {
    let mut host = None;
    let mut brpc_port = None;
    let mut probe = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args
            .next()
            .with_context(|| format!("missing value for {arg}"))?;
        match arg.as_str() {
            "--host" if host.is_none() => host = Some(value),
            "--brpc-port" if brpc_port.is_none() => {
                brpc_port = Some(value.parse::<u16>().context("invalid --brpc-port")?)
            }
            "--probe" if probe.is_none() => probe = Some(value),
            _ => bail!("unknown or duplicate argument: {arg}"),
        }
    }
    let host = host.context("--host is required")?;
    if host.is_empty() {
        bail!("--host must not be empty");
    }
    let brpc_port = brpc_port.context("--brpc-port is required")?;
    if brpc_port == 0 {
        bail!("--brpc-port must be positive");
    }
    let probe = probe.context("--probe is required")?;
    if !PROBES.contains(&probe.as_str()) {
        bail!("unknown probe: {probe}");
    }
    Ok(Args {
        host,
        brpc_port,
        probe,
    })
}

fn take_buf(buf: &mut NovaRocksRustBuf) -> Vec<u8> {
    let bytes = if buf.ptr.is_null() || buf.len == 0 {
        Vec::new()
    } else {
        // SAFETY: The C ABI returns a readable allocation of exactly len bytes.
        unsafe { std::slice::from_raw_parts(buf.ptr, buf.len).to_vec() }
    };
    if !buf.ptr.is_null() {
        // SAFETY: The allocation came from the matching C ABI and is freed once.
        unsafe { novarocks_compat_free_buf(buf.ptr, buf.len) };
    }
    buf.ptr = ptr::null_mut();
    buf.len = 0;
    bytes
}

fn call_with_attachment<M: Message>(
    host: &CString,
    port: u16,
    request: &M,
    attachment: &[u8],
    client: NetworkClient,
) -> Result<Vec<u8>> {
    let request = request.encode_to_vec();
    let mut response = NovaRocksRustBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let mut error = NovaRocksRustBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    // SAFETY: All slices remain alive for the synchronous call and output buffers are initialized.
    let rc = unsafe {
        client(
            host.as_ptr(),
            port,
            request.as_ptr(),
            request.len(),
            attachment.as_ptr(),
            attachment.len(),
            &mut response,
            &mut error,
        )
    };
    let response = take_buf(&mut response);
    let error = take_buf(&mut error);
    if rc != 0 && error.is_empty() {
        bail!("network client failed with status {rc} and an empty error");
    }
    if response.is_empty() {
        if rc == 0 {
            bail!("network client returned an empty protobuf response");
        }
        bail!(
            "network client failed with status {rc} and no protobuf response: {}",
            String::from_utf8_lossy(&error)
        );
    }
    Ok(response)
}

fn call_without_attachment<M: Message>(
    host: &CString,
    port: u16,
    request: &M,
    client: unsafe extern "C" fn(
        *const c_char,
        u16,
        *const u8,
        usize,
        *mut NovaRocksRustBuf,
        *mut NovaRocksRustBuf,
    ) -> i32,
) -> Result<Vec<u8>> {
    let request = request.encode_to_vec();
    let mut response = NovaRocksRustBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let mut error = NovaRocksRustBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    // SAFETY: All slices remain alive for the synchronous call and output buffers are initialized.
    let rc = unsafe {
        client(
            host.as_ptr(),
            port,
            request.as_ptr(),
            request.len(),
            &mut response,
            &mut error,
        )
    };
    let response = take_buf(&mut response);
    let error = take_buf(&mut error);
    if rc != 0 && error.is_empty() {
        bail!("network client failed with status {rc} and an empty error");
    }
    if response.is_empty() {
        if rc == 0 {
            bail!("network client returned an empty protobuf response");
        }
        bail!(
            "network client failed with status {rc} and no protobuf response: {}",
            String::from_utf8_lossy(&error)
        );
    }
    Ok(response)
}

fn require_error_status(status: &StatusPb) -> Result<()> {
    if status.status_code == 0 {
        bail!("negative local fixture unexpectedly succeeded");
    }
    Ok(())
}

fn unique_id() -> PUniqueId {
    PUniqueId {
        hi: 0x5354_4301,
        lo: 0x4e52_0006,
    }
}

fn run_probe(args: &Args) -> Result<()> {
    let host = CString::new(args.host.as_str()).context("--host contains a NUL byte")?;
    match args.probe.as_str() {
        "malformed-plan" => {
            let request = PExecPlanFragmentRequest {
                attachment_protocol: Some("binary".to_string()),
            };
            let bytes = call_with_attachment(
                &host,
                args.brpc_port,
                &request,
                &[0xff, 0x00, 0x01],
                novarocks_compat_exec_plan_fragment,
            )?;
            let response = PExecPlanFragmentResult::decode(bytes.as_slice())?;
            require_error_status(&response.status)
        }
        "malformed-batch-plan" => {
            let request = PExecBatchPlanFragmentsRequest {
                attachment_protocol: Some("binary".to_string()),
            };
            let bytes = call_with_attachment(
                &host,
                args.brpc_port,
                &request,
                &[0xff, 0x00, 0x02],
                novarocks_compat_exec_batch_plan_fragments,
            )?;
            let response = PExecBatchPlanFragmentsResult::decode(bytes.as_slice())?;
            require_error_status(
                response
                    .status
                    .as_ref()
                    .context("response status is missing")?,
            )
        }
        "malformed-chunk" => {
            let request = PTransmitChunkParams {
                finst_id: Some(unique_id()),
                node_id: Some(-1),
                sender_id: Some(-1),
                be_number: Some(-1),
                eos: Some(false),
                sequence: Some(-1),
                chunks: Vec::new(),
                query_statistics: None,
                use_pass_through: None,
                is_pipeline_level_shuffle: None,
                driver_sequences: Vec::new(),
            };
            let bytes = call_without_attachment(
                &host,
                args.brpc_port,
                &request,
                novarocks_compat_transmit_chunk,
            )?;
            let response = PTransmitChunkResult::decode(bytes.as_slice())?;
            require_error_status(
                response
                    .status
                    .as_ref()
                    .context("response status is missing")?,
            )
        }
        "malformed-runtime-filter" => {
            let request = PTransmitRuntimeFilterParams {
                is_partial: Some(false),
                query_id: Some(unique_id()),
                filter_id: Some(-1),
                finst_id: Some(unique_id()),
                data: Some(vec![0xff, 0x00, 0x03]),
                probe_finst_ids: Vec::new(),
                build_be_number: Some(-1),
                forward_targets: Vec::new(),
                broadcast_timestamp: None,
                is_pipeline: Some(true),
                is_skew_broadcast_join: None,
                column_type: None,
                skew_shuffle_filter_id: None,
                transmit_timeout_ms: Some(10_000),
                transmit_via_http_min_size: None,
            };
            let bytes = call_without_attachment(
                &host,
                args.brpc_port,
                &request,
                novarocks_compat_transmit_runtime_filter,
            )?;
            let response = PTransmitRuntimeFilterResult::decode(bytes.as_slice())?;
            require_error_status(
                response
                    .status
                    .as_ref()
                    .context("response status is missing")?,
            )
        }
        "malformed-lookup" => {
            let request = PLookUpRequest {
                query_id: Some(unique_id()),
                lookup_node_id: Some(-1),
                request_tuple_id: Some(-1),
                request_columns: Vec::new(),
                lookup_slots: Vec::new(),
            };
            let bytes =
                call_without_attachment(&host, args.brpc_port, &request, novarocks_compat_lookup)?;
            let response = PLookUpResponse::decode(bytes.as_slice())?;
            require_error_status(
                response
                    .status
                    .as_ref()
                    .context("response status is missing")?,
            )
        }
        "terminal-fetch" => {
            let request = PFetchDataRequest {
                finst_id: unique_id(),
            };
            let bytes = call_with_attachment(
                &host,
                args.brpc_port,
                &request,
                &[],
                novarocks_compat_fetch_data,
            )?;
            let response = PFetchDataResult::decode(bytes.as_slice())?;
            require_error_status(&response.status)
        }
        _ => bail!("unknown probe: {}", args.probe),
    }
}

fn main() {
    let result = (|| -> Result<()> {
        let args = parse_args()?;
        run_probe(&args)?;
        println!(
            "probe={} status=PASS endpoint={}:{}",
            args.probe, args.host, args.brpc_port
        );
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!("starrocks-compat-probe failed: {error:#}");
        std::process::exit(1);
    }
}
