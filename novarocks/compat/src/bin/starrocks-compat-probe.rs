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

include!(concat!(env!("OUT_DIR"), "/proto_root_mod.rs"));

extern crate novarocks_compat as _;

use crate::proto::starrocks::{
    PExecBatchPlanFragmentsRequest, PExecBatchPlanFragmentsResult, PExecPlanFragmentRequest,
    PExecPlanFragmentResult, PFetchDataRequest, PFetchDataResult, PLookUpRequest, PLookUpResponse,
    PTransmitChunkParams, PTransmitChunkResult, PUniqueId, StatusPb,
};
use anyhow::{Context, Result, bail};
use prost::Message;
use std::ffi::{CString, c_char};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::ptr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PROBES: &[&str] = &[
    "malformed-plan",
    "malformed-batch-plan",
    "malformed-chunk",
    "malformed-lookup",
    "terminal-fetch",
    "stream-load",
    "transaction-load",
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
    http_port: u16,
    probe: String,
}

fn parse_args() -> Result<Args> {
    let mut host = None;
    let mut brpc_port = None;
    let mut http_port = None;
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
            "--http-port" if http_port.is_none() => {
                http_port = Some(value.parse::<u16>().context("invalid --http-port")?)
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
    let http_port = http_port.context("--http-port is required")?;
    if http_port == 0 {
        bail!("--http-port must be positive");
    }
    let probe = probe.context("--probe is required")?;
    if !PROBES.contains(&probe.as_str()) {
        bail!("unknown probe: {probe}");
    }
    Ok(Args {
        host,
        brpc_port,
        http_port,
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

fn unique_label(prefix: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nonce)
}

fn http_request(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> Result<serde_json::Value> {
    let mut stream = TcpStream::connect((host, port))
        .with_context(|| format!("connect compat HTTP endpoint {host}:{port}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .context("set compat HTTP read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .context("set compat HTTP write timeout")?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .context("write compat HTTP headers")?;
    stream.write_all(body).context("write compat HTTP body")?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("read compat HTTP response")?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("compat HTTP response is missing headers")?;
    let status = head.lines().next().unwrap_or_default();
    if !status.contains(" 200 ") {
        bail!("compat HTTP request failed: {status}; body={body}");
    }
    serde_json::from_str(body).context("decode compat load JSON response")
}

fn load_headers(label: &str) -> Vec<(&'static str, String)> {
    vec![
        ("Authorization", "Basic cm9vdDo=".to_string()),
        ("format", "csv".to_string()),
        ("label", label.to_string()),
    ]
}

fn require_load_response(
    response: &serde_json::Value,
    status: &str,
    label: &str,
    require_txn_id: bool,
) -> Result<()> {
    if response.get("Status").and_then(serde_json::Value::as_str) != Some(status) {
        bail!("unexpected compat load status: {response}");
    }
    if response
        .get("Message")
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        bail!("compat load response is missing Message: {response}");
    }
    if response.get("Label").and_then(serde_json::Value::as_str) != Some(label) {
        bail!("compat load response has an unexpected label: {response}");
    }
    if require_txn_id
        && response
            .get("TxnId")
            .and_then(serde_json::Value::as_i64)
            .is_none()
    {
        bail!("compat load response is missing TxnId: {response}");
    }
    Ok(())
}

fn run_stream_load_probe(args: &Args) -> Result<()> {
    let label = unique_label("rci5d_stream");
    let response = http_request(
        &args.host,
        args.http_port,
        "PUT",
        "/api/starrocks_compat_suite_setup/load_ingress_rows/_stream_load",
        &load_headers(&label),
        b"7001\tstream\n",
    )?;
    require_load_response(&response, "Success", &label, true)
}

fn run_transaction_load_probe(args: &Args) -> Result<()> {
    let label = unique_label("rci5d_txn");
    let mut headers = load_headers(&label);
    headers.push(("db", "starrocks_compat_suite_setup".to_string()));
    headers.push(("table", "load_ingress_rows".to_string()));
    let begin = http_request(
        &args.host,
        args.http_port,
        "POST",
        "/api/transaction/begin",
        &headers,
        &[],
    )?;
    require_load_response(&begin, "OK", &label, true)?;
    let load = http_request(
        &args.host,
        args.http_port,
        "PUT",
        "/api/transaction/load",
        &headers,
        b"7002\ttransaction\n",
    )?;
    require_load_response(&load, "OK", &label, true)?;
    let prepare = http_request(
        &args.host,
        args.http_port,
        "POST",
        "/api/transaction/prepare",
        &headers,
        &[],
    )?;
    require_load_response(&prepare, "OK", &label, true)?;
    let commit = http_request(
        &args.host,
        args.http_port,
        "POST",
        "/api/transaction/commit",
        &headers,
        &[],
    )?;
    require_load_response(&commit, "OK", &label, true)
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
        "stream-load" => run_stream_load_probe(args),
        "transaction-load" => run_transaction_load_probe(args),
        _ => bail!("unknown probe: {}", args.probe),
    }
}

#[cfg(not(test))]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    enum LocalResponse {
        Close,
        Invalid,
    }

    fn invoke_exec_plan_against_local_endpoint(response: LocalResponse) -> (i32, Vec<u8>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local probe endpoint");
        let port = listener.local_addr().expect("read local endpoint").port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept C ABI connection");
            if matches!(response, LocalResponse::Invalid) {
                stream
                    .write_all(b"invalid local brpc response")
                    .expect("write invalid local response");
            }
        });

        let host = CString::new("127.0.0.1").expect("valid local host");
        let request = PExecPlanFragmentRequest {
            attachment_protocol: Some("binary".to_string()),
        }
        .encode_to_vec();
        let attachment = [0xff, 0x00, 0x01];
        let mut response = NovaRocksRustBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let mut error = NovaRocksRustBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let started = Instant::now();
        // SAFETY: The listener is local, all input slices outlive the synchronous call,
        // and both output buffers are initialized and freed with the matching C ABI.
        let rc = unsafe {
            novarocks_compat_exec_plan_fragment(
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
        let _ = take_buf(&mut response);
        let error = take_buf(&mut error);
        server.join().expect("join local endpoint");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "local network C ABI failure was not bounded"
        );
        (rc, error)
    }

    #[test]
    fn network_c_abi_returns_error_when_local_endpoint_closes() {
        let (rc, error) = invoke_exec_plan_against_local_endpoint(LocalResponse::Close);
        assert_ne!(rc, 0, "a closed local endpoint must fail");
        assert!(!error.is_empty(), "transport failure must include an error");
    }

    #[test]
    fn network_c_abi_returns_error_for_invalid_local_response() {
        let (rc, error) = invoke_exec_plan_against_local_endpoint(LocalResponse::Invalid);
        assert_ne!(rc, 0, "an invalid local response must fail");
        assert!(!error.is_empty(), "protocol failure must include an error");
    }
}
