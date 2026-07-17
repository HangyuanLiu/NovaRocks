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
#![cfg(feature = "compat")]

use ::std::time::{Duration, Instant};
use ::thrift::protocol::{TBinaryInputProtocol, TSerializable};
use ::thrift::transport::{TBufferChannel, TIoChannel};

const FIXTURE: &[u8] = ::std::include_bytes!(
    "fixtures/external_starrocks_fe/select_1_exec_batch_plan_fragments_v1.bin"
);
const FINST_ID_HI: i64 = 0x019f51afb5c37c92;
const FINST_ID_LO: i64 = 0x9c5da9f1a67bbec0_u64 as i64;
const FETCH_OK: i32 = 0;
const FETCH_NOT_FOUND: i32 = 1;
const FETCH_NOT_READY: i32 = 4;

struct OwnedFfiBuf {
    raw: ::novarocks::service::engine_ffi::NovaRocksRustBuf,
}

impl OwnedFfiBuf {
    fn empty() -> Self {
        Self {
            raw: ::novarocks::service::engine_ffi::NovaRocksRustBuf {
                ptr: ::std::ptr::null_mut(),
                len: 0,
            },
        }
    }

    fn as_mut(&mut self) -> &mut ::novarocks::service::engine_ffi::NovaRocksRustBuf {
        &mut self.raw
    }

    fn bytes(&self) -> Vec<u8> {
        if self.raw.ptr.is_null() || self.raw.len == 0 {
            Vec::new()
        } else {
            unsafe { ::std::slice::from_raw_parts(self.raw.ptr, self.raw.len) }.to_vec()
        }
    }
}

impl Drop for OwnedFfiBuf {
    fn drop(&mut self) {
        if !self.raw.ptr.is_null() {
            ::novarocks::service::engine_ffi::novarocks_rs_free_buf(self.raw.ptr, self.raw.len);
            self.raw.ptr = ::std::ptr::null_mut();
            self.raw.len = 0;
        }
    }
}

fn decode_result_batch(bytes: &[u8]) -> ::novarocks::thrift::data::TResultBatch {
    let mut channel = TBufferChannel::with_capacity(bytes.len(), 1024);
    channel.set_readable_bytes(bytes);
    let (reader, _) = channel.split().expect("split result-batch buffer");
    let mut protocol = TBinaryInputProtocol::new(reader, true);
    ::novarocks::thrift::data::TResultBatch::read_from_in_protocol(&mut protocol)
        .expect("decode TResultBatch returned by FFI")
}

fn assert_terminal_result_buffer_cleanup() {
    let mut packet_seq = -1;
    let mut eos = false;
    let mut batch = OwnedFfiBuf::empty();
    let mut error = OwnedFfiBuf::empty();
    let rc = ::novarocks::service::engine_ffi::novarocks_rs_try_fetch_result_batch(
        FINST_ID_HI,
        FINST_ID_LO,
        &mut packet_seq,
        &mut eos,
        batch.as_mut(),
        error.as_mut(),
    );
    let batch_bytes = batch.bytes();
    let error_bytes = error.bytes();
    let error_message = String::from_utf8_lossy(&error_bytes);
    assert_eq!(
        rc, FETCH_NOT_FOUND,
        "fetch after EOS must consume terminal state and return NOT_FOUND: {error_message}"
    );
    assert!(
        batch_bytes.is_empty(),
        "terminal cleanup must return no batch"
    );
    assert!(
        error_message.contains("eos"),
        "terminal cleanup must explain the EOS state: {error_message}"
    );
}

fn execute_fixture_once() {
    let created = ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE)
        .expect("submit external StarRocks FE batch attachment");
    assert_eq!(
        created, 1,
        "fixture must create exactly one fragment instance"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut rows = Vec::new();
    loop {
        let mut packet_seq = -1;
        let mut eos = false;
        let mut batch = OwnedFfiBuf::empty();
        let mut error = OwnedFfiBuf::empty();
        let rc = ::novarocks::service::engine_ffi::novarocks_rs_try_fetch_result_batch(
            FINST_ID_HI,
            FINST_ID_LO,
            &mut packet_seq,
            &mut eos,
            batch.as_mut(),
            error.as_mut(),
        );
        let batch_bytes = batch.bytes();
        let error_bytes = error.bytes();
        let error_message = String::from_utf8_lossy(&error_bytes);

        match rc {
            FETCH_OK if eos => {
                assert!(
                    batch_bytes.is_empty(),
                    "EOS must not carry a TResultBatch attachment"
                );
                assert_eq!(rows, vec![vec![1, b'1']], "expected one MySQL text row `1`");
                assert_terminal_result_buffer_cleanup();
                break;
            }
            FETCH_OK => {
                assert!(
                    error_bytes.is_empty(),
                    "successful fetch returned an FFI error: {error_message}"
                );
                assert!(
                    !batch_bytes.is_empty(),
                    "non-EOS fetch packet {packet_seq} has no TResultBatch attachment"
                );
                let result = decode_result_batch(&batch_bytes);
                assert_eq!(result.packet_seq, packet_seq);
                rows.extend(result.rows);
            }
            FETCH_NOT_READY if Instant::now() < deadline => {
                assert!(
                    error_bytes.is_empty(),
                    "not-ready fetch returned an FFI error: {error_message}"
                );
                ::std::thread::sleep(Duration::from_millis(10));
            }
            FETCH_NOT_READY => panic!("timed out after 5 seconds waiting for fixture result"),
            other => panic!(
                "fixture result fetch failed with rc={other}, packet_seq={packet_seq}, eos={eos}: {error_message}"
            ),
        }
    }
}

#[test]
fn external_starrocks_fe_batch_attachment_executes_twice_and_returns_one() {
    execute_fixture_once();
    execute_fixture_once();
}
