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

#[repr(C)]
struct NovaRocksCompatConfig {
    host: *const std::os::raw::c_char,
    heartbeat_port: u16,
    brpc_port: u16,
    internal_service_query_rpc_thread_num: u32,
    debug_exec_batch_plan_json: u8,
    log_level: u8,
}

unsafe extern "C" {
    fn novarocks_compat_start(
        cfg: *const NovaRocksCompatConfig,
        err_buf: *mut std::os::raw::c_char,
        err_buf_len: i32,
    ) -> i32;
    fn novarocks_compat_stop();
}

#[derive(Debug, Clone)]
pub struct CompatConfig<'a> {
    pub host: &'a str,
    pub heartbeat_port: u16,
    pub brpc_port: u16,
    pub internal_service_query_rpc_thread_num: u32,
    pub debug_exec_batch_plan_json: bool,
    pub log_level: u8,
}

#[derive(Debug)]
pub struct CompatError {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for CompatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "compat start failed (code={}): {}",
            self.code, self.message
        )
    }
}

impl std::error::Error for CompatError {}

pub fn start(cfg: &CompatConfig<'_>) -> Result<(), CompatError> {
    start_with(cfg, |native_cfg, err_buf, err_buf_len| unsafe {
        novarocks_compat_start(native_cfg, err_buf, err_buf_len)
    })
}

fn start_with(
    cfg: &CompatConfig<'_>,
    native_start: impl FnOnce(&NovaRocksCompatConfig, *mut std::os::raw::c_char, i32) -> i32,
) -> Result<(), CompatError> {
    let host = CString::new(cfg.host).map_err(|e| CompatError {
        code: -1,
        message: format!("invalid host string: {e}"),
    })?;

    let native_cfg = NovaRocksCompatConfig {
        host: host.as_ptr(),
        heartbeat_port: cfg.heartbeat_port,
        brpc_port: cfg.brpc_port,
        internal_service_query_rpc_thread_num: cfg.internal_service_query_rpc_thread_num,
        debug_exec_batch_plan_json: u8::from(cfg.debug_exec_batch_plan_json),
        log_level: cfg.log_level,
    };

    let mut err_buf = vec![0i8; 512];
    let code = native_start(&native_cfg, err_buf.as_mut_ptr(), err_buf.len() as i32);
    if code == 0 {
        Ok(())
    } else {
        let bytes = err_buf
            .iter()
            .map(|byte| *byte as u8)
            .take_while(|byte| *byte != 0)
            .collect::<Vec<_>>();
        let message = String::from_utf8_lossy(&bytes).trim().to_string();
        Err(CompatError { code, message })
    }
}

pub fn stop() {
    unsafe { novarocks_compat_stop() }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::mem::offset_of;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{CompatConfig, NovaRocksCompatConfig, start_with};

    #[test]
    fn rust_config_preserves_c_field_order_and_bool_mapping() {
        let config = CompatConfig {
            host: "127.0.0.1",
            heartbeat_port: 9050,
            brpc_port: 8060,
            internal_service_query_rpc_thread_num: 17,
            debug_exec_batch_plan_json: true,
            log_level: 2,
        };

        start_with(&config, |native, _, _| {
            assert_eq!(
                unsafe { CStr::from_ptr(native.host) }.to_str().unwrap(),
                "127.0.0.1"
            );
            assert_eq!(native.heartbeat_port, 9050);
            assert_eq!(native.brpc_port, 8060);
            assert_eq!(native.internal_service_query_rpc_thread_num, 17);
            assert_eq!(native.debug_exec_batch_plan_json, 1);
            assert_eq!(native.log_level, 2);
            0
        })
        .expect("native start");

        assert!(
            offset_of!(NovaRocksCompatConfig, host)
                < offset_of!(NovaRocksCompatConfig, heartbeat_port)
        );
        assert!(
            offset_of!(NovaRocksCompatConfig, heartbeat_port)
                < offset_of!(NovaRocksCompatConfig, brpc_port)
        );
        assert!(
            offset_of!(NovaRocksCompatConfig, brpc_port)
                < offset_of!(NovaRocksCompatConfig, internal_service_query_rpc_thread_num)
        );
        assert!(
            offset_of!(NovaRocksCompatConfig, internal_service_query_rpc_thread_num)
                < offset_of!(NovaRocksCompatConfig, debug_exec_batch_plan_json)
        );
        assert!(
            offset_of!(NovaRocksCompatConfig, debug_exec_batch_plan_json)
                < offset_of!(NovaRocksCompatConfig, log_level)
        );
    }

    #[test]
    fn interior_nul_is_rejected_before_native_call() {
        let called = AtomicBool::new(false);
        let config = CompatConfig {
            host: "bad\0host",
            heartbeat_port: 9050,
            brpc_port: 8060,
            internal_service_query_rpc_thread_num: 1,
            debug_exec_batch_plan_json: false,
            log_level: 0,
        };

        let error = start_with(&config, |_, _, _| {
            called.store(true, Ordering::Relaxed);
            0
        })
        .expect_err("interior NUL must fail");

        assert_eq!(error.code, -1);
        assert!(error.to_string().contains("invalid host string"));
        assert!(!called.load(Ordering::Relaxed));
    }

    #[test]
    fn native_error_code_and_buffer_are_propagated() {
        let config = CompatConfig {
            host: "127.0.0.1",
            heartbeat_port: 9050,
            brpc_port: 8060,
            internal_service_query_rpc_thread_num: 1,
            debug_exec_batch_plan_json: false,
            log_level: 0,
        };

        let error = start_with(&config, |_, buffer, buffer_len| {
            let message = b"native bind failed\0";
            assert!(buffer_len as usize >= message.len());
            unsafe {
                std::ptr::copy_nonoverlapping(message.as_ptr().cast(), buffer, message.len());
            }
            41
        })
        .expect_err("native error must fail");

        assert_eq!(error.code, 41);
        assert_eq!(
            error.to_string(),
            "compat start failed (code=41): native bind failed"
        );
    }
}
