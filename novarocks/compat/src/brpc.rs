use std::ffi::CString;
use std::ffi::c_void;
use std::fmt;
use std::os::raw::c_char;

const ERROR_BUFFER_LEN: usize = 512;

#[repr(C)]
struct NovaRocksCompatConfig {
    host: *const c_char,
    heartbeat_port: u16,
    brpc_port: u16,
    internal_service_query_rpc_thread_num: u32,
    debug_exec_batch_plan_json: u8,
    log_level: u8,
    fragment_service_context: *const c_void,
    lake_service_context: *const c_void,
}

unsafe extern "C" {
    fn novarocks_compat_start(
        config: *const NovaRocksCompatConfig,
        error_buffer: *mut c_char,
        error_buffer_len: i32,
    ) -> i32;
    fn novarocks_compat_stop();
}

pub(crate) struct CompatConfig<'a> {
    pub host: &'a str,
    pub heartbeat_port: u16,
    pub brpc_port: u16,
    pub internal_service_query_rpc_thread_num: u32,
    pub debug_exec_batch_plan_json: bool,
    pub log_level: u8,
    pub fragment_service_context: *const c_void,
    pub lake_service_context: *const c_void,
}

#[derive(Debug)]
pub(crate) struct CompatError {
    code: i32,
    message: String,
}

impl CompatError {
    #[cfg(test)]
    fn code(&self) -> i32 {
        self.code
    }
}

impl fmt::Display for CompatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "compat start failed (code={}): {}",
            self.code, self.message
        )
    }
}

pub(crate) fn start(config: &CompatConfig<'_>) -> Result<(), CompatError> {
    start_with(config, |native, error_buffer, error_buffer_len| unsafe {
        novarocks_compat_start(native, error_buffer, error_buffer_len)
    })
}

fn start_with(
    config: &CompatConfig<'_>,
    native_start: impl FnOnce(&NovaRocksCompatConfig, *mut c_char, i32) -> i32,
) -> Result<(), CompatError> {
    let host = CString::new(config.host).map_err(|error| CompatError {
        code: -1,
        message: format!("invalid host string: {error}"),
    })?;
    let native_config = NovaRocksCompatConfig {
        host: host.as_ptr(),
        heartbeat_port: config.heartbeat_port,
        brpc_port: config.brpc_port,
        internal_service_query_rpc_thread_num: config.internal_service_query_rpc_thread_num,
        debug_exec_batch_plan_json: u8::from(config.debug_exec_batch_plan_json),
        log_level: config.log_level,
        fragment_service_context: config.fragment_service_context,
        lake_service_context: config.lake_service_context,
    };
    let mut error_buffer = vec![0 as c_char; ERROR_BUFFER_LEN];
    let code = native_start(
        &native_config,
        error_buffer.as_mut_ptr(),
        error_buffer.len() as i32,
    );
    if code == 0 {
        return Ok(());
    }
    let bytes = error_buffer
        .iter()
        .map(|byte| *byte as u8)
        .take_while(|byte| *byte != 0)
        .collect::<Vec<_>>();
    Err(CompatError {
        code,
        message: String::from_utf8_lossy(&bytes).trim().to_string(),
    })
}

pub(crate) fn stop() {
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
            fragment_service_context: std::ptr::dangling(),
            lake_service_context: std::ptr::dangling(),
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
            assert_eq!(
                native.fragment_service_context,
                config.fragment_service_context
            );
            assert_eq!(native.lake_service_context, config.lake_service_context);
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
        assert!(
            offset_of!(NovaRocksCompatConfig, log_level)
                < offset_of!(NovaRocksCompatConfig, fragment_service_context)
        );
        assert!(
            offset_of!(NovaRocksCompatConfig, fragment_service_context)
                < offset_of!(NovaRocksCompatConfig, lake_service_context)
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
            fragment_service_context: std::ptr::dangling(),
            lake_service_context: std::ptr::dangling(),
        };

        let error = start_with(&config, |_, _, _| {
            called.store(true, Ordering::Relaxed);
            0
        })
        .expect_err("interior NUL must fail");

        assert_eq!(error.code(), -1);
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
            fragment_service_context: std::ptr::dangling(),
            lake_service_context: std::ptr::dangling(),
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

        assert_eq!(error.code(), 41);
        assert_eq!(
            error.to_string(),
            "compat start failed (code=41): native bind failed"
        );
    }
}
