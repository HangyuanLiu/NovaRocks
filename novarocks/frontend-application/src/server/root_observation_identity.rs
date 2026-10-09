// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Default-off independent original-FE observation, with no MySQL gate owner.
use crate::application::FrontendApplicationError;
use novarocks_native_trust::{NativeProcessIdentity, NativeTrust};
use novarocks_types::FrontendProcessId;
use std::io::{self, Write};

pub(super) fn emit(trust: &NativeTrust) -> Result<(), FrontendApplicationError> {
    let Some(NativeProcessIdentity::Frontend(frontend)) = trust.local_process_identity() else {
        return Err(FrontendApplicationError::server(
            "root observation requires actual FE NativeTrust identity",
        ));
    };
    write_marker(&mut io::stdout().lock(), frontend)
        .map_err(FrontendApplicationError::server_root_observation)
}
fn write_marker(output: &mut impl Write, frontend: FrontendProcessId) -> io::Result<()> {
    let mut bytes = [0; 128];
    let length = {
        let mut cursor = io::Cursor::new(&mut bytes[..]);
        writeln!(
            cursor,
            "NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FE frontend_process_id={frontend}"
        )?;
        cursor.position() as usize
    };
    output.write_all(&bytes[..length])?;
    output.flush()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_native_uuid_literal_has_no_gate_or_secret_fields() {
        let mut bytes = Vec::new();
        write_marker(
            &mut bytes,
            "01890f6e-7a00-7123-8123-456789abcdef".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(bytes, b"NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FE frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n");
    }
    #[test]
    fn actual_original_stdout_io_source_is_retained() {
        struct ActualSource;
        impl std::fmt::Debug for ActualSource {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("must not debug original source");
            }
        }
        impl std::fmt::Display for ActualSource {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("must not format original source")
            }
        }
        impl std::error::Error for ActualSource {}
        struct Fail;
        impl Write for Fail {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other(ActualSource))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let io = write_marker(
            &mut Fail,
            "01890f6e-7a00-7123-8123-456789abcdef".parse().unwrap(),
        )
        .unwrap_err();
        let error = FrontendApplicationError::server_root_observation(io);
        let original = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert!(
            original
                .get_ref()
                .unwrap()
                .downcast_ref::<ActualSource>()
                .is_some()
        );
        let debug = format!("{error:?}");
        assert!(debug.contains("source_retained: true"));
        assert!(!debug.contains("ActualSource"));
        assert_eq!(
            error.to_string(),
            "Server: root observation stdout IO failed"
        );
    }
}
