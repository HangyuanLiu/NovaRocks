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

use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::collections::TryReserveError;

/// A captured allocation refusal is already the originating resource cause.
/// Observe the opaque exit only after success; a later cancellation must not
/// replace an actual resource failure. Request admission belongs to the caller.
pub(crate) fn reserve_exit<E: From<CompileControlError>>(
    result: Result<(), TryReserveError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    result.map_err(|_| E::from(CompileControlError::ResourceExhausted))?;
    work.flush().map_err(E::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    struct Control {
        trace: Mutex<Vec<u32>>,
        later_cause: Option<CompileControlError>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Encode);
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            if trace.len() > 1
                && let Some(cause) = self.later_cause
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];

    #[test]
    fn real_reserve_refusal_is_primary_before_any_later_exit_callback() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                later_cause: Some(cause),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            work.step().unwrap();
            // Capture an actual standard-library capacity refusal; caller
            // layout/preflight tests independently reject such source counts.
            let mut values = Vec::<u8>::new();
            let refused = values.try_reserve_exact(usize::MAX);
            assert!(refused.is_err());
            assert_eq!(
                reserve_exit::<CompileControlError>(refused, &mut work),
                Err(CompileControlError::ResourceExhausted)
            );
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
    #[test]
    fn successful_reserve_observes_original_exit_control_and_units() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                later_cause: Some(cause),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            work.step().unwrap();
            let mut values = Vec::<u8>::new();
            let reserved = values.try_reserve_exact(1);
            assert_eq!(
                reserve_exit::<CompileControlError>(reserved, &mut work),
                Err(cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), [0, 1]);
            assert!(values.capacity() >= 1);
        }
    }
}
