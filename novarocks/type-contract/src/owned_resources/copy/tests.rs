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

use super::*;
use crate::{CompilePhase, PureCompileControl};
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

#[derive(Debug, Eq, PartialEq)]
enum CopyError {
    Control(CompileControlError),
}
impl From<CompileControlError> for CopyError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

#[derive(Default)]
struct PrefixControl {
    trace: Mutex<Vec<u32>>,
    failure: Option<(usize, CompileControlError)>,
}
impl PrefixControl {
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            failure: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for PrefixControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if let Some((failure_at, cause)) = self.failure
            && at == failure_at
        {
            return Err(cause);
        }
        Ok(())
    }
}

// This is the actual caller scope; the copy leaf creates neither boundary.
fn copy_in_scope(input: &str, control: &PrefixControl) -> Result<String, CopyError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let output = copy_string::<CopyError>(input, &mut work)?;
    work.finish()?;
    Ok(output)
}

#[test]
fn string_copy_preserves_independent_utf8_nul_bytes_and_caller_owned_tail() {
    let control = PrefixControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let output = copy_string::<CopyError>("a\0中🦀é", &mut work).unwrap();
    // Hand-written UTF-8 byte oracle, independent of the append algorithm.
    assert_eq!(
        output.as_bytes(),
        [97, 0, 228, 184, 173, 240, 159, 166, 128, 195, 169]
    );
    assert_eq!(control.trace(), [0, 0, 1]);
    work.finish().unwrap();
    assert_eq!(control.trace(), [0, 0, 1, 5]);

    let empty = PrefixControl::default();
    assert_eq!(copy_in_scope("", &empty).unwrap(), "");
    assert_eq!(empty.trace(), [0, 0, 1, 0]);
}

#[test]
fn string_copy_every_actual_small_callback_preserves_all_three_primary_causes() {
    let source = "A\0北🦀Z";
    let baseline = PrefixControl::default();
    assert_eq!(copy_in_scope(source, &baseline).unwrap(), "A\0北🦀Z");
    let trace = baseline.trace();
    assert_eq!(trace, [0, 0, 1, 5]);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = PrefixControl::refusing(at, cause);
            assert_eq!(
                copy_in_scope(source, &control),
                Err(CopyError::Control(cause))
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn string_copy_multibyte_boundary_and_wide_actual_quantum_preserve_spelling_and_causes() {
    // The first 256-byte segment ends inside the four-byte crab. The leaf
    // must adjust to a true UTF-8 boundary before copying the 255 ASCII bytes.
    let mut source = "a".repeat(255);
    source.push_str("🦀\0中");
    source.push_str(&"z".repeat(320));
    let baseline = PrefixControl::default();
    let output = copy_in_scope(&source, &baseline).unwrap();
    assert_eq!(output.len(), 583);
    assert!(output.as_bytes()[..255].iter().all(|byte| *byte == b'a'));
    assert_eq!(
        output.as_bytes()[255..263],
        [240, 159, 166, 128, 0, 228, 184, 173]
    );
    assert!(output.as_bytes()[263..].iter().all(|byte| *byte == b'z'));
    let trace = baseline.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    assert_eq!(
        trace.iter().map(|units| *units as usize).sum::<usize>(),
        580
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = PrefixControl::refusing(at, cause);
            assert_eq!(
                copy_in_scope(&source, &control),
                Err(CopyError::Control(cause))
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn reserve_refusal_with_pending_255_never_reaches_the_next_armed_callback() {
    for cause in CAUSES {
        let control = PrefixControl::refusing(1, cause);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let source = [7_u8; 255];
        let mut copied = [0_u8; 255];
        for (source, copied) in source.iter().zip(copied.iter_mut()) {
            // Real caller-owned byte copies precede the library reserve.
            *copied = *source;
            work.step().unwrap();
        }
        assert_eq!(copied, [7_u8; 255]);
        let mut output = Vec::<u8>::new();
        let refusal = output.try_reserve_exact(usize::MAX);
        assert!(refusal.is_err());
        assert_eq!(
            reserve_exit::<CopyError>(refusal, &mut work),
            Err(CopyError::Control(CompileControlError::ResourceExhausted))
        );
        assert_eq!(control.trace(), [0]);
    }
}
