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
use crate::{KernelFailure, kernel_control::KernelDiagnostic};
#[test]
fn append_trailing_shared_original_predicate_renderer_and_lazy_suffix() {
    for (s, ch, expected) in [
        ("", "c", Some("")),
        ("a", "c", Some("ac")),
        ("ac", "c", Some("ac")),
        ("é中", ".", Some("é中.")),
        ("x\0", "\0", Some("x\0")),
        ("a", "", None),
        ("a", "xy", None),
        ("a", "中", None),
    ] {
        assert_eq!(
            plan(s, ch).map(render_original),
            expected.map(str::to_owned)
        );
    }
    let mut steps = 0;
    assert!(
        plan_observed(
            "中",
            || panic!("invalid suffix must not read text"),
            || {
                steps += 1;
                Ok::<_, ()>(())
            }
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(steps, 1);
}
#[test]
fn append_trailing_shared_predicate_observer_preserves_all_seven_first_causes() {
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("input")),
        KernelFailure::Operational(KernelDiagnostic::new("operational")),
        KernelFailure::InstanceFailed,
        KernelFailure::Internal(KernelDiagnostic::new("internal")),
    ];
    for cause in causes {
        for stop in 0..2 {
            let mut callbacks = 0;
            let actual = plan_observed(
                "c",
                || "a",
                || {
                    let at = callbacks;
                    callbacks += 1;
                    if at == stop {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert_eq!(actual, cause);
            assert_eq!(callbacks, stop + 1);
        }
    }
}

#[test]
fn append_trailing_shared_original_writer_order_capacity_and_failure_prefix() {
    struct Writer {
        trace: Vec<String>,
        stop: Option<usize>,
    }
    impl OutputWriter for Writer {
        type Output = Vec<String>;
        type Error = Vec<String>;
        fn unchanged(mut self, s: &str) -> Result<Self::Output, Self::Error> {
            self.trace.push(format!("unchanged:{s}"));
            if self.stop == Some(0) {
                Err(self.trace)
            } else {
                Ok(self.trace)
            }
        }
        fn begin_append(mut self, n: usize) -> Result<Self, Self::Error> {
            self.trace.push(format!("capacity:{n}"));
            if self.stop == Some(0) {
                Err(self.trace)
            } else {
                Ok(self)
            }
        }
        fn push_str(&mut self, s: &str) -> Result<(), Self::Error> {
            let at = self.trace.len();
            self.trace.push(format!("push:{s}"));
            if self.stop == Some(at) {
                Err(self.trace.clone())
            } else {
                Ok(())
            }
        }
        fn finish(self) -> Result<Self::Output, Self::Error> {
            Ok(self.trace)
        }
    }
    let full = vec![
        "capacity:6".to_owned(),
        "push:é中".to_owned(),
        "push:.".to_owned(),
    ];
    assert_eq!(
        render_with(
            plan("é中", ".").unwrap(),
            Writer {
                trace: vec![],
                stop: None
            }
        )
        .unwrap(),
        full
    );
    for stop in 0..3 {
        assert_eq!(
            render_with(
                plan("é中", ".").unwrap(),
                Writer {
                    trace: vec![],
                    stop: Some(stop)
                }
            )
            .unwrap_err(),
            full[..=stop]
        );
    }
    assert_eq!(
        render_with(
            plan("", ".").unwrap(),
            Writer {
                trace: vec![],
                stop: None
            }
        )
        .unwrap(),
        vec!["unchanged:".to_owned()]
    );
    assert_eq!(
        render_with(
            plan("a.", ".").unwrap(),
            Writer {
                trace: vec![],
                stop: None
            }
        )
        .unwrap(),
        vec!["unchanged:a.".to_owned()]
    );
}
