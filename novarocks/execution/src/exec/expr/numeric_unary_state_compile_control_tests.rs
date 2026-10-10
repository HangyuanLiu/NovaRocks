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

//! Every real compile checkpoint on one fresh source transaction preserves cause.
use novarocks_sql::compiler::{
    SqlCompileError, SqlPhysicalEmissionMode, ordinary_union_source_for_test,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Mutex;
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((at, _)) = self.stop {
            assert!(ordinal <= at, "callback after first real refusal");
        }
        trace.push((phase, units));
        match self.stop {
            Some((at, cause)) if ordinal == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn check(sum: Option<(i64, i64, i64)>, intermediate: bool) {
    let success = Control {
        trace: Mutex::new(Vec::new()),
        stop: None,
    };
    ordinary_union_source_for_test(
        sum,
        intermediate,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        &success,
    )
    .unwrap();
    let trace = success.trace.into_inner().unwrap();
    assert!(!trace.is_empty());
    for ordinal in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = Control {
                trace: Mutex::new(Vec::new()),
                stop: Some((ordinal, cause)),
            };
            let result = ordinary_union_source_for_test(
                sum,
                intermediate,
                SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
                &refused,
            );
            let actual = match result {
                Ok(_) => panic!("publication after a real refusal"),
                Err(error) => error,
            };
            let expected = match cause {
                CompileControlError::Cancelled => SqlCompileError::Cancelled,
                CompileControlError::DeadlineExceeded => SqlCompileError::DeadlineExceeded,
                CompileControlError::ResourceExhausted => SqlCompileError::ResourceExhausted,
            };
            assert_eq!(
                actual, expected,
                "actual compile callback {ordinal} phase {:?}",
                trace[ordinal]
            );
            let actual_trace = refused.trace.into_inner().unwrap();
            assert_eq!(
                actual_trace,
                trace[..=ordinal],
                "no tail or reset after first refusal"
            );
        }
    }
}
#[test]
fn numeric_unary_exact_state_compile_control_count_union_every_callback() {
    check(None, false);
}
#[test]
fn numeric_unary_exact_state_compile_control_count_multistage_every_callback() {
    check(None, true);
}
#[test]
fn numeric_unary_exact_state_compile_control_sum_distinct_cv_every_callback() {
    check(Some((1, 2, 3)), true);
}
