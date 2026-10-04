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
use novarocks_constant_contract::ConstantError;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Mutex;

struct Control(Mutex<Vec<u32>>);
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        self.0.lock().unwrap().push(units);
        Ok(())
    }
}

#[test]
fn dependency_refusals_preserve_the_first_cause_without_an_enclosing_tail() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for error in [
            NodeCodecError::from(ExpressionCodecError::Control(cause)),
            NodeCodecError::from(p::ConstantReferenceError::Control(cause)),
            NodeCodecError::from(p::ConstantReferenceError::Constant(ConstantError::Control(
                cause,
            ))),
        ] {
            let control = Control(Mutex::new(Vec::new()));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            work.step().unwrap();
            assert!(
                matches!(finish::<()>(work, Err(error)), Err(NodeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.0.lock().unwrap(), [0]);
        }
    }
    let control = Control(Mutex::new(Vec::new()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    work.step().unwrap();
    let error =
        p::ConstantReferenceError::Constant(ConstantError::Limit("selected source envelope"));
    assert!(matches!(
        finish::<()>(work, Err(error.into())),
        Err(NodeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*control.0.lock().unwrap(), [0]);
}

#[test]
fn ordinary_dependency_errors_retain_typed_details_and_completed_work() {
    for error in [
        NodeCodecError::from(ExpressionCodecError::InvalidShape(
            "missing scalar expression",
        )),
        NodeCodecError::from(p::ConstantReferenceError::MissingPool(
            p::ConstantPoolId::new(u32::MAX),
        )),
    ] {
        let control = Control(Mutex::new(Vec::new()));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        work.step().unwrap();
        let actual = finish::<()>(work, Err(error)).unwrap_err();
        assert!(std::error::Error::source(&actual).is_some());
        match actual {
            NodeCodecError::Expression(ExpressionCodecError::InvalidShape(message)) => {
                assert_eq!(message, "missing scalar expression")
            }
            NodeCodecError::Constant(p::ConstantReferenceError::MissingPool(id)) => {
                assert_eq!(id.get(), u32::MAX)
            }
            other => panic!("unexpected dependency error: {other:?}"),
        }
        assert_eq!(*control.0.lock().unwrap(), [0, 1]);
    }
}
