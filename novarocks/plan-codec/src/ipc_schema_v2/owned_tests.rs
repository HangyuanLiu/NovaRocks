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
#[test]
fn parent_schema_writer_and_reader_same_field_without_writer_request_charge() {
    let field = Field::new("original", DataType::Int64, true)
        .with_metadata(HashMap::from([("source".into(), "x\0é".into())]));
    let control = Control::recording();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let token = prepare_schema_writer_in(
        &field,
        1 << 20,
        limits(),
        1 << 30,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let writer = token.facts();
    let bytes = token.emit_in(&mut |_| Ok(()), &mut work).unwrap();
    assert_eq!(decoded_field(&bytes), field);
    let verifier = novarocks_arrow_ipc_frame::VerifierOptions {
        max_depth: 67,
        max_tables: 4096,
        max_apparent_size: 16 << 20,
        ignore_missing_null_terminator: false,
    };
    let mut reader = SchemaWriterRequestFacts::default();
    verify_single_field_schema_in(
        &bytes,
        &field,
        limits(),
        &verifier,
        1 << 20,
        1 << 30,
        &mut |facts| {
            reader = *facts;
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert!(reader.request_bytes < writer.request_bytes);
    assert_eq!(reader.request_count, 1);
    assert!(reader.work_upper_bound > 0);
}
#[test]
fn parent_schema_known_name_resource_wins_late_callback() {
    let field = Field::new("original", DataType::Int64, true);
    for cause in CAUSES {
        let control = Control::refusing(1, cause);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let mut l = limits();
        l.max_string_bytes = field.name().len() - 1;
        assert!(matches!(
            prepare_schema_writer_in(&field, 1 << 20, l, 1 << 30, &mut |_| Ok(()), &mut work),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
    }
}
#[test]
fn parent_schema_reader_success_and_malformed_every_actual_prefix() {
    let field = Field::new("original", DataType::Int64, true);
    let bytes = encode_single_field_schema(&field, limits(), &Control::recording()).unwrap();
    let verifier = novarocks_arrow_ipc_frame::VerifierOptions {
        max_depth: 67,
        max_tables: 4096,
        max_apparent_size: 16 << 20,
        ignore_missing_null_terminator: false,
    };
    for malformed in [false, true] {
        let input = if malformed {
            &bytes[..1]
        } else {
            bytes.as_slice()
        };
        let run = |control: &Control| -> Result<(), TypeCodecError> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
            let outcome = verify_single_field_schema_in(
                input,
                &field,
                limits(),
                &verifier,
                1 << 20,
                1 << 30,
                &mut |_| Ok(()),
                &mut work,
            );
            if matches!(&outcome, Err(TypeCodecError::Control(_))) {
                return outcome;
            }
            work.finish()?;
            outcome
        };
        let control = Control::recording();
        let outcome = run(&control);
        if malformed {
            assert!(matches!(
                outcome,
                Err(TypeCodecError::InvalidShape(
                    "invalid Arrow IPC message metadata"
                ))
            ))
        } else {
            outcome.unwrap();
        }
        let baseline = control.trace();
        for at in 0..baseline.len() {
            for cause in CAUSES {
                let control = Control::refusing(at, cause);
                assert!(
                    matches!(run(&control),Err(TypeCodecError::Control(actual))if actual==cause)
                );
                assert_eq!(control.trace(), baseline[..=at]);
            }
        }
    }
}

#[test]
fn parent_schema_known_verifier_overflow_precedes_pending_control_and_parent() {
    let field = Field::new("original", DataType::Int64, true);
    let bytes = encode_single_field_schema(&field, limits(), &Control::recording()).unwrap();
    assert!(!bytes.is_empty());
    let verifier = novarocks_arrow_ipc_frame::VerifierOptions {
        max_depth: 67,
        max_tables: 4096,
        max_apparent_size: usize::MAX,
        ignore_missing_null_terminator: false,
    };
    for cause in CAUSES {
        let control = Control::refusing(1, cause);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = verify_single_field_schema_in(
            &bytes,
            &field,
            limits(),
            &verifier,
            1 << 20,
            1 << 30,
            &mut |_| panic!("parent after known verifier arithmetic refusal"),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
    }
    let control = Control::recording();
    assert!(matches!(
        verify_single_field_schema_message(&bytes, &field, limits(), &verifier, &control),
        Err(TypeCodecError::InvalidShape("IPC schema extent overflow"))
    ));
}
