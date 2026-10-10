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
use crate::{
    CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
    ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
};
use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if let Some((index, error)) = self.stop
            && at == index
        {
            return Err(error);
        }
        Ok(())
    }
}
fn envelope() -> (ConnectorWriteBinding, ConnectorEncodedPayload) {
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes::Bytes::from(vec![42; 777]),
    );
    (binding, payload)
}
fn field(id: u8, data_type: DataType) -> ConnectorWriteFieldBinding {
    ConnectorWriteFieldBinding::new(
        ConnectorWriteFieldToken::from_bytes([id; 32]),
        Field::new(format!("field{id}"), data_type, true),
    )
}
fn construct(
    input: &ConnectorWriteInputShape,
    control: &Control,
) -> (ConnectorWriteRecipeDraft, WriterOwnedResourceFacts) {
    let (binding, payload) = envelope();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation).unwrap();
    let mut facts = WriterOwnedResourceFacts::default();
    let output = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        input,
        32 * 1024 * 1024,
        &mut |known| {
            facts = *known;
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    (output, facts)
}
#[test]
fn original_five_roles_and_charged_invoice_survive_observed_construction() {
    let fields = vec![field(1, DataType::Int32)];
    let second = vec![field(2, DataType::Utf8)];
    let shapes = [
        ConnectorWriteInputShape::Data {
            fields: fields.clone(),
        },
        ConnectorWriteInputShape::EqualityDelete {
            equality_fields: fields.clone(),
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields: fields.clone(),
            row_identity_fields: second.clone(),
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields: fields.clone(),
            partition_source_fields: second.clone(),
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: fields,
            partition_source_fields: second,
        },
    ];
    for input in shapes {
        let (binding, payload) = envelope();
        let expected = ConnectorWriteRecipeDraft::try_new(binding, payload, input.clone()).unwrap();
        let (actual, facts) = construct(&input, &Control::default());
        assert_eq!(actual, expected);
        assert_eq!(actual.input(), &input);
        assert!(facts.allocation_requests > 16);
        assert_eq!(
            facts.coexistence_bytes,
            facts.source_retained_bytes + facts.requested_bytes
        );
        assert!(facts.work_units > 0);
    }
}
#[test]
fn count_then_copy_preserves_dictionary_and_detaches_nested_metadata() {
    let mut metadata = HashMap::with_capacity(4096);
    metadata.insert("opaque".to_owned(), "v".repeat(64 * 1024));
    #[allow(deprecated)]
    let child = Arc::new(
        Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
            777,
            true,
        )
        .with_metadata(metadata),
    );
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(
            1,
            DataType::Struct(Fields::from(vec![child.clone()])),
        )],
    };
    let (actual, _) = construct(&input, &Control::default());
    let DataType::Struct(fields) = actual
        .input()
        .fields_iter()
        .next()
        .unwrap()
        .field()
        .data_type()
    else {
        panic!()
    };
    assert!(!Arc::ptr_eq(&fields[0], &child));
    assert!(crate::arrow_fields_exact(&fields[0], &child));
    assert!(fields[0].metadata().capacity() < child.metadata().capacity());
    assert_eq!(fields[0].metadata()["opaque"].capacity(), 64 * 1024);
}
#[test]
fn writer_wide_struct_keeps_its_original_domain() {
    let children: Vec<_> = (0..5000)
        .map(|id| Arc::new(Field::new(format!("child{id}"), DataType::Int64, false)))
        .collect();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Struct(children.into()))],
    };
    let (actual, facts) = construct(&input, &Control::default());
    let DataType::Struct(fields) = actual
        .input()
        .fields_iter()
        .next()
        .unwrap()
        .field()
        .data_type()
    else {
        panic!()
    };
    assert_eq!(fields.len(), 5000);
    assert!(facts.allocation_requests >= 10_000);
}
#[test]
fn numeric_known_request_refusal_precedes_late_control() {
    let control = Control {
        stop: Some((1, CompileControlError::Cancelled)),
        ..Default::default()
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let (binding, payload) = envelope();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Int64)],
    };
    let result = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        1024 * 1024,
        &mut |facts| {
            if facts.allocation_requests > 16 {
                Err(CompileControlError::ResourceExhausted)
            } else {
                Ok(())
            }
        },
        &mut work,
    );
    assert!(matches!(
        result,
        Err(PureProviderCompileError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
}
#[test]
fn every_constructor_checkpoint_preserves_all_three_control_causes_without_a_tail() {
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(
            1,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )],
    };
    let control = Control::default();
    let (binding, payload) = envelope();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        1024 * 1024,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let calls = control.trace.lock().unwrap().len();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for index in 1..calls {
            let control = Control {
                stop: Some((index, cause)),
                ..Default::default()
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
            let result = ConnectorWriteRecipeDraft::try_new_observed(
                &binding,
                &payload,
                &input,
                1024 * 1024,
                &mut |_| Ok(()),
                &mut work,
            );
            assert!(
                matches!(result, Err(PureProviderCompileError::Control(error)) if error == cause),
                "index {index}"
            );
            assert_eq!(control.trace.lock().unwrap().len(), index + 1);
        }
    }
}
#[test]
fn original_contract_errors_remain_provider_errors_with_caller_owned_footer() {
    let (binding, payload) = envelope();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Int64), field(1, DataType::Utf8)],
    };
    let failure = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        1024 * 1024,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap_err();
    let PureProviderCompileError::Provider(error) = failure else {
        panic!()
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "connector write input shape contains a duplicate field token or name"
    );
    let before = control.trace.lock().unwrap().len();
    work.finish().unwrap();
    assert_eq!(control.trace.lock().unwrap().len(), before + 1);
}
#[test]
fn original_union_identity_author_runs_after_child_copy_without_a_shadow_gate() {
    let fields: UnionFields = [
        (1, Arc::new(Field::new("a", DataType::Int32, false))),
        (1, Arc::new(Field::new("b", DataType::Int32, false))),
    ]
    .into_iter()
    .collect();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Union(fields, UnionMode::Dense))],
    };
    let (binding, payload) = envelope();
    let expected =
        ConnectorWriteRecipeDraft::try_new(binding.clone(), payload.clone(), input.clone())
            .unwrap_err();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let mut facts = WriterOwnedResourceFacts::default();
    let failure = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        1024 * 1024,
        &mut |known| {
            facts = *known;
            Ok(())
        },
        &mut work,
    )
    .unwrap_err();
    let PureProviderCompileError::Provider(error) = failure else {
        panic!()
    };
    assert_eq!(error, expected);
    assert_eq!(
        error.message(),
        "frozen schema has invalid union field identities"
    );
    assert!(facts.allocation_requests > 20);
}
#[test]
fn deleted_empty_metadata_uses_real_constant_iteration_work() {
    let mut deleted = HashMap::with_capacity(16384);
    deleted.insert("unused".to_owned(), "value".to_owned());
    deleted.clear();
    assert!(deleted.capacity() > 10000);
    let normal = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Int64)],
    };
    let retained = ConnectorWriteInputShape::Data {
        fields: vec![ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            Field::new("field1", DataType::Int64, true).with_metadata(deleted),
        )],
    };
    let (_, normal_facts) = construct(&normal, &Control::default());
    let (_, deleted_facts) = construct(&retained, &Control::default());
    assert_eq!(normal_facts.work_units, deleted_facts.work_units);
    assert_eq!(normal_facts.requested_bytes, deleted_facts.requested_bytes);
}

#[test]
fn source_invoice_understatement_is_not_a_control_text_classifier() {
    let (binding, payload) = envelope();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Int64)],
    };
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let failure = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        0,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap_err();
    let PureProviderCompileError::Provider(error) = failure else {
        panic!()
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "writer retained source invoice is understated"
    );
}
#[test]
fn coexistence_overflow_is_typed_resource_before_any_late_checkpoint() {
    let (binding, payload) = envelope();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![field(1, DataType::Int64)],
    };
    let control = Control {
        stop: Some((1, CompileControlError::DeadlineExceeded)),
        ..Default::default()
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let failure = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        usize::MAX,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap_err();
    assert!(matches!(
        failure,
        PureProviderCompileError::Control(CompileControlError::ResourceExhausted)
    ));
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
}
#[test]
fn original_writer_law_resource_error_stays_ordinary_provider_error() {
    let (binding, payload) = envelope();
    let input = ConnectorWriteInputShape::Data {
        fields: vec![ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            Field::new("x".repeat(1025), DataType::Int64, false),
        )],
    };
    let expected =
        ConnectorWriteRecipeDraft::try_new(binding.clone(), payload.clone(), input.clone())
            .unwrap_err();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let failure = ConnectorWriteRecipeDraft::try_new_observed(
        &binding,
        &payload,
        &input,
        1024 * 1024,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap_err();
    let PureProviderCompileError::Provider(error) = failure else {
        panic!()
    };
    assert_eq!(error, expected);
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
}
#[test]
fn before_field_and_type_events_precede_original_work_and_partial_charges() {
    let input = Field::new("valid", DataType::Int64, false);
    let mut charged = 0;
    let failure = validate_write_field_schema_events::<PureProviderCompileError<ConnectorError>>(
        &input,
        1,
        &mut charged,
        |event| match event {
            WriteSchemaVisit::BeforeField(_) => Err(CompileControlError::Cancelled.into()),
            _ => panic!("before-field refusal must stop the original traversal"),
        },
    )
    .unwrap_err();
    assert!(matches!(
        failure,
        PureProviderCompileError::Control(CompileControlError::Cancelled)
    ));
    assert_eq!(charged, 0);
    let mut charged = 0;
    let mut completed = 0;
    let failure = validate_write_field_schema_events::<PureProviderCompileError<ConnectorError>>(
        &input,
        1,
        &mut charged,
        |event| match event {
            WriteSchemaVisit::BeforeType(_) => Err(CompileControlError::DeadlineExceeded.into()),
            WriteSchemaVisit::Completed => {
                completed += 1;
                Ok(())
            }
            _ => Ok(()),
        },
    )
    .unwrap_err();
    assert!(matches!(
        failure,
        PureProviderCompileError::Control(CompileControlError::DeadlineExceeded)
    ));
    assert_eq!(charged, WRITE_FIELD_ALLOCATION_CHARGE + "valid".len());
    assert_eq!(completed, 1);
}

#[test]
fn captured_actual_reserve_refusal_keeps_resource_before_every_late_cause() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            stop: Some((1, cause)),
            ..Default::default()
        };
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
        let mut admit = |_: &WriterOwnedResourceFacts| Ok(());
        let mut context = ObservedCopy::new(1024 * 1024, &mut admit, &mut work).unwrap();
        // This executes the actual std fallible reserve and captures its
        // unrepresentable-capacity refusal, without allocating a giant buffer.
        let reservation = Vec::<u8>::new().try_reserve(usize::MAX);
        assert!(reservation.is_err());
        let failure = context.reserve_exit(reservation).unwrap_err();
        assert!(matches!(
            failure,
            PureProviderCompileError::Control(CompileControlError::ResourceExhausted)
        ));
        assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    }
}
