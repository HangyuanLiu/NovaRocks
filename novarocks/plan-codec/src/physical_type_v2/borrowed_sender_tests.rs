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
use arrow::datatypes::TimeUnit;
use novarocks_connector_contract as c;
use novarocks_proto_models::plan;
use std::{alloc::Layout, collections::HashMap, sync::Mutex};
use wire::carrier_type_definition::Kind;

// Truthful conservative union invoice for bounded fresh test owners, not an
// introspection of HashMap deleted buckets or a formal host grant.
const SOURCE: usize = 128 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    events: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push(units);
        if let Some((stop, cause)) = self.stop
            && at == stop
        {
            return Err(cause);
        }
        Ok(())
    }
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.events.lock().unwrap().clone()
    }
}
fn limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn strict_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
    }
}
fn recipe(fields: Vec<Field>) -> c::ConnectorWriteRecipeDraft {
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let binding = c::ConnectorWriteBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = c::ConnectorEncodedPayload::new(
        c::ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            c::ConnectorCodecCategory::WriteHandle,
            c::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7u8].into(),
    );
    c::ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        c::ConnectorWriteInputShape::Data {
            fields: fields
                .into_iter()
                .enumerate()
                .map(|(i, field)| {
                    c::ConnectorWriteFieldBinding::new(
                        c::ConnectorWriteFieldToken::from_bytes([u8::try_from(i + 1).unwrap(); 32]),
                        field,
                    )
                })
                .collect(),
        },
    )
    .unwrap()
}
fn borrowed<'a>(
    values: &'a [(u32, &'a FunctionValueType)],
    fields: &'a [(u32, &'a Arc<Field>)],
    writers: &'a [WriterTypeSource<'a>],
    source: usize,
    control: &Control,
) -> Result<EncodedTypeTable<'a>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_borrowed_type_table_writer_sources_in(
        values,
        fields,
        writers,
        source,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    );
    if matches!(result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn field_definition(id: u32, carrier: u32, name: &str, nullable: bool) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: name.into(),
        nullable,
        carrier_type_id: Some(carrier),
        metadata: vec![],
        dictionary_id: None,
        dictionary_is_ordered: None,
    }
}

#[test]
fn borrowed_dictionary_and_strict_arc_preserve_original_loans_and_sparse_repeated_wire() {
    let value = FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
        true,
    );
    let strict = Arc::new(
        Field::new("strict\0", DataType::Int64, false).with_metadata(HashMap::from([
            ("a".into(), "x".repeat(10 * 1024)),
            ("b".into(), "y".repeat(10 * 1024 - 1) + "\0"),
        ])),
    );
    let values = [(0, &value), (u32::MAX, &value)];
    let fields = [(0, &strict), (u32::MAX, &strict)];
    let control = Control::default();
    let original_arc_owners = Arc::strong_count(&strict);
    let encoded = borrowed(&values, &fields, &[], SOURCE, &control).unwrap();
    assert_eq!(Arc::strong_count(&strict), original_arc_owners);
    let owned_values = [(0, value.clone()), (u32::MAX, value.clone())];
    let owned_fields = [(0, strict.clone()), (u32::MAX, strict.clone())];
    let old_control = Control::default();
    let mut old_work = CompileCheckpoints::try_new(&old_control, CompilePhase::Encode).unwrap();
    let old = encode_type_table_writer_sources_observed(
        &owned_values,
        &owned_fields,
        &[],
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut old_work,
    )
    .unwrap();
    old_work.finish().unwrap();
    assert_eq!(encoded.as_wire(), old.as_wire());
    assert_eq!(control.trace(), old_control.trace());
    assert_eq!(
        encoded
            .as_wire()
            .value_types
            .iter()
            .map(|v| v.id)
            .collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    assert_eq!(
        encoded
            .as_wire()
            .fields
            .iter()
            .map(|v| v.id)
            .collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    // Repeated Dictionary roots emit independent three-carrier occurrences.
    assert_eq!(encoded.as_wire().carriers.len(), 8);
    let key_ids: Vec<_> = encoded
        .as_wire()
        .carriers
        .iter()
        .filter_map(|v| match &v.kind {
            Some(Kind::Dictionary(d)) => Some(d.key_type_id),
            _ => None,
        })
        .collect();
    assert_eq!(key_ids.len(), 2);
    assert_ne!(key_ids[0], key_ids[1]);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    for id in [0, u32::MAX] {
        let actual = encoded.value_type_observed(id, &mut work).unwrap().unwrap();
        assert!(std::ptr::eq(actual, &value));
        let (DataType::Dictionary(actual_key, actual_value), DataType::Dictionary(key, inner)) =
            (&actual.data_type, &value.data_type)
        else {
            panic!("Dictionary source");
        };
        assert!(std::ptr::eq(actual_key.as_ref(), key.as_ref()));
        assert!(std::ptr::eq(actual_value.as_ref(), inner.as_ref()));
        let actual_field = encoded.field_observed(id, &mut work).unwrap().unwrap();
        assert!(std::ptr::eq(actual_field, &strict));
        assert!(Arc::ptr_eq(actual_field, &strict));
        assert_eq!(
            actual_field.metadata()["a"].len() + actual_field.metadata()["b"].len(),
            20 * 1024
        );
        assert!(actual_field.metadata()["b"].ends_with('\0'));
    }
    work.finish().unwrap();
    // Existing plain source API remains valid for the same strict roots.
    let plain = encode_type_table_sources(
        &owned_values,
        &owned_fields,
        strict_limits(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(plain.as_wire(), encoded.as_wire());
}

#[test]
fn borrowed_writer_recipe_retains_permissive_original_fields_across_ordinals() {
    let recipe = recipe(vec![
        Field::new("writer\0", DataType::Int64, false)
            .with_metadata(HashMap::from([("large".into(), "雪".repeat(6826) + "ab")])),
        Field::new(
            "clock",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(2000).into())),
            true,
        ),
    ]);
    let ids = [0, u32::MAX];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let encoded = borrowed(&[], &[], &writers, SOURCE, &Control::default()).unwrap();
    let mut expected_first = field_definition(0, 0, "writer\0", false);
    expected_first.metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "large".into(),
        value: "雪".repeat(6826) + "ab",
    }];
    assert_eq!(
        encoded.as_wire(),
        &wire::TypeTable {
            carriers: vec![
                wire::CarrierTypeDefinition {
                    id: 0,
                    kind: Some(Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32))
                },
                wire::CarrierTypeDefinition {
                    id: 1,
                    kind: Some(Kind::Timestamp(plan::ArrowTimestampType {
                        unit: plan::ArrowTimeUnit::Nanosecond as i32,
                        timezone: Some("z".repeat(2000))
                    }))
                },
            ],
            fields: vec![expected_first, field_definition(u32::MAX, 1, "clock", true)],
            value_types: vec![],
        }
    );
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    for (id, binding) in ids.iter().zip(recipe.input().fields_iter()) {
        let actual = encoded
            .field_source_observed(*id, &mut work)
            .unwrap()
            .unwrap();
        assert!(std::ptr::eq(actual, binding.field()));
        assert!(encoded.field_observed(*id, &mut work).unwrap().is_none());
    }
    work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let old = encode_type_table_writer_sources_observed(
        &[],
        &[],
        &writers,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(old.as_wire(), encoded.as_wire());
    let strict = [(
        0,
        Arc::new(recipe.input().fields_iter().next().unwrap().field().clone()),
    )];
    assert!(matches!(
        encode_type_table_sources(&[], &strict, strict_limits(), &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow field metadata entry exceeds its owner bound"
        ))
    ));
}

#[test]
fn borrowed_actual_success_and_ordinary_callback_prefixes_preserve_original_causes() {
    let value = FunctionValueType::new(DataType::Int64, false);
    let valid = Arc::new(
        Field::new("v", DataType::Int8, true)
            .with_metadata(HashMap::from([("key".into(), "x\0雪".into())])),
    );
    let invalid = Arc::new(
        Field::new("v", DataType::Int8, true)
            .with_metadata(HashMap::from([("key".into(), "x".repeat(16 * 1024 + 1))])),
    );
    let values = [(u32::MAX, &value)];
    for field in [&valid, &invalid] {
        let fields = [(0, field)];
        let control = Control::default();
        let outcome = borrowed(&values, &fields, &[], SOURCE, &control);
        if std::ptr::eq(field, &valid) {
            assert!(outcome.is_ok());
        } else {
            assert!(matches!(
                outcome,
                Err(TypeCodecError::InvalidShape(
                    "Arrow field metadata entry exceeds its owner bound"
                ))
            ));
        }
        let trace = control.trace();
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause)),
                    ..Default::default()
                };
                assert!(
                    matches!(borrowed(&values,&fields,&[],SOURCE,&control),Err(TypeCodecError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn borrowed_known_initial_parent_refusal_precedes_pending_actual_copy_observation() {
    let value = FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
        false,
    );
    let values = [(0, &value)];
    for cause in CAUSES {
        let control = Control {
            stop: Some((3, cause)),
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let source = "x".repeat(255);
        assert_eq!(
            novarocks_type_contract::owned_resources::copy::copy_string::<CompileControlError>(
                &source, &mut work
            )
            .unwrap(),
            source
        );
        assert_eq!(control.trace(), [0, 0, 1]);
        let mut admitted = 0;
        let outcome = encode_borrowed_type_table_writer_sources_in(
            &values,
            &[],
            &[],
            SOURCE,
            limits(),
            &mut |facts| {
                admitted += 1;
                assert!(facts.allocation_requests_upper_bound > 0);
                assert!(facts.allocation_request_bytes_upper_bound > 0);
                assert!(facts.cumulative_work_upper_bound > 0);
                Err(CompileControlError::ResourceExhausted)
            },
            &mut work,
        );
        assert!(matches!(
            outcome,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(admitted, 1);
        assert_eq!(control.trace(), [0, 0, 1]);
    }
}

#[test]
fn borrowed_tuple_floor_uses_actual_loan_geometry_and_preserves_owned_legal_baseline() {
    let value = FunctionValueType::new(DataType::Int64, false);
    let values: [(u32, &FunctionValueType); 128] =
        std::array::from_fn(|i| (u32::try_from(i).unwrap(), &value));
    let empty_fields: [(u32, &Arc<Field>); 0] = [];
    let source = Layout::array::<(u32, &FunctionValueType)>(values.len())
        .unwrap()
        .size()
        + size_of::<FunctionValueType>();
    assert_eq!(
        ValueRootSources::Borrowed(&values)
            .payload_layout()
            .unwrap(),
        Layout::array::<(u32, &FunctionValueType)>(128).unwrap()
    );
    assert_eq!(
        FieldRootSources::Borrowed(&empty_fields)
            .payload_layout()
            .unwrap(),
        Layout::array::<(u32, &Arc<Field>)>(0).unwrap()
    );
    let encoded = borrowed(&values, &[], &[], source, &Control::default()).unwrap();
    assert_eq!(encoded.as_wire().value_types.len(), 128);
    assert_eq!(encoded.as_wire().carriers.len(), 128);
    // This explicit negative understates the independently known original
    // borrowed-tuple payload by exactly one, not an arbitrary guessed B.
    let tuple_floor = Layout::array::<(u32, &FunctionValueType)>(128)
        .unwrap()
        .size();
    for cause in CAUSES {
        let control = Control {
            stop: Some((1, cause)),
            ..Default::default()
        };
        assert!(matches!(
            borrowed(&values, &[], &[], tuple_floor - 1, &control),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [0]);
    }
    let owned_values: Vec<_> = values
        .iter()
        .map(|(id, value)| (*id, (*value).clone()))
        .collect();
    let owned_floor = Layout::array::<(u32, FunctionValueType)>(owned_values.len())
        .unwrap()
        .size();
    assert!(owned_floor > source);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let owned_source = Layout::array::<(u32, FunctionValueType)>(owned_values.capacity())
        .unwrap()
        .size()
        + size_of_val(&owned_values);
    let old = encode_type_table_writer_sources_observed(
        &owned_values,
        &[],
        &[],
        owned_source,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(old.as_wire(), encoded.as_wire());
}
