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
    ConnectorWriteFieldBinding, ConnectorWriteFieldRef, ConnectorWriteFieldToken,
    ConnectorWriteInputRef,
};
use arrow_schema::{DataType, Field, TimeUnit};
use std::{collections::HashMap, sync::Mutex};

const SOURCE: usize = 32 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after originating refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = self.stop
            && at == stop
        {
            return Err(cause);
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
fn token(id: u8) -> ConnectorWriteFieldToken {
    ConnectorWriteFieldToken::from_bytes([id; 32])
}
fn run(
    input: &ConnectorWriteInputRef<'_>,
    control: &Control,
) -> Result<
    (ConnectorWriteRecipeDraft, WriterOwnedResourceFacts),
    PureProviderCompileError<ConnectorError>,
> {
    let (binding, payload) = envelope();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let mut facts = WriterOwnedResourceFacts::default();
    let result = ConnectorWriteRecipeDraft::try_new_from_borrowed_input_observed(
        &binding,
        &payload,
        input,
        SOURCE,
        &mut |known| {
            facts = *known;
            Ok(())
        },
        &mut work,
    );
    if matches!(&result, Err(PureProviderCompileError::Control(_))) {
        return result.map(|output| (output, facts));
    }
    work.finish()?;
    result.map(|output| (output, facts))
}
fn assert_prefixes(input: &ConnectorWriteInputRef<'_>) {
    let baseline_control = Control::default();
    let _ = run(input, &baseline_control);
    let baseline = baseline_control.trace();
    assert!(baseline.len() >= 2);
    for stop in 0..baseline.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((stop, cause)),
                ..Default::default()
            };
            assert!(
                matches!(run(input, &control), Err(PureProviderCompileError::Control(actual)) if actual == cause),
                "stop={stop} cause={cause:?}"
            );
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}
fn assert_field(actual: &ConnectorWriteFieldBinding, id: u8, expected: &Field) {
    assert_eq!(actual.token(), token(id));
    assert!(crate::arrow_fields_exact(actual.field(), expected));
    assert!(!std::ptr::eq(actual.field(), expected));
}

#[test]
fn borrowed_five_roles_preserve_exact_order_tokens_and_owned_role_boundaries() {
    let a = Field::new("first", DataType::Int64, false);
    let b = Field::new("second", DataType::Utf8, true);
    let first = [ConnectorWriteFieldRef::new(token(1), &a)];
    let second = [ConnectorWriteFieldRef::new(token(2), &b)];
    let both = [
        ConnectorWriteFieldRef::new(token(1), &a),
        ConnectorWriteFieldRef::new(token(2), &b),
    ];
    assert_eq!(first[0].token(), token(1));
    assert!(std::ptr::eq(first[0].field(), &a));
    for role in 0..5 {
        let input = match role {
            0 => ConnectorWriteInputRef::Data { fields: &both },
            1 => ConnectorWriteInputRef::RowLineage {
                data_fields: &first,
                row_identity_fields: &second,
            },
            2 => ConnectorWriteInputRef::PositionDelete {
                identity_fields: &first,
                partition_source_fields: &second,
            },
            3 => ConnectorWriteInputRef::DeletionVector {
                identity_fields: &first,
                partition_source_fields: &second,
            },
            _ => ConnectorWriteInputRef::EqualityDelete {
                equality_fields: &both,
            },
        };
        let (output, facts) = run(&input, &Control::default()).unwrap();
        let (left, right) = match (role, output.input()) {
            (0, ConnectorWriteInputShape::Data { fields }) => (fields.as_slice(), &[][..]),
            (
                1,
                ConnectorWriteInputShape::RowLineage {
                    data_fields,
                    row_identity_fields,
                },
            ) => (data_fields.as_slice(), row_identity_fields.as_slice()),
            (
                2,
                ConnectorWriteInputShape::PositionDelete {
                    identity_fields,
                    partition_source_fields,
                },
            ) => (
                identity_fields.as_slice(),
                partition_source_fields.as_slice(),
            ),
            (
                3,
                ConnectorWriteInputShape::DeletionVector {
                    identity_fields,
                    partition_source_fields,
                },
            ) => (
                identity_fields.as_slice(),
                partition_source_fields.as_slice(),
            ),
            (4, ConnectorWriteInputShape::EqualityDelete { equality_fields }) => {
                (equality_fields.as_slice(), &[][..])
            }
            _ => panic!("original role must be retained"),
        };
        assert_eq!(
            (left.len(), right.len()),
            if role == 0 || role == 4 {
                (2, 0)
            } else {
                (1, 1)
            }
        );
        assert_field(&left[0], 1, &a);
        assert_field(
            if right.is_empty() {
                &left[1]
            } else {
                &right[0]
            },
            2,
            &b,
        );
        assert_eq!(facts.source_retained_bytes, SOURCE);
        assert_eq!(facts.coexistence_bytes, SOURCE + facts.requested_bytes);
    }
}

#[test]
fn borrowed_dictionary_metadata_is_complete_detached_and_survives_source_drop() {
    let output = {
        let mut metadata = HashMap::with_capacity(4096);
        metadata.insert("opaque".into(), "雪".repeat(7000));
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
        let field = Field::new("root", DataType::Struct(vec![child.clone()].into()), false);
        let refs = [ConnectorWriteFieldRef::new(token(7), &field)];
        let (output, _) = run(
            &ConnectorWriteInputRef::Data { fields: &refs },
            &Control::default(),
        )
        .unwrap();
        let actual = output.input().fields_iter().next().unwrap().field();
        assert!(!std::ptr::eq(actual, &field));
        let DataType::Struct(fields) = actual.data_type() else {
            panic!()
        };
        assert!(!Arc::ptr_eq(&fields[0], &child));
        assert!(crate::arrow_fields_exact(&fields[0], &child));
        assert_ne!(
            fields[0].metadata()["opaque"].as_ptr(),
            child.metadata()["opaque"].as_ptr()
        );
        output
    };
    let actual = output.input().fields_iter().next().unwrap();
    assert_eq!(actual.token(), token(7));
    assert_eq!(actual.field().name(), "root");
    assert!(!actual.field().is_nullable());
    let DataType::Struct(fields) = actual.field().data_type() else {
        panic!()
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(
        fields[0].data_type(),
        &DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8))
    );
    #[allow(deprecated)]
    {
        assert_eq!(fields[0].dict_id(), Some(777));
    }
    assert_eq!(fields[0].dict_is_ordered(), Some(true));
    assert_eq!(fields[0].metadata()["opaque"], "雪".repeat(7000));
}

#[test]
fn borrowed_writer_wide_and_timezone_keep_original_constructor_domain() {
    let fields: Vec<_> = (0..5000)
        .map(|i| Arc::new(Field::new(format!("child{i}"), DataType::Int64, false)))
        .collect();
    let wide = Field::new("root", DataType::Struct(fields.into()), false);
    let zone = Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(2000).into())),
        true,
    );
    for field in [&wide, &zone] {
        let refs = [ConnectorWriteFieldRef::new(token(1), field)];
        let (actual, _) = run(
            &ConnectorWriteInputRef::Data { fields: &refs },
            &Control::default(),
        )
        .unwrap();
        let (binding, payload) = envelope();
        let original = ConnectorWriteRecipeDraft::try_new(
            binding,
            payload,
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(token(1), (*field).clone())],
            },
        )
        .unwrap();
        assert_eq!(actual, original);
        assert!(crate::arrow_fields_exact(
            actual.input().fields_iter().next().unwrap().field(),
            field
        ));
    }
    let DataType::Struct(children) = wide.data_type() else {
        panic!()
    };
    assert_eq!(children.len(), 5000);
    // Component source construction does not widen any whole Package Value gate.
}

#[test]
fn borrowed_original_ordinary_priority_and_all_actual_callback_prefixes_are_preserved() {
    let a = Field::new("a", DataType::Int64, false);
    // Duplicate token precedes the later invalid empty field-name law.
    let b = Field::new("", DataType::Utf8, true);
    let duplicate = [
        ConnectorWriteFieldRef::new(token(1), &a),
        ConnectorWriteFieldRef::new(token(1), &b),
    ];
    let input = ConnectorWriteInputRef::Data { fields: &duplicate };
    let failure = run(&input, &Control::default()).unwrap_err();
    let PureProviderCompileError::Provider(error) = failure else {
        panic!()
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "connector write input shape contains a duplicate field token or name"
    );
    let (binding, payload) = envelope();
    let original = ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::new(token(1), a.clone()),
                ConnectorWriteFieldBinding::new(token(1), b.clone()),
            ],
        },
    )
    .unwrap_err();
    assert_eq!(error, original);
    assert_prefixes(&input);
    let empty = ConnectorWriteInputRef::Data { fields: &[] };
    let failure = run(&empty, &Control::default()).unwrap_err();
    let PureProviderCompileError::Provider(error) = failure else {
        panic!()
    };
    assert_eq!(
        error.message(),
        "connector write input shape must contain at least one field"
    );
    assert_prefixes(&empty);
}

#[test]
fn borrowed_success_every_actual_small_callback_keeps_three_primary_causes_and_no_after() {
    let field = Field::new(
        "root",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    );
    let refs = [ConnectorWriteFieldRef::new(token(1), &field)];
    assert_prefixes(&ConnectorWriteInputRef::Data { fields: &refs });
}

#[test]
fn borrowed_one_field_request_inventory_counts_actual_strings_once_and_rejects_before_copy() {
    let short = Field::new("s", DataType::Int64, false);
    let long = Field::new("s".repeat(129), DataType::Int64, false);
    let short_refs = [ConnectorWriteFieldRef::new(token(1), &short)];
    let long_refs = [ConnectorWriteFieldRef::new(token(1), &long)];
    let (_, short_facts) = run(
        &ConnectorWriteInputRef::Data {
            fields: &short_refs,
        },
        &Control::default(),
    )
    .unwrap();
    let (_, long_facts) = run(
        &ConnectorWriteInputRef::Data { fields: &long_refs },
        &Control::default(),
    )
    .unwrap();
    // A real uniqueness-key String plus one materialized Field name. Count's
    // dry schema pass and Copy's schema pass do not request a third name copy.
    assert_eq!(
        long_facts.requested_bytes - short_facts.requested_bytes,
        2 * 128
    );
    assert_eq!(
        long_facts.allocation_requests,
        short_facts.allocation_requests
    );
    // Original role reserve and possible Vec-to-Box trim are two requests.
    assert_eq!(
        short_facts.allocation_requests,
        16 + 2 + 1 + 1 + 1 + 2 + 1 + 1
    );
    assert_eq!(
        short_facts.coexistence_bytes,
        SOURCE + short_facts.requested_bytes
    );
    let baseline_control = Control::default();
    let (binding, payload) = envelope();
    let mut work =
        CompileCheckpoints::try_new(&baseline_control, CompilePhase::ProviderValidation).unwrap();
    let mut before_copy = None;
    ConnectorWriteRecipeDraft::try_new_from_borrowed_input_observed(
        &binding,
        &payload,
        &ConnectorWriteInputRef::Data {
            fields: &short_refs,
        },
        SOURCE,
        &mut |facts| {
            if facts.allocation_requests == short_facts.allocation_requests && before_copy.is_none()
            {
                before_copy = Some(baseline_control.trace().len());
            }
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let baseline = baseline_control.trace();
    let before_copy = before_copy.unwrap();
    assert!(before_copy < baseline.len());
    for cause in CAUSES {
        let control = Control {
            stop: Some((before_copy, cause)),
            ..Default::default()
        };
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
        let result = ConnectorWriteRecipeDraft::try_new_from_borrowed_input_observed(
            &binding,
            &payload,
            &ConnectorWriteInputRef::Data {
                fields: &short_refs,
            },
            SOURCE,
            &mut |facts| {
                if facts.allocation_requests >= short_facts.allocation_requests {
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
        // Count's last known request refuses before the armed next callback,
        // without a caller footer or destruction of the borrowed source field.
        assert_eq!(control.trace(), baseline[..before_copy]);
        assert_eq!(short.name(), "s");
    }
}

// The old concrete owned name must still infer an empty role Vec even when
// only Debug consumes the result, with no constructor/method type context.
#[test]
fn original_owned_shape_empty_role_keeps_context_free_construction() {
    let input = ConnectorWriteInputShape::Data { fields: vec![] };
    let rendered = format!("{input:?}");
    assert_eq!(rendered, "Data { fields: [] }");
}
