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

//! Root presentation/scalar contracts remain independent output facts. Their
//! DTOs borrow the genuine source vocabulary and bind its exact slot occurrences.
//! No source type, metadata or presentation label creates execution semantics.
use super::{TypeEncodingResources, TypeEncodingResult};
use novarocks_proto_models::result as wire;
use novarocks_result_contract::{
    FrozenRootOutput, NativeRenderType, OpaqueRenderType, RenderField, RenderPresentation,
    RenderTimeUnit, RootOutputContract, ScalarField, ScalarOpaqueType as O,
    ScalarTimestampUnit as U, ScalarValueType as S,
};
use novarocks_type_contract::CompileControlError;

pub(crate) fn encode<H>(
    source: &RootOutputContract,
    slots: &[u32],
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<wire::RootOutputContract, H> {
    source
        .validate_purpose()
        .map_err(|error| error.to_string())?;
    let mut output = wire::RootOutputContract {
        profile_id: source.profile().get(),
        output_kind: Some(novarocks_proto_codec::root_result::encode_kind(
            source.kind(),
        )),
        scalar_schema: None,
        client_schema: None,
    };
    match source.output() {
        FrozenRootOutput::ClientRows(schema) => {
            if slots.len() > novarocks_result_contract::RootProfileV1::MAX_COLUMNS {
                return Err("root render input exceeds the profile column bound".into());
            }
            let mut count = 0usize;
            for column in schema.columns() {
                count_render(&column.field, &mut count, resources)?;
            }
            let mut nodes = resources.vector(count)?;
            let mut columns = resources.vector(schema.columns().len())?;
            for column in schema.columns() {
                resources.work.step()?;
                let slot = *slots
                    .get(column.source_ordinal as usize)
                    .ok_or("root render source ordinal is outside the exact input layout")?;
                if column.source_slot.is_some_and(|old| old != slot) {
                    return Err(
                        "root render source slot disagrees with the exact input layout".into(),
                    );
                }
                columns.push(wire::RenderColumn {
                    source_ordinal: column.source_ordinal,
                    source_slot: Some(slot),
                    name: resources.copy_string(&column.name)?,
                    field_node_id: encode_render_field(&column.field, &mut nodes, resources)?,
                });
            }
            output.client_schema = Some(wire::ClientRenderSchema {
                columns,
                field_nodes: nodes,
            });
        }
        FrozenRootOutput::ScalarValue(schema) => {
            let [slot] = slots else {
                return Err("scalar root requires exactly one input occurrence".into());
            };
            if schema.source_slot().is_some_and(|old| old != *slot) {
                return Err("scalar root source slot disagrees with the exact input layout".into());
            }
            let mut count = 0usize;
            count_scalar(schema.field(), &mut count, resources)?;
            let mut nodes = resources.vector(count)?;
            let root_field_node_id = encode_scalar_field(schema.field(), &mut nodes, resources)?;
            output.scalar_schema = Some(wire::ScalarSchema {
                source_slot: Some(*slot),
                root_field_node_id,
                field_nodes: nodes,
            });
        }
        _ => {}
    }
    Ok(output)
}
fn increment<H>(
    count: &mut usize,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(), H> {
    resources.work.step()?;
    *count = count
        .checked_add(1)
        .filter(|count| *count <= u32::MAX as usize)
        .ok_or(CompileControlError::ResourceExhausted)?;
    Ok(())
}
fn count_render<H>(
    field: &RenderField,
    count: &mut usize,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(), H> {
    increment(count, resources)?;
    match &field.native_type {
        NativeRenderType::List(child) => count_render(child, count, resources)?,
        NativeRenderType::Map { key, value } => {
            count_render(key, count, resources)?;
            count_render(value, count, resources)?;
        }
        NativeRenderType::Struct(fields) => {
            for child in fields {
                count_render(&child.field, count, resources)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn count_scalar<H>(
    field: &ScalarField,
    count: &mut usize,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(), H> {
    increment(count, resources)?;
    match &field.value_type {
        S::List(child) => count_scalar(child, count, resources)?,
        S::Map { key, value } => {
            count_scalar(key, count, resources)?;
            count_scalar(value, count, resources)?;
        }
        S::Struct(fields) => {
            for child in fields {
                count_scalar(&child.field, count, resources)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn encode_render_field<H>(
    field: &RenderField,
    nodes: &mut Vec<wire::RenderField>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<u32, H> {
    resources.work.step()?;
    let id = (nodes.len() + 1) as u32;
    nodes.push(wire::RenderField::default());
    let presentation = match field.presentation {
        RenderPresentation::ScalarText => wire::RenderPresentation::ScalarText,
        RenderPresentation::TimestampUtcMicros => wire::RenderPresentation::TimestampUtcMicros,
        RenderPresentation::TimestampContainerText => {
            wire::RenderPresentation::TimestampContainerText
        }
        RenderPresentation::JsonText => wire::RenderPresentation::JsonText,
        RenderPresentation::VariantSerializedBytes => {
            wire::RenderPresentation::VariantSerializedBytes
        }
        RenderPresentation::VariantJson { .. } => wire::RenderPresentation::VariantJson,
        RenderPresentation::OpaqueNull => wire::RenderPresentation::OpaqueNull,
        RenderPresentation::MysqlContainer => wire::RenderPresentation::MysqlContainer,
    };
    let native_type = encode_render_type(&field.native_type, nodes, resources)?;
    nodes[id as usize - 1] = wire::RenderField {
        nullable: field.nullable,
        native_type: Some(native_type),
        presentation: presentation as i32,
        variant_json_timezone_offset_seconds: match field.presentation {
            RenderPresentation::VariantJson {
                timezone_offset_seconds,
            } => Some(timezone_offset_seconds),
            _ => None,
        },
    };
    Ok(id)
}
fn encode_render_type<H>(
    ty: &NativeRenderType,
    nodes: &mut Vec<wire::RenderField>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<wire::NativeRenderType, H> {
    resources.work.step()?;
    use wire::RenderTypeKind as K;
    let mut out = wire::NativeRenderType::default();
    let kind = match ty {
        NativeRenderType::Null => K::NullValue,
        NativeRenderType::Boolean => K::Boolean,
        NativeRenderType::SignedInteger(bits) => {
            out.bits = u32::from(*bits);
            K::SignedInteger
        }
        NativeRenderType::UnsignedInteger(bits) => {
            out.bits = u32::from(*bits);
            K::UnsignedInteger
        }
        NativeRenderType::LargeInt => K::LargeInt,
        NativeRenderType::Float32 => K::Float32,
        NativeRenderType::Float64 => K::Float64,
        NativeRenderType::Decimal {
            bits,
            precision,
            scale,
        } => {
            out.bits = u32::from(*bits);
            out.precision = u32::from(*precision);
            out.scale = i32::from(*scale);
            K::Decimal
        }
        NativeRenderType::String => K::String,
        NativeRenderType::Binary => K::Binary,
        NativeRenderType::Date => K::Date,
        NativeRenderType::Time { unit } => {
            out.time_unit = encode_time_unit(*unit);
            K::Time
        }
        NativeRenderType::Timestamp { unit, timezone } => {
            out.time_unit = encode_time_unit(*unit);
            out.time_zone = timezone
                .as_deref()
                .map(|zone| resources.copy_string(zone))
                .transpose()?;
            K::Timestamp
        }
        NativeRenderType::Json => K::Json,
        NativeRenderType::Variant => K::Variant,
        NativeRenderType::Opaque(kind) => {
            out.opaque_kind = match kind {
                OpaqueRenderType::Hll => wire::OpaqueKind::Hll,
                OpaqueRenderType::Bitmap => wire::OpaqueKind::Bitmap,
                OpaqueRenderType::Object => wire::OpaqueKind::Object,
                OpaqueRenderType::Percentile => wire::OpaqueKind::Percentile,
            } as i32;
            K::Opaque
        }
        NativeRenderType::List(child) => {
            out.children = resources.vector(1)?;
            out.children.push(wire::NamedRenderField {
                name: resources.copy_string("item")?,
                field_node_id: encode_render_field(child, nodes, resources)?,
            });
            K::List
        }
        NativeRenderType::Map { key, value } => {
            out.children = resources.vector(2)?;
            for entry in [
                wire::NamedRenderField {
                    name: resources.copy_string("key")?,
                    field_node_id: encode_render_field(key, nodes, resources)?,
                },
                wire::NamedRenderField {
                    name: resources.copy_string("value")?,
                    field_node_id: encode_render_field(value, nodes, resources)?,
                },
            ] {
                out.children.push(entry);
            }
            K::Map
        }
        NativeRenderType::Struct(fields) => {
            out.children = resources.vector(fields.len())?;
            for child in fields {
                out.children.push(wire::NamedRenderField {
                    name: resources.copy_string(&child.name)?,
                    field_node_id: encode_render_field(&child.field, nodes, resources)?,
                });
            }
            K::Struct
        }
    };
    out.kind = kind as i32;
    Ok(out)
}
fn encode_time_unit(unit: RenderTimeUnit) -> i32 {
    (match unit {
        RenderTimeUnit::Second => wire::RenderTimeUnit::Second,
        RenderTimeUnit::Millisecond => wire::RenderTimeUnit::Millisecond,
        RenderTimeUnit::Microsecond => wire::RenderTimeUnit::Microsecond,
        RenderTimeUnit::Nanosecond => wire::RenderTimeUnit::Nanosecond,
    }) as i32
}
fn encode_scalar_field<H>(
    field: &ScalarField,
    nodes: &mut Vec<wire::ScalarField>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<u32, H> {
    resources.work.step()?;
    let id = (nodes.len() + 1) as u32;
    nodes.push(wire::ScalarField::default());
    let mut ty = wire::ScalarValueType::default();
    use wire::ScalarTypeKind as K;
    let kind = match &field.value_type {
        S::Null => K::ScalarNull,
        S::Boolean => K::ScalarBoolean,
        S::SignedInteger(bits) => {
            ty.bits = u32::from(*bits);
            K::ScalarSignedInteger
        }
        S::LargeInt => K::ScalarLargeInt,
        S::Float32 => K::ScalarFloat32,
        S::Float64 => K::ScalarFloat64,
        S::Decimal {
            bits,
            precision,
            scale,
        } => {
            ty.bits = u32::from(*bits);
            ty.precision = u32::from(*precision);
            ty.scale = u32::from(*scale);
            K::ScalarDecimal
        }
        S::String => K::ScalarString,
        S::Binary => K::ScalarBinary,
        S::Date => K::ScalarDate,
        S::TimeMicros => K::ScalarTimeMicros,
        S::Timestamp { unit, timezone } => {
            ty.timestamp_unit = match unit {
                U::Microsecond => wire::ScalarTimestampUnit::ScalarMicrosecond,
                U::Nanosecond => wire::ScalarTimestampUnit::ScalarNanosecond,
            } as i32;
            ty.time_zone = timezone
                .as_deref()
                .map(|zone| resources.copy_string(zone))
                .transpose()?;
            K::ScalarTimestamp
        }
        S::Json => K::ScalarJson,
        S::Variant => K::ScalarVariant,
        S::Opaque(kind) => {
            ty.opaque_kind = match kind {
                O::Hll => wire::OpaqueKind::Hll,
                O::Bitmap => wire::OpaqueKind::Bitmap,
                O::Object => wire::OpaqueKind::Object,
                O::Percentile => wire::OpaqueKind::Percentile,
            } as i32;
            K::ScalarOpaque
        }
        S::List(child) => {
            ty.children = resources.vector(1)?;
            ty.children.push(wire::NamedScalarField {
                name: resources.copy_string("item")?,
                field_node_id: encode_scalar_field(child, nodes, resources)?,
            });
            K::ScalarList
        }
        S::Map { key, value } => {
            ty.children = resources.vector(2)?;
            for entry in [
                wire::NamedScalarField {
                    name: resources.copy_string("key")?,
                    field_node_id: encode_scalar_field(key, nodes, resources)?,
                },
                wire::NamedScalarField {
                    name: resources.copy_string("value")?,
                    field_node_id: encode_scalar_field(value, nodes, resources)?,
                },
            ] {
                ty.children.push(entry);
            }
            K::ScalarMap
        }
        S::Struct(fields) => {
            ty.children = resources.vector(fields.len())?;
            for child in fields {
                ty.children.push(wire::NamedScalarField {
                    name: resources.copy_string(&child.name)?,
                    field_node_id: encode_scalar_field(&child.field, nodes, resources)?,
                });
            }
            K::ScalarStruct
        }
    };
    ty.kind = kind as i32;
    nodes[id as usize - 1] = wire::ScalarField {
        nullable: field.nullable,
        value_type: Some(ty),
    };
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_projection_v2::AdmissionRefusal;
    use novarocks_result_contract::{
        ClientRenderSchema, RenderColumn, RootProfileId, RootProfileV1,
    };
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueTypeStorageLifetime};
    use std::convert::Infallible;

    struct TestControl;
    impl novarocks_type_contract::PureCompileControl for TestControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    fn source() -> RootOutputContract {
        RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::ClientRows(
                ClientRenderSchema::try_new(
                    vec![RenderColumn {
                        source_ordinal: 1,
                        source_slot: None,
                        name: "nested".into(),
                        field: RenderField {
                            nullable: true,
                            presentation: RenderPresentation::MysqlContainer,
                            native_type: NativeRenderType::List(Box::new(RenderField {
                                nullable: false,
                                presentation: RenderPresentation::TimestampContainerText,
                                native_type: NativeRenderType::Timestamp {
                                    unit: RenderTimeUnit::Nanosecond,
                                    timezone: Some("Etc/UTC".into()),
                                },
                            })),
                        },
                    }],
                    2,
                )
                .unwrap(),
            ),
        )
    }
    #[test]
    fn borrowed_render_projection_matches_original_vocabulary_and_exact_slot_binding() {
        let source = source();
        let mut request = |_| Ok::<_, AdmissionRefusal<Infallible>>(());
        let mut resources = TypeEncodingResources {
            request: &mut request,
            work: CompileCheckpoints::try_new(&TestControl, CompilePhase::Encode).unwrap(),
            type_policy: crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::LegacyInline,
            storage: ValueTypeStorageLifetime::pure(),
        };
        let actual = encode(&source, &[13, 41], &mut resources).unwrap();
        let bound = source.clone().bind_native_slots(&[13, 41]).unwrap();
        assert_eq!(
            actual,
            novarocks_proto_codec::root_result::encode_root_contract(&bound)
        );
        let mut oversized = vec![0; RootProfileV1::MAX_COLUMNS + 1];
        oversized[1] = 41;
        assert!(source.bind_native_slots(&oversized).is_err());
        assert!(encode(&bound, &oversized, &mut resources).is_err());
    }
}
