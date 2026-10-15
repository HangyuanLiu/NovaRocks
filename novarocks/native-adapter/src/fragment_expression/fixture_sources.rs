//! Complete source declarations for Native expression decoder fixtures.
//! Raw DTOs borrow these independent declarations; they do not author domains.

use arrow::{
    array::*,
    datatypes::{DataType, Field, TimeUnit},
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{call_request, common, expr, plan};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueKind,
    ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::sync::Arc;

pub(crate) trait Declaration {
    fn declared(&self) -> FunctionValueType;
}
impl Declaration for DataType {
    fn declared(&self) -> FunctionValueType {
        FunctionValueType::new(self.clone(), true)
    }
}
impl Declaration for FunctionValueType {
    fn declared(&self) -> FunctionValueType {
        self.clone()
    }
}

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

pub(crate) fn largeint_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
pub(crate) fn json_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json).unwrap()
}
pub(crate) fn binary_domain(domain: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Binary, true, domain).unwrap()
}
pub(crate) fn variant_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::LargeBinary, true, ValueLogicalType::Variant)
        .unwrap()
}
pub(crate) fn map_json_type() -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let key = ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let ty = ValueTypeRef::try_new(ValueKind::Map { key, value }, true, storage.clone()).unwrap();
    let carrier = map_type(DataType::Utf8, DataType::Utf8);
    FunctionValueType::try_from_value(ty, ValueLayout::try_new(&carrier, storage).unwrap()).unwrap()
}
fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", key, false),
                    Field::new("value", value, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}
fn declarations() -> Vec<FunctionValueType> {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int32,
        DataType::Int64,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::Date32,
        DataType::FixedSizeBinary(16),
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 0),
        DataType::Decimal256(40, 3),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::LargeBinary,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(10, 2),
            true,
        ))),
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(12, 3),
            true,
        ))),
        map_type(DataType::Int64, DataType::Utf8),
        map_type(DataType::Int64, DataType::Int64),
    ]
    .into_iter()
    .map(|ty| ty.declared())
    .collect::<Vec<_>>();
    types.extend([
        largeint_type(),
        variant_type(),
        json_type(),
        map_json_type(),
    ]);
    types.extend([
        binary_domain(ValueLogicalType::Hll),
        binary_domain(ValueLogicalType::Bitmap),
        binary_domain(ValueLogicalType::Percentile),
        binary_domain(ValueLogicalType::Object),
    ]);
    for source in super::fixture_aggregate_sources::source_catalogue() {
        for value in source.roots() {
            if !types.contains(&value) {
                types.push(value);
            }
        }
    }
    let nonnull = types
        .iter()
        .filter(|ty| ty.data_type() != &DataType::Null)
        .map(|ty| ty.with_nullable(false).unwrap())
        .collect::<Vec<_>>();
    types.extend(nonnull);
    types
}
pub(crate) fn type_desc(declared: &impl Declaration) -> common::TypeDesc {
    type_desc_with_nullable(declared, true)
}
pub(crate) fn type_desc_with_nullable(
    declared: &impl Declaration,
    nullable: bool,
) -> common::TypeDesc {
    let ty = declared.declared().with_nullable(nullable).unwrap();
    let index = declarations()
        .iter()
        .position(|source| source == &ty)
        .expect("the fixture supplies an independent complete declaration");
    common::TypeDesc {
        value_binding_id: Some(u32::try_from(index).unwrap()),
    }
}

struct LiteralSource {
    ty: FunctionValueType,
    value: common::literal_value::Value,
    array: ArrayRef,
}
fn literal_sources() -> Vec<LiteralSource> {
    use common::literal_value::Value;
    let mut values = Vec::new();
    for value in [0i64, 1, 2, 3, 4, 5, 7, 8, 9, 10, 20, 30, 42, 123] {
        values.push(LiteralSource {
            ty: DataType::Int64.declared(),
            value: Value::IntValue(value),
            array: Arc::new(Int64Array::from(vec![value])),
        });
    }
    for value in [
        "a", "b", "x", "x%", "in", "like", "miss", "abc", "|", ",", "a,b", "unused", "alice",
        "bob", "v", "first",
    ] {
        values.push(LiteralSource {
            ty: DataType::Utf8.declared(),
            value: Value::StringValue(value.to_owned()),
            array: Arc::new(StringArray::from(vec![value])),
        });
    }
    for ty in [DataType::Null, DataType::Int32, DataType::Int64] {
        values.push(LiteralSource {
            ty: ty.declared(),
            value: Value::NullValue(true),
            array: new_null_array(&ty, 1),
        });
    }
    values.extend([
        LiteralSource {
            ty: json_type(),
            value: Value::StringValue("{\"v\":1}".to_owned()),
            array: Arc::new(StringArray::from(vec!["{\"v\":1}"])),
        },
        LiteralSource {
            ty: DataType::Boolean.declared(),
            value: Value::BoolValue(false),
            array: Arc::new(BooleanArray::from(vec![false])),
        },
        LiteralSource {
            ty: DataType::Boolean.declared(),
            value: Value::BoolValue(true),
            array: Arc::new(BooleanArray::from(vec![true])),
        },
        LiteralSource {
            ty: DataType::Float64.declared(),
            value: Value::FloatValue(1.25),
            array: Arc::new(Float64Array::from(vec![1.25])),
        },
        LiteralSource {
            ty: DataType::Binary.declared(),
            value: Value::BinaryValue(vec![1, 2, 3]),
            array: Arc::new(BinaryArray::from(vec![&[1u8, 2, 3][..]])),
        },
        LiteralSource {
            ty: DataType::Date32.declared(),
            value: Value::Date32Value(20_000),
            array: Arc::new(Date32Array::from(vec![20_000])),
        },
        LiteralSource {
            ty: largeint_type(),
            value: Value::LargeintValue((-12_345i128).to_be_bytes().to_vec()),
            array: Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![(-12_345i128).to_be_bytes()].into_iter())
                    .unwrap(),
            ),
        },
        LiteralSource {
            ty: DataType::Decimal128(10, 2).declared(),
            value: Value::DecimalValue(common::DecimalLiteral {
                value: 12345i128.to_be_bytes().to_vec(),
                precision: 10,
                scale: 2,
            }),
            array: Arc::new(
                Decimal128Array::from(vec![12345])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        },
        LiteralSource {
            ty: DataType::Decimal256(40, 3).declared(),
            value: Value::DecimalValue(common::DecimalLiteral {
                value: arrow_buffer::i256::from_i128(123_456)
                    .to_be_bytes()
                    .to_vec(),
                precision: 40,
                scale: 3,
            }),
            array: Arc::new(
                Decimal256Array::from(vec![arrow_buffer::i256::from_i128(123_456)])
                    .with_precision_and_scale(40, 3)
                    .unwrap(),
            ),
        },
    ]);
    for ty in [
        DataType::Binary.declared(),
        binary_domain(ValueLogicalType::Hll),
        binary_domain(ValueLogicalType::Bitmap),
        binary_domain(ValueLogicalType::Percentile),
        binary_domain(ValueLogicalType::Object),
    ] {
        values.push(LiteralSource {
            ty,
            value: Value::BinaryValue(b"state".to_vec()),
            array: Arc::new(BinaryArray::from(vec![b"state".as_slice()])),
        });
    }
    for source in &mut values {
        if !matches!(&source.value, Value::NullValue(true)) {
            source.ty = source.ty.with_nullable(false).unwrap();
        }
    }
    values
}
pub(crate) fn constant_reference(
    declared: &impl Declaration,
    literal: &expr::LiteralExpr,
) -> call_request::ConstantReference {
    let ty = declared.declared();
    let index = literal_sources()
        .iter()
        .position(|source| {
            source.ty.with_nullable(ty.nullable()).unwrap() == ty
                && Some(&source.value)
                    == literal
                        .value
                        .as_ref()
                        .and_then(|value| value.value.as_ref())
        })
        .expect(
            "the literal refers to its exact independent source value and complete declaration",
        );
    call_request::ConstantReference {
        pool_id: Some(100 + u32::try_from(index).unwrap()),
        row_ordinal: 0,
    }
}

pub(crate) fn with_decoder<T, E: From<crate::fragment_error::NativeFragmentDecodeError>>(
    operation: impl FnOnce(
        &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
    ) -> Result<T, E>,
) -> Result<T, E> {
    with_original_requests(&[], operation)
}

pub(crate) fn with_original_requests<
    T,
    E: From<crate::fragment_error::NativeFragmentDecodeError>,
>(
    original_requests: &[call_request::OriginalCallRequest],
    operation: impl FnOnce(
        &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
    ) -> Result<T, E>,
) -> Result<T, E> {
    with_original_owner(original_requests, |decoder, _| operation(decoder))
}
pub(crate) fn with_original_owner<T, E: From<crate::fragment_error::NativeFragmentDecodeError>>(
    original_requests: &[call_request::OriginalCallRequest],
    operation: impl FnOnce(
        &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
        &Arc<crate::fragment_original_requests::NativeOriginalRequests>,
    ) -> Result<T, E>,
) -> Result<T, E> {
    with_fragment_owners(original_requests, |decoder, original, _| {
        operation(decoder, original)
    })
}
pub(crate) fn with_fragment_owners<T, E: From<crate::fragment_error::NativeFragmentDecodeError>>(
    original_requests: &[call_request::OriginalCallRequest],
    operation: impl FnOnce(
        &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
        &Arc<crate::fragment_original_requests::NativeOriginalRequests>,
        &Arc<novarocks_plan_codec::physical_semantics_v2::CreatedWriterSemanticParameters>,
    ) -> Result<T, E>,
) -> Result<T, E> {
    use novarocks_plan_codec::{
        execution_type_table::{
            ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
        },
        physical_constant_v2::prepare_constant_record_write_pure_in,
        physical_type_v2::{TypeProjectionLimits, encode_source_field_table_pure_in},
    };
    let declared = declarations();
    let roots = declared
        .iter()
        .enumerate()
        .map(|(id, value)| ExecutionTypeRoot {
            id: u32::try_from(id).unwrap(),
            value,
        })
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let source_types = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap(),
            &mut |_| {
                Control
                    .checkpoint(CompilePhase::Encode, 0)
                    .map_err(Into::into)
            },
            &mut work,
        )
        .unwrap()
        .into_wire();
    let literal_sources = literal_sources();
    let mut pools = Vec::new();
    for (index, source) in literal_sources.iter().enumerate() {
        let pool = original_literal_pool(index, source);
        pools.push(pool);
    }
    let field_roots = pools
        .iter()
        .enumerate()
        .map(|(id, pool)| (u32::try_from(id).unwrap(), pool.field_ref()))
        .collect::<Vec<_>>();
    let source_fields = encode_source_field_table_pure_in(
        &field_roots,
        TypeProjectionLimits::for_validated_source_root_count(field_roots.len()).unwrap(),
        &mut work,
    )
    .unwrap();
    let mut records = Vec::new();
    for (index, pool) in pools.iter().enumerate() {
        let value_id = declared
            .iter()
            .position(|value| value == pool.value_type())
            .unwrap();
        records.push(
            prepare_constant_record_write_pure_in(
                novarocks_physical_plan::ConstantPoolId::new(100 + u32::try_from(index).unwrap()),
                u32::try_from(value_id).unwrap(),
                u32::try_from(index).unwrap(),
                pool,
                &mut work,
            )
            .unwrap()
            .emit_in(&mut work)
            .unwrap(),
        );
    }
    work.finish().unwrap();
    let source = plan::PlanFragment {
        execution_types: Some(source_types),
        constant_source_fields: Some(source_fields),
        constants: records,
        original_call_requests: Some(call_request::FragmentCallRequests {
            entries: original_requests.to_vec(),
        }),
        ..Default::default()
    };
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("expression fixture"),
        &Control,
    )
    .map_err(crate::fragment_error::NativeFragmentDecodeError::from)?;
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let mut resources = crate::fragment_type_decode::NativeDecodeResources::try_new(&creation)?;
    resources.bind_function_catalog(&catalog);
    let types = crate::fragment_type_decode::decode_fragment_type_owner(&source, &mut resources)?;
    let original = crate::fragment_original_sources::decode_original_requests(
        &source,
        &types,
        &mut resources,
    )?;
    let mut exact = |layout| creation.request_layout(layout);
    let mut opaque = |bytes| creation.request_opaque_upper_bound(bytes);
    let mut port = novarocks_type_contract::ValueSchemaCreation::new(
        creation.storage_lifetime(),
        &mut exact,
        &mut opaque,
    );
    let parameters = novarocks_plan_codec::physical_semantics_v2::decode_writer_semantic_parameters_with_creator_in(
        &novarocks_proto_models::physical_semantics_v2::SemanticParameters { entries: Vec::new() }, &mut port, &mut resources.work,
    ).map_err(|error| match error {
        novarocks_plan_codec::host_projection_v2::ProjectionFailure::Host(cause) => crate::fragment_error::NativeFragmentDecodeError::from(cause),
        novarocks_plan_codec::host_projection_v2::ProjectionFailure::Codec(cause) => crate::fragment_error::NativeFragmentDecodeError::invalid_value(FieldPath::root("fixture").field("writer_parameters"), cause),
    })?;
    let mut decoder = crate::fragment_type_decode::NativeTypeDecoder::Strong {
        types: &types.execution,
        original_requests: Some(original.as_ref()),
        resources: &mut resources,
    };
    let result = operation(&mut decoder, &original, &parameters)?;
    drop(decoder);
    resources.finish()?;
    Ok(result)
}

pub(crate) fn constant_is_null(value: &novarocks_functions::ConstantValue) -> bool {
    value
        .is_null_observed(CompilePhase::Validate, &Control)
        .unwrap()
}

pub(crate) fn decode(
    wire: &expr::Expr,
    path: FieldPath,
    arena: &mut novarocks_execution::exec::expr::ExprArena,
    layout: &super::NativeExpressionInputLayout,
) -> Result<novarocks_execution::exec::expr::ExprId, super::NativeExpressionDecodeError> {
    with_decoder(|decoder| super::decode_expr_at_with_types(wire, path, arena, layout, decoder))
}

pub(crate) use crate::connector_write_test_support::relation_fields;

pub(crate) fn declaration_for_reference(reference: &common::TypeDesc) -> FunctionValueType {
    declarations()[reference
        .value_binding_id
        .expect("complete source reference") as usize]
        .clone()
}

/// An actual empty source namespace for fixtures whose source has no constants.
/// This is an explicit original author, not healing malformed raw fragments.
pub(crate) fn empty_source_fields() -> novarocks_proto_models::physical_type_v2::SourceFieldTable {
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let output=novarocks_plan_codec::physical_type_v2::encode_source_field_table_pure_in(
        &[],novarocks_plan_codec::physical_type_v2::TypeProjectionLimits::for_validated_source_root_count(0).unwrap(),&mut work,
    ).unwrap();
    work.finish().unwrap();
    output
}

fn original_literal_pool(
    index: usize,
    source: &LiteralSource,
) -> novarocks_functions::ConstantPool {
    let name = format!("fixture literal {index}");
    let field = Arc::new(Field::new(
        &name,
        source.ty.data_type().clone(),
        source.ty.nullable(),
    ));
    let metadata_bytes = u64::try_from(name.len()).unwrap();
    let pool = novarocks_functions::ConstantPool::try_new(
        field,
        source.ty.clone(),
        source.array.to_data(),
        novarocks_functions::ConstantPolicy {
            max_rows: 1,
            max_array_nodes: 1,
            max_logical_elements: 1,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 1,
            max_metadata_bytes: metadata_bytes,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        },
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    pool
}
pub(crate) fn original_literal_value(
    declared: &FunctionValueType,
    literal: &expr::LiteralExpr,
) -> (
    novarocks_functions::ConstantValue,
    call_request::ConstantReference,
) {
    let reference = constant_reference(declared, literal);
    let index = usize::try_from(reference.pool_id.unwrap() - 100).unwrap();
    let original = literal_sources();
    let source = &original[index];
    assert_eq!(
        declared, &source.ty,
        "explicit original constant NULL/domain source"
    );
    (
        original_literal_pool(index, source).value(0).unwrap(),
        reference,
    )
}
