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

//! Fixture preparation for two consumers with different view admission laws.
//! Raw pure-owner outputs are checked directly; neither output is normalized.
use super::*;
use arrow::array::{
    BinaryArray, BinaryViewArray, LargeListArray, LargeListViewArray, ListArray, ListViewArray,
    MapArray, StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::Field;
use novarocks_type_contract::{ValueLayout, ValueTypeStorageLifetime};

// This is a representation projection of the already-declared layout. The
// semantic tree, names and every NULL promise come from the original FVT.
fn plain_view_layout(ty: &DataType) -> DataType {
    let field = |field: &Arc<Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(plain_view_layout(field.data_type())),
        )
    };
    match ty {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::ListView(item) | DataType::List(item) => DataType::List(field(item)),
        DataType::LargeListView(item) | DataType::LargeList(item) => {
            DataType::LargeList(field(item))
        }
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(field(item), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
        DataType::Union(fields, mode) => DataType::Union(
            arrow::datatypes::UnionFields::try_new(
                fields.iter().map(|(id, _)| id),
                fields.iter().map(|(_, value)| field(value)),
            )
            .unwrap(),
            *mode,
        ),
        // Other encodings retain the unchanged existing fixture preparation.
        _ => ty.clone(),
    }
}
fn plain_type(source: &FunctionValueType) -> FunctionValueType {
    FunctionValueType::try_from_value(
        source.value_type().clone(),
        ValueLayout::try_new(
            &plain_view_layout(source.data_type()),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn prepare_array(source: &ArrayRef, declared: &FunctionValueType) -> ArrayRef {
    let cast = arrow::compute::cast(source.as_ref(), declared.data_type()).unwrap();
    let plain = crate::exec::chunk::normalize_execution_array(&cast).unwrap();
    assert!(novarocks_type_contract::arrow_data_types_exact(
        plain.data_type(),
        declared.data_type()
    ));
    plain
}
fn list_value(array: &ArrayRef, row: usize) -> ArrayRef {
    match array.data_type() {
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(row),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap()
            .value(row),
        DataType::ListView(_) => array
            .as_any()
            .downcast_ref::<ListViewArray>()
            .unwrap()
            .value(row),
        DataType::LargeListView(_) => array
            .as_any()
            .downcast_ref::<LargeListViewArray>()
            .unwrap()
            .value(row),
        _ => unreachable!("the declared list branches are matched by the caller"),
    }
}
// Borrow actual Arrow accessors for byte/list/map view comparisons. This does
// not create a replacement output array or sign a type from a result carrier.
fn assert_same_value(
    raw: &ArrayRef,
    r: usize,
    plain: &ArrayRef,
    p: usize,
    floats: FloatComparison,
) {
    assert_eq!(raw.is_null(r), plain.is_null(p));
    if raw.is_null(r) {
        return;
    }
    match (raw.data_type(), plain.data_type()) {
        (DataType::Utf8View, DataType::Utf8) => assert_eq!(
            raw.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(r)
                .as_bytes(),
            plain
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(p)
                .as_bytes()
        ),
        (DataType::BinaryView, DataType::Binary) => assert_eq!(
            raw.as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .value(r),
            plain
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(p)
        ),
        (
            DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_),
            DataType::List(_) | DataType::LargeList(_),
        ) => {
            let raw = list_value(raw, r);
            let plain = list_value(plain, p);
            assert_eq!(raw.len(), plain.len());
            for row in 0..raw.len() {
                assert_same_value(&raw, row, &plain, row, floats);
            }
        }
        (DataType::Map(_, ro), DataType::Map(_, po)) => {
            assert_eq!(ro, po);
            let raw: ArrayRef = Arc::new(raw.as_any().downcast_ref::<MapArray>().unwrap().value(r));
            let plain: ArrayRef =
                Arc::new(plain.as_any().downcast_ref::<MapArray>().unwrap().value(p));
            assert_eq!(raw.len(), plain.len());
            for row in 0..raw.len() {
                assert_same_value(&raw, row, &plain, row, floats);
            }
        }
        (DataType::Struct(rf), DataType::Struct(pf)) => {
            assert_eq!(rf.len(), pf.len());
            let raw = raw.as_any().downcast_ref::<StructArray>().unwrap();
            let plain = plain.as_any().downcast_ref::<StructArray>().unwrap();
            for index in 0..rf.len() {
                assert_eq!(rf[index].name(), pf[index].name());
                assert_eq!(rf[index].is_nullable(), pf[index].is_nullable());
                assert_same_value(raw.column(index), r, plain.column(index), p, floats);
            }
        }
        _ => {
            compare_value(raw, r, plain, p, floats).unwrap();
        }
    }
}
fn view_payloads(data: &arrow::array::ArrayData, output: &mut Vec<usize>) {
    if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
        output.extend(
            data.buffers()
                .iter()
                .skip(1)
                .map(|buffer| buffer.as_ptr() as usize),
        );
    }
    for child in data.child_data() {
        view_payloads(child, output);
    }
}
// The original one-argument BinaryView MD5 refinement is a VARCHAR cast,
// whereas Binary is a direct byte profile. Bind that exact selected signature
// before observing results; invalid UTF8 therefore does not imply equal hashes.
struct BinaryViewMd5Oracle {
    raw: novarocks_functions::builtin::md5_shared::OwnedBytesArray,
    plain: novarocks_functions::builtin::md5_shared::OwnedBytesArray,
    numeric: bool,
}
impl BinaryViewMd5Oracle {
    fn try_new(
        raw: &ScalarDiffSpec,
        plain: &ScalarDiffSpec,
        bound: &ResolvedFunctionBinding,
        result: &FunctionValueType,
    ) -> Option<Self> {
        let (overload, numeric) = match bound.function_id.as_str() {
            "builtin.scalar/md5sum/v1" => {
                ("builtin.scalar/md5sum/(any...)->utf8;strict;legacy", false)
            }
            "builtin.scalar/md5sum_numeric/v1" => {
                ("builtin.scalar/md5sum_numeric/dynamic-v1", true)
            }
            _ => return None,
        };
        let raw_type = FunctionValueType::new(DataType::BinaryView, true);
        if bound.selected.argument_types.as_ref() != [FunctionArgumentType::Value(raw_type.clone())]
        {
            return None;
        }
        assert_eq!(bound.selected.overload.as_str(), overload);
        assert_eq!(raw.arguments.len(), 1);
        assert_eq!(plain.arguments.len(), 1);
        let (
            DiffArgument::Column {
                value_type: rt,
                values: rv,
            },
            DiffArgument::Column {
                value_type: pt,
                values: pv,
            },
        ) = (&raw.arguments[0], &plain.arguments[0])
        else {
            panic!("this exact original BinaryView fixture declares a column");
        };
        assert_eq!(rt, &raw_type);
        assert_eq!(pt, &FunctionValueType::new(DataType::Binary, true));
        let expected_result = if numeric {
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            )
            .unwrap()
        } else {
            FunctionValueType::new(DataType::Utf8, true)
        };
        assert_eq!(result, &expected_result);
        // Use the ONE original cast/admission author once per source. This is
        // an independent input refinement oracle, not output normalization or
        // another MD5 implementation. The plain source keeps every original byte.
        let cast = |source: &ArrayRef| {
            novarocks_functions::builtin::md5_shared::to_owned_bytes_array_with_varchar_cast_observed(
                source.clone(), 0, &mut |_| Ok(()),
            ).unwrap()
        };
        Some(Self {
            raw: cast(rv),
            plain: cast(pv),
            numeric,
        })
    }
    fn digest(
        input: &novarocks_functions::builtin::md5_shared::OwnedBytesArray,
        row: usize,
    ) -> &'static str {
        let bytes = if input.is_null(row) {
            b"".as_slice()
        } else {
            input.bytes(row)
        };
        // Independently frozen hashes for the unchanged closed source corpus.
        // Unknown input fails this oracle rather than acquiring a new profile.
        match bytes {
            b"" => "d41d8cd98f00b204e9800998ecf8427e",
            b"\xff" => "00594fd4f42ba43fc1ca0427a0576295",
            b"\xc3\xa9\x00" => "a0b67120306f6965f7ce58b6f9aca39a",
            b"hello" => "5d41402abc4b2a76b9719d911017c592",
            _ => panic!("the frozen BinaryView MD5 input corpus has changed"),
        }
    }
    fn assert_digest(
        &self,
        input: &novarocks_functions::builtin::md5_shared::OwnedBytesArray,
        source_row: usize,
        output: &ArrayRef,
        output_row: usize,
    ) {
        // Called-on-NULL MD5SUM skips NULL inputs and still emits a non-NULL
        // digest of the empty byte stream, including the original invalid UTF8.
        assert!(!output.is_null(output_row));
        let expected = Self::digest(input, source_row);
        if self.numeric {
            let decoded = hex::decode(expected).unwrap();
            let bytes: [u8; 16] = decoded.try_into().unwrap();
            let value =
                novarocks_types::largeint::value_at(output.as_fixed_size_binary(), output_row)
                    .unwrap();
            assert_eq!(value, i128::from_be_bytes(bytes));
        } else {
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(output_row),
                expected
            );
        }
    }
    fn assert_row(
        &self,
        source_row: usize,
        raw: &ArrayRef,
        plain: &ArrayRef,
        output_row: usize,
        floats: FloatComparison,
    ) {
        self.assert_digest(&self.raw, source_row, raw, output_row);
        self.assert_digest(&self.plain, source_row, plain, output_row);
        // Keep raw/plain equality wherever the original input refinement is
        // identical. The distinct invalid-UTF8 row is pinned by BOTH hashes.
        if self.raw.is_null(source_row) == self.plain.is_null(source_row)
            && (self.raw.is_null(source_row)
                || self.raw.bytes(source_row) == self.plain.bytes(source_row))
        {
            assert_same_value(raw, output_row, plain, output_row, floats);
        } else {
            assert!(self.raw.is_null(source_row));
            assert!(!self.plain.is_null(source_row));
            assert_eq!(self.plain.bytes(source_row), b"\xff");
        }
    }
}

#[track_caller]
pub(super) fn assert_scalar_matches_v1_with_plain_views(raw: ScalarDiffSpec) -> ScalarDiffSummary {
    let has_views = raw
        .arguments
        .iter()
        .any(|argument| plain_type(argument.value_type()) != *argument.value_type());
    if !has_views {
        return assert_scalar_matches_v1(raw);
    }
    let mut plain = raw.clone();
    let mut sources = Vec::new();
    for (original, prepared) in raw.arguments.iter().zip(&mut plain.arguments) {
        let declared = plain_type(original.value_type());
        assert_eq!(declared.value_type(), original.value_type().value_type());
        if declared == *original.value_type() {
            continue;
        }
        let source = match original {
            DiffArgument::Column { values, .. } => values.clone(),
            DiffArgument::Constant(value) => {
                value.pool().array().slice(value.ordinal() as usize, 1)
            }
        };
        let before = source.to_data();
        // Independent current-engine rejection remains a real ingress fact,
        // including a constant-only call whose host Chunk would use an anchor.
        let error = crate::exec::chunk::validate_chunk_carrier_encoding_observed::<String>(
            source.data_type(),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(
            error,
            "view carriers are not supported by the current execution engine"
        );
        let values = prepare_array(&source, &declared);
        assert_eq!(source.len(), values.len());
        for row in 0..source.len() {
            assert_same_value(&source, row, &values, row, raw.float_comparison);
        }
        *prepared = match original {
            DiffArgument::Column { .. } => DiffArgument::Column {
                value_type: declared,
                values,
            },
            DiffArgument::Constant(_) => DiffArgument::Constant(constant(declared, values)),
        };
        sources.push((source, before));
    }
    if let Some(expected) = &raw.expected_result_type {
        plain.expected_result_type = Some(plain_type(expected));
    }
    let rows = raw.rows().unwrap();
    let catalog = builtin_engine_function_catalog();
    let prepare = |spec: &ScalarDiffSpec| {
        let bound =
            resolve_like_sql(catalog, &spec.name, CatalogKind::Scalar, &spec.arguments).unwrap();
        require_selected_arguments(&spec.name, &bound, &spec.arguments).unwrap();
        let (_, dependencies) = installed_declaration(
            catalog,
            &spec.name,
            &bound,
            CatalogKind::Scalar,
            LegacyStatus::Available,
        )
        .unwrap();
        let prepared = prepare_scalar(catalog, spec, &bound, &dependencies).unwrap();
        (bound, prepared)
    };
    let (raw_bound, raw_prepared) = prepare(&raw);
    let (plain_bound, plain_prepared) = prepare(&plain);
    assert_eq!(raw_bound.function_id, plain_bound.function_id);
    assert_eq!(raw_bound.selected.overload, plain_bound.selected.overload);
    let (FunctionResultType::Scalar(raw_result), FunctionResultType::Scalar(plain_result)) = (
        &raw_bound.selected.result_type,
        &plain_bound.selected.result_type,
    ) else {
        panic!("scalar fixture selected a relation");
    };
    assert_eq!(raw_result.value_type(), plain_result.value_type());
    let binary_view_md5 = BinaryViewMd5Oracle::try_new(&raw, &plain, &raw_bound, raw_result);
    let legacy_kind =
        scalar_legacy_implementation(&legacy_wire_name(&raw_bound.function_id).unwrap()).unwrap();
    let raw_call = ScalarCall {
        spec: &raw,
        legacy_kind,
        result_type: raw_result,
        rows,
    };
    let plain_call = ScalarCall {
        spec: &plain,
        legacy_kind,
        result_type: plain_result,
        rows,
    };
    let mut selections = vec![None];
    if rows > 1 {
        let mut generator = InputGenerator::new(raw.selection_seed);
        selections.extend((0..raw.sparse_selections).map(|_| Some(generator.selection(rows, 0.5))));
    }
    for selection in selections {
        let selected = selection.as_deref();
        let raw_output = raw_call.evaluate_pure(&raw_prepared, selected).unwrap();
        let plain_output = plain_call.evaluate_pure(&plain_prepared, selected).unwrap();
        let (PureScalarOutcome::Selected(raw_output), PureScalarOutcome::Selected(plain_output)) =
            (raw_output, plain_output)
        else {
            panic!("these original view profiles have no invocation-data failure");
        };
        assert!(raw_output.errors.is_empty());
        assert!(plain_output.errors.is_empty());
        let expected_rows = selected.map_or(rows, <[usize]>::len);
        assert_eq!(raw_output.values.len(), expected_rows);
        assert_eq!(plain_output.values.len(), expected_rows);
        assert!(novarocks_type_contract::arrow_data_types_exact(
            raw_output.values.data_type(),
            raw_result.data_type(),
        ));
        assert!(novarocks_type_contract::arrow_data_types_exact(
            plain_output.values.data_type(),
            plain_result.data_type(),
        ));
        for row in 0..raw_output.values.len() {
            if let Some(profile) = &binary_view_md5 {
                let source_row = selected.map_or(row, |selected| selected[row]);
                profile.assert_row(
                    source_row,
                    &raw_output.values,
                    &plain_output.values,
                    row,
                    raw.float_comparison,
                );
            } else {
                assert_same_value(
                    &raw_output.values,
                    row,
                    &plain_output.values,
                    row,
                    raw.float_comparison,
                );
            }
        }
        if raw.name == "array_append" && selected.is_none() && rows > 0 {
            let mut output_payloads = Vec::new();
            view_payloads(&raw_output.values.to_data(), &mut output_payloads);
            for (_, before) in &sources {
                let mut source_payloads = Vec::new();
                view_payloads(before, &mut source_payloads);
                for pointer in source_payloads {
                    assert!(
                        output_payloads.contains(&pointer),
                        "raw append retains its original view payload buffer mapping"
                    );
                }
            }
        }
    }
    for (source, before) in sources {
        assert_eq!(source.to_data(), before);
        let mut before_payloads = Vec::new();
        let mut after_payloads = Vec::new();
        view_payloads(&before, &mut before_payloads);
        view_payloads(&source.to_data(), &mut after_payloads);
        assert_eq!(before_payloads, after_payloads);
    }
    // Both actual consumers now receive the same declared plain source. The
    // unchanged differential still checks types, errors, values and selections.
    assert_scalar_matches_v1(plain)
}
