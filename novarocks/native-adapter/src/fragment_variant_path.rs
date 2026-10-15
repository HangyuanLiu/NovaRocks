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

//! Native fragment VARIANT-path scan wire decoding.

use std::collections::{HashMap, HashSet};

use crate::fragment_type_decode::NativeTypeDecoder;
use crate::native_type_construction::NativeTypeConstructionError;
use arrow::datatypes::{DataType, Field};
use novarocks_execution::exec::variant_read::{VariantPathSpec, VariantPathSpecParts};
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::{common, plan};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType, ValueTypeStorageLifetime};
use novarocks_types::SlotId;
use std::alloc::Layout;

use crate::fragment_error::NativeFragmentLeafDecodeError;

#[derive(Clone, Debug, Default)]
pub struct NativeVariantPathPlan {
    pub specs: Vec<VariantPathSpec>,
    pub output_slot_ids: HashSet<SlotId>,
    // Only responsibility for this actually constructed projection, never its
    // source namespace, query controller or provider authority.
    _storage: ValueTypeStorageLifetime,
}

pub(crate) fn parse_native_scan_variant_path_columns(
    scan: &plan::ScanNode,
    table: &plan::TableDef,
    output_columns: &[common::OutputColumn],
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<NativeVariantPathPlan, NativeFragmentLeafDecodeError> {
    if scan.variant_columns.is_empty() {
        return Ok(NativeVariantPathPlan::default());
    }
    // Scopes borrow the original control and share the established tracker.
    // Scratch is never retained by any published spec or transform.
    observe_work(type_decoder, 1)?;
    let creation = match &*type_decoder {
        NativeTypeDecoder::Strong { resources, .. } => Some(resources.creation),
    };
    let scratch_scope = creation
        .map(|creator| creator.begin_scope())
        .transpose()
        .map_err(|error| {
            NativeFragmentLeafDecodeError::from(
                crate::fragment_error::NativeFragmentDecodeError::from(error),
            )
        })?;
    let container_scope = creation
        .map(|creator| creator.begin_scope())
        .transpose()
        .map_err(|error| {
            NativeFragmentLeafDecodeError::from(
                crate::fragment_error::NativeFragmentDecodeError::from(error),
            )
        })?;
    request_fresh_table::<SlotId, &common::OutputColumn>(
        output_columns.len(),
        type_decoder,
        scratch_scope.as_ref(),
    )?;
    let mut output_by_slot = HashMap::with_capacity(output_columns.len());
    for column in output_columns {
        observe_work(type_decoder, 1)?;
        output_by_slot.insert(SlotId::new(column.column_id), column);
    }
    request_fresh_table::<SlotId, &common::OutputColumn>(
        scan.columns.len(),
        type_decoder,
        scratch_scope.as_ref(),
    )?;
    let mut scan_by_slot = HashMap::with_capacity(scan.columns.len());
    for column in &scan.columns {
        observe_work(type_decoder, 1)?;
        scan_by_slot.insert(SlotId::new(column.column_id), column);
    }
    request_fresh_table::<SlotId, ()>(
        scan.variant_columns.len(),
        type_decoder,
        container_scope.as_ref(),
    )?;
    request_layout(
        type_decoder,
        Layout::array::<VariantPathSpec>(scan.variant_columns.len())
            .map_err(|_| resource_arithmetic())?,
        container_scope.as_ref(),
    )?;
    let storage = container_scope
        .as_ref()
        .map(|scope| scope.storage_lifetime())
        .unwrap_or_else(ValueTypeStorageLifetime::pure);
    let mut plan = NativeVariantPathPlan {
        specs: Vec::with_capacity(scan.variant_columns.len()),
        output_slot_ids: HashSet::with_capacity(scan.variant_columns.len()),
        _storage: storage,
    };

    for (idx, column) in scan.variant_columns.iter().enumerate() {
        observe_work(type_decoder, 1)?;
        let spec_scope = creation
            .map(|creator| creator.begin_scope())
            .transpose()
            .map_err(|error| {
                NativeFragmentLeafDecodeError::from(
                    crate::fragment_error::NativeFragmentDecodeError::from(error),
                )
            })?;
        let source_slot_id = SlotId::new(column.source_column_id);
        let output_slot_id = SlotId::new(column.synthetic_column_id);
        if source_slot_id == output_slot_id {
            return Err(variant_error(
                idx,
                "source_column_id",
                ProtocolErrorKind::InconsistentFields,
                "source_column_id must differ from synthetic_column_id",
            ));
        }

        let source_name = required_native_variant_path_string(
            idx,
            "source_column",
            &column.source_column,
            type_decoder,
        )?;
        let output_name = required_native_variant_path_string(
            idx,
            "synthetic_column",
            &column.synthetic_column,
            type_decoder,
        )?;
        let canonical_path = required_native_variant_path_string(
            idx,
            "canonical_path",
            &column.canonical_path,
            type_decoder,
        )?;
        observe_work(type_decoder, canonical_path.len())?;
        validate_native_variant_path_column_path(idx, &canonical_path, type_decoder)?;
        observe_work(type_decoder, 1)?;

        let source_scan_column = scan_by_slot.get(&source_slot_id).ok_or_else(|| {
            variant_error(
                idx,
                "source_column_id",
                ProtocolErrorKind::InvalidValue,
                format!("source_column_id={source_slot_id} is not a scan column"),
            )
        })?;
        if source_scan_column.name != source_name {
            return Err(variant_error(
                idx,
                "source_column",
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "source_column={source_name:?} does not match source_column_id={source_slot_id} name {:?}",
                    source_scan_column.name
                ),
            ));
        }
        let source_table_column = table
            .columns
            .iter()
            .find(|col| col.name == source_name)
            .ok_or_else(|| {
                variant_error(
                    idx,
                    "source_column",
                    ProtocolErrorKind::InvalidValue,
                    format!("source_column={source_name:?} is not in table column definitions"),
                )
            })?;
        let (source_binding, source_type) = column_def_data_type(source_table_column, type_decoder)
            .map_err(|err| {
                err.prepend_field("source_column")
                    .prepend_index(idx)
                    .prepend_field("variant_columns")
            })?;
        if !matches!(source_type, DataType::LargeBinary)
            || source_binding
                .as_ref()
                .is_some_and(|value| value.logical_type() != ValueLogicalType::Variant)
        {
            return Err(variant_error(
                idx,
                "source_column",
                ProtocolErrorKind::InvalidValue,
                format!(
                    "source_column={source_name:?} expects VARIANT/LargeBinary, got {:?}",
                    source_type
                ),
            ));
        }

        if let Some(source) = &source_binding {
            let (scan_source, _) = output_column_data_type(source_scan_column, type_decoder)?;
            let scan_source = scan_source.ok_or_else(|| {
                variant_error(
                    idx,
                    "source_column",
                    ProtocolErrorKind::MissingField,
                    "complete scan source binding missing",
                )
            })?;
            if !type_decoder
                .bindings_match(source.typed_binding(), scan_source.typed_binding(), false)
                .map_err(NativeFragmentLeafDecodeError::from)?
            {
                return Err(variant_error(
                    idx,
                    "source_column",
                    ProtocolErrorKind::InconsistentFields,
                    "scan source differs from its complete table declaration",
                ));
            }
        }

        let output_column = output_by_slot.get(&output_slot_id).ok_or_else(|| {
            variant_error(
                idx,
                "synthetic_column_id",
                ProtocolErrorKind::InvalidValue,
                format!("synthetic_column_id={output_slot_id} is not an output column"),
            )
        })?;
        if output_column.name != output_name {
            return Err(variant_error(
                idx,
                "synthetic_column",
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "synthetic_column={output_name:?} does not match synthetic_column_id={output_slot_id} name {:?}",
                    output_column.name
                ),
            ));
        }
        let (output_binding, output_type) = output_column_data_type(output_column, type_decoder)
            .map_err(|err| {
                err.prepend_field("synthetic_column")
                    .prepend_index(idx)
                    .prepend_field("variant_columns")
            })?;
        let requested_type_desc = column.requested_type.as_ref().ok_or_else(|| {
            variant_error(
                idx,
                "requested_type",
                ProtocolErrorKind::MissingField,
                "requested_type missing",
            )
        })?;
        let (requested_binding, requested_type) =
            resolve_value_type(requested_type_desc, type_decoder)?;
        if !is_supported_native_variant_path_requested_type(&requested_type)
            || requested_binding
                .as_ref()
                .is_some_and(|value| value.logical_type() != ValueLogicalType::Physical)
        {
            return Err(variant_error(
                idx,
                "requested_type",
                ProtocolErrorKind::Unsupported,
                format!(
                    "unsupported requested_type {:?} for synthetic_column_id={output_slot_id}",
                    requested_type
                ),
            ));
        }
        let same_binding = match (&requested_binding, &output_binding) {
            (Some(requested), Some(output)) => type_decoder
                .bindings_match(requested.typed_binding(), output.typed_binding(), false)
                .map_err(NativeFragmentLeafDecodeError::from)?,
            (None, None) => requested_type == output_type,
            _ => false,
        };
        if !same_binding {
            return Err(variant_error(
                idx,
                "requested_type",
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "requested_type {:?} does not match synthetic_column_id={output_slot_id} type {:?}",
                    requested_type, output_type
                ),
            ));
        }
        if !plan.output_slot_ids.insert(output_slot_id) {
            return Err(variant_error(
                idx,
                "synthetic_column_id",
                ProtocolErrorKind::InconsistentFields,
                format!("duplicate synthetic_column_id={output_slot_id}"),
            ));
        }

        let source_field_name = copy_text(source_name, type_decoder, spec_scope.as_ref())?;
        let output_field_name = copy_text(output_name, type_decoder, spec_scope.as_ref())?;
        let canonical_path = copy_text(canonical_path, type_decoder, spec_scope.as_ref())?;
        let parts = VariantPathSpecParts {
            source_slot_id,
            source_read_slot_id: source_slot_id,
            output_slot_id,
            source_field_id: None,
            source_field: Field::new(source_field_name, source_type, source_table_column.nullable),
            output_field: Field::new(output_field_name, output_type, output_column.nullable),
            canonical_path,
            strict: column.strict,
            source_binding,
            output_binding,
        };
        let spec = match spec_scope.as_ref() {
            Some(scope) => {
                observe_work(type_decoder, 1)?;
                VariantPathSpec::try_from_parts_observed(
                    parts,
                    scope.storage_lifetime(),
                    &mut |layout| scope.request_layout(layout),
                )
                .map_err(|error| {
                    NativeFragmentLeafDecodeError::from(
                        crate::fragment_error::NativeFragmentDecodeError::from(error),
                    )
                })?
            }
            None => VariantPathSpec::from_pure_parts(parts),
        };
        plan.specs.push(spec);
    }

    Ok(plan)
}

fn required_native_variant_path_string<'a>(
    idx: usize,
    field_name: &'static str,
    value: &'a str,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<&'a str, NativeFragmentLeafDecodeError> {
    observe_work(type_decoder, value.len())?;
    if value.trim().is_empty() {
        return Err(variant_error(
            idx,
            field_name,
            ProtocolErrorKind::MissingField,
            format!("{field_name} missing"),
        ));
    }
    Ok(value.trim())
}

enum NativePathFailure {
    Source(String),
    Decode(NativeFragmentLeafDecodeError),
}
impl From<String> for NativePathFailure {
    fn from(error: String) -> Self {
        Self::Source(error)
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for NativePathFailure
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Decode(crate::fragment_error::NativeFragmentDecodeError::from(error).into())
    }
}
fn validate_native_variant_path_column_path(
    idx: usize,
    canonical_path: &str,
    decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<(), NativeFragmentLeafDecodeError> {
    use novarocks_types::value::variant::{
        VariantPathConstructionStep, parse_variant_path_with_storage_observed,
    };
    let creation = match &*decoder {
        NativeTypeDecoder::Strong { resources, .. } => Some(resources.creation),
    };
    let scope = creation
        .map(|creator| creator.begin_scope())
        .transpose()
        .map_err(|error| {
            NativeFragmentLeafDecodeError::from(
                crate::fragment_error::NativeFragmentDecodeError::from(error),
            )
        })?;
    let plain;
    let observed;
    let segments = if let Some(scope) = &scope {
        observed = parse_variant_path_with_storage_observed::<NativePathFailure>(
            canonical_path,
            scope.storage_lifetime(),
            |step| {
                match step {
                    VariantPathConstructionStep::Request(layout) => {
                        observe_work(decoder, 0).map_err(NativePathFailure::Decode)?;
                        scope.request_layout(layout).map_err(|error| {
                            NativePathFailure::Decode(
                                crate::fragment_error::NativeFragmentDecodeError::from(error)
                                    .into(),
                            )
                        })?;
                    }
                    VariantPathConstructionStep::Work(units) => {
                        observe_work(decoder, units).map_err(NativePathFailure::Decode)?
                    }
                }
                Ok(())
            },
        )
        .map_err(|error| match error {
            NativePathFailure::Decode(error) => error,
            NativePathFailure::Source(error) => variant_error(
                idx,
                "canonical_path",
                ProtocolErrorKind::InvalidValue,
                format!("invalid canonical_path={canonical_path:?}: {error}"),
            ),
        })?;
        observed.segments()
    } else {
        plain = novarocks_types::value::variant::parse_variant_path(canonical_path).map_err(
            |error| {
                variant_error(
                    idx,
                    "canonical_path",
                    ProtocolErrorKind::InvalidValue,
                    format!("invalid canonical_path={canonical_path:?}: {error}"),
                )
            },
        )?;
        &plain.segments
    };
    if segments.is_empty() {
        return Err(variant_error(
            idx,
            "canonical_path",
            ProtocolErrorKind::InvalidValue,
            format!("canonical_path={canonical_path:?} must reference at least one object key"),
        ));
    }
    if segments.iter().any(|segment| {
        !matches!(
            segment,
            novarocks_types::value::variant::VariantPathSegment::ObjectKey(_)
        )
    }) {
        return Err(variant_error(
            idx,
            "canonical_path",
            ProtocolErrorKind::Unsupported,
            format!("canonical_path={canonical_path:?} only supports object-key path segments"),
        ));
    }
    Ok(())
}

fn column_def_data_type(
    column: &plan::ColumnDef,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<(Option<FunctionValueType>, DataType), NativeFragmentLeafDecodeError> {
    let desc = column.data_type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "data_type",
            format!("column {} type missing", column.name),
        )
    })?;
    let (binding, layout) = resolve_value_type(desc, type_decoder)?;
    if binding
        .as_ref()
        .is_some_and(|value| value.nullable() != column.nullable)
    {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "nullable",
            "table column nullability differs from its complete binding",
        ));
    }
    Ok((binding, layout))
}
fn output_column_data_type(
    column: &common::OutputColumn,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<(Option<FunctionValueType>, DataType), NativeFragmentLeafDecodeError> {
    let desc = column.r#type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "type",
            format!("output column {} type missing", column.name),
        )
    })?;
    let (binding, layout) = resolve_value_type(desc, type_decoder)?;
    if binding
        .as_ref()
        .is_some_and(|value| value.nullable() != column.nullable)
    {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "nullable",
            "output nullability differs from its complete binding",
        ));
    }
    Ok((binding, layout))
}
fn resolve_value_type(
    desc: &common::TypeDesc,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<(Option<FunctionValueType>, DataType), NativeFragmentLeafDecodeError> {
    let binding = type_decoder
        .value_type(desc, FieldPath::root("variant_columns").field("type"))
        .map_err(NativeFragmentLeafDecodeError::from)?;
    let layout = match &binding {
        Some(value) => {
            observe_work(type_decoder, 1)?;
            value.data_type().clone()
        }
        None => type_decoder
            .data_type(desc, FieldPath::root("variant_columns").field("type"))
            .map_err(NativeFragmentLeafDecodeError::from)?,
    };
    Ok((binding, layout))
}
fn observe_work(
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
    units: usize,
) -> Result<(), NativeFragmentLeafDecodeError> {
    {
        let NativeTypeDecoder::Strong { resources, .. } = type_decoder;
        for _ in 0..units {
            resources.work.step().map_err(|error| {
                NativeFragmentLeafDecodeError::from(
                    crate::fragment_error::NativeFragmentDecodeError::from(
                        NativeTypeConstructionError::Control(error),
                    ),
                )
            })?;
        }
        resources.work.flush().map_err(|error| {
            NativeFragmentLeafDecodeError::from(
                crate::fragment_error::NativeFragmentDecodeError::from(
                    NativeTypeConstructionError::Control(error),
                ),
            )
        })?;
    }
    Ok(())
}
fn request_layout(
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
    layout: Layout,
    scope: Option<&crate::native_type_construction::NativeTypeScope<'_>>,
) -> Result<(), NativeFragmentLeafDecodeError> {
    {
        let NativeTypeDecoder::Strong { resources, .. } = type_decoder;
        resources.work.flush().map_err(|cause| {
            NativeFragmentLeafDecodeError::from(
                crate::fragment_error::NativeFragmentDecodeError::from(
                    NativeTypeConstructionError::Control(cause),
                ),
            )
        })?;
        let scope = scope.expect("strong native spec construction has its explicit entity scope");
        scope.request_layout(layout).map_err(|cause| {
            NativeFragmentLeafDecodeError::from(
                crate::fragment_error::NativeFragmentDecodeError::from(cause),
            )
        })?;
    }
    Ok(())
}
fn resource_arithmetic() -> NativeFragmentLeafDecodeError {
    crate::fragment_error::NativeFragmentDecodeError::from(NativeTypeConstructionError::Resource(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    ))
    .into()
}
fn request_fresh_table<K, V>(
    count: usize,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
    scope: Option<&crate::native_type_construction::NativeTypeScope<'_>>,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if matches!(type_decoder, NativeTypeDecoder::Strong { .. }) {
        let facts=novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<K,V>(count).map_err(|error| {
            let error=match error {novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_)=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,_=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel};
            NativeFragmentLeafDecodeError::from(crate::fragment_error::NativeFragmentDecodeError::from(NativeTypeConstructionError::Resource(error)))
        })?;
        if let Some(layout) = facts.layout {
            request_layout(type_decoder, layout, scope)?;
        }
    }
    Ok(())
}
fn copy_text(
    value: &str,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
    scope: Option<&crate::native_type_construction::NativeTypeScope<'_>>,
) -> Result<String, NativeFragmentLeafDecodeError> {
    observe_work(type_decoder, value.len())?;
    if !value.is_empty() {
        request_layout(
            type_decoder,
            Layout::array::<u8>(value.len()).map_err(|_| resource_arithmetic())?,
            scope,
        )?;
    }
    Ok(value.to_owned())
}

fn variant_error(
    idx: usize,
    field: &'static str,
    kind: ProtocolErrorKind,
    detail: impl std::fmt::Display,
) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_field(kind, "variant_columns", detail)
        .append_index(idx)
        .append_field(field)
}

fn is_supported_native_variant_path_requested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Date32
    )
}

#[cfg(test)]
mod strong_tests {
    use super::*;
    use crate::fragment_type_decode::{NativeDecodeResources, table_failure};
    use crate::native_type_construction::{NativeTypeConstruction, NativeTypeFactory};
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
        decode_execution_type_table_with_factory_in,
    };
    use novarocks_type_contract::{
        CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
    };
    use std::sync::Mutex;

    struct SourceControl;
    impl PureCompileControl for SourceControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            if let Some((at, _)) = self.refusal {
                assert!(trace.len() <= at, "no observation after original refusal");
            }
            let at = trace.len();
            trace.push((phase, units));
            match self.refusal {
                Some((stop, cause)) if at == stop => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn reference(id: u32) -> common::TypeDesc {
        common::TypeDesc {
            value_binding_id: Some(id),
        }
    }
    fn fixture(
        source_plain: bool,
        output_json: bool,
    ) -> (
        plan::ScanNode,
        Vec<common::OutputColumn>,
        Vec<FunctionValueType>,
    ) {
        let values = vec![
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::LargeBinary, true),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ];
        let source_id = if source_plain { 2 } else { 0 };
        let output_id = if output_json { 3 } else { 1 };
        let source = common::OutputColumn {
            column_id: 1,
            name: "v".into(),
            r#type: Some(reference(source_id)),
            nullable: true,
            ..Default::default()
        };
        let output = common::OutputColumn {
            column_id: 2,
            name: "v_key".into(),
            r#type: Some(reference(output_id)),
            nullable: true,
            ..Default::default()
        };
        let scan = plan::ScanNode {
            columns: vec![source, output.clone()],
            table: Some(plan::TableDef {
                name: "declared_source".into(),
                columns: vec![plan::ColumnDef {
                    name: "v".into(),
                    data_type: Some(reference(source_id)),
                    nullable: true,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            variant_columns: vec![plan::ScanVariantColumn {
                source_column_id: 1,
                source_column: "v".into(),
                synthetic_column_id: 2,
                synthetic_column: "v_key".into(),
                canonical_path: "$.a.b".into(),
                requested_type: Some(reference(1)),
                strict: true,
            }],
            ..Default::default()
        };
        (scan, vec![output], values)
    }
    fn run(
        source_plain: bool,
        output_json: bool,
        control: &Control,
    ) -> Result<NativeVariantPathPlan, crate::fragment_error::NativeFragmentDecodeError> {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "variant path native type owner",
        );
        run_owned(source_plain, output_json, control, tracker, false, false).map(|(plan, _)| plan)
    }
    fn run_owned(
        source_plain: bool,
        output_json: bool,
        control: &Control,
        tracker: std::sync::Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
        handoff: bool,
        unused_extra: bool,
    ) -> Result<
        (
            NativeVariantPathPlan,
            Option<
                std::sync::Arc<
                    dyn novarocks_worker::connector_batch_transform::ConnectorBatchTransform,
                >,
            >,
        ),
        crate::fragment_error::NativeFragmentDecodeError,
    > {
        let (scan, outputs, mut values) = fixture(source_plain, output_json);
        if unused_extra {
            values.push(FunctionValueType::new(
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    Some("x".repeat(1024).into()),
                ),
                true,
            ));
        }
        let roots = values
            .iter()
            .enumerate()
            .map(|(index, value)| ExecutionTypeRoot {
                id: u32::try_from(index).unwrap(),
                value,
            })
            .collect::<Vec<_>>();
        let limits = ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap();
        let mut source_work =
            CompileCheckpoints::try_new(&SourceControl, CompilePhase::Encode).unwrap();
        let table = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed::<std::convert::Infallible>(limits, &mut |_| Ok(()), &mut source_work)
            .unwrap()
            .into_wire();
        source_work.finish().unwrap();
        let creation = NativeTypeConstruction::try_with_control(tracker, control)?;
        let mut resources = NativeDecodeResources::try_new(&creation)?;
        let factory = NativeTypeFactory::new(&creation);
        let types = decode_execution_type_table_with_factory_in(
            &table,
            limits,
            &factory,
            &mut resources.work,
        )
        .map_err(|error| table_failure(error, FieldPath::root("execution_types")))?;
        let output_schema = if handoff {
            let scope = creation.begin_scope()?;
            Some(
                crate::fragment_layout::chunk_schema_from_output_columns_typed_observed(
                    &outputs,
                    FieldPath::root("output"),
                    &types,
                    scope.storage_lifetime(),
                    &mut |layout| scope.request(layout),
                    &mut resources.work,
                )
                .map_err(crate::fragment_type_decode::layout_failure)?,
            )
        } else {
            None
        };
        let mut decoder = NativeTypeDecoder::Strong {
            types: &types,
            original_requests: None,
            resources: &mut resources,
        };
        let result = parse_native_scan_variant_path_columns(
            &scan,
            scan.table.as_ref().unwrap(),
            &outputs,
            &mut decoder,
        )
        .map_err(|error| error.into_native(FieldPath::root("scan")))?;
        let transform = match output_schema {
            Some(output) => crate::fragment_typed_connector_scan::output_materialization(
                &scan,
                &[SlotId::new(1)],
                &output,
                &result,
                &mut decoder,
            )
            .map_err(|error| error.into_native(FieldPath::root("scan")))?,
            None => None,
        };
        drop(decoder);
        resources.finish()?;
        Ok((result, transform))
    }
    #[test]
    fn nonempty_variant_path_uses_actual_strong_owner_and_rejects_same_carrier_domain_drift() {
        let output = run(false, false, &Control::default()).unwrap();
        assert_eq!(output.specs.len(), 1);
        let spec = &output.specs[0];
        assert_eq!(spec.source_slot_id(), SlotId::new(1));
        assert_eq!(spec.output_slot_id(), SlotId::new(2));
        assert_eq!(spec.source_field().data_type(), &DataType::LargeBinary);
        assert_eq!(spec.output_field().data_type(), &DataType::Utf8);
        assert!(spec.source_field().is_nullable());
        assert!(spec.output_field().is_nullable());
        assert_eq!(spec.requested_type(), &DataType::Utf8);
        assert_eq!(spec.canonical_path(), "$.a.b");
        assert_eq!(spec.source_field().metadata().capacity(), 0);
        assert_eq!(spec.output_field().metadata().capacity(), 0);
        let source = run(true, false, &Control::default()).unwrap_err();
        assert!(source.to_string().contains("expects VARIANT/LargeBinary"));
        let output = run(false, true, &Control::default()).unwrap_err();
        assert!(output.to_string().contains("does not match"));
    }
    #[test]
    fn strong_variant_spec_and_transform_keep_only_their_finite_creation_owners() {
        fn late_bytes(unused_extra: bool) -> i64 {
            let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                "late variant extraction owner",
            );
            let control = Control::default();
            let (plan, transform) =
                run_owned(false, false, &control, tracker.clone(), true, unused_extra).unwrap();
            let late = plan.specs[0].clone();
            assert_eq!(
                late.canonical_path().as_ptr(),
                plan.specs[0].canonical_path().as_ptr(),
                "spec clone shares names and path, not another backing copy"
            );
            assert_eq!(
                late.source_binding().unwrap().logical_type(),
                ValueLogicalType::Variant
            );
            assert_eq!(
                late.output_binding().unwrap().logical_type(),
                ValueLogicalType::Physical
            );
            let all = tracker.current();
            drop(plan);
            assert!(
                tracker.current() < all,
                "decoded container exits independently from transform/spec"
            );
            let with_transform = tracker.current();
            drop(transform);
            assert!(
                tracker.current() < with_transform,
                "transform vectors and output scaffold exit independently from late spec"
            );
            drop(control);
            let own = tracker.current();
            assert!(own > 0, "late spec retains actual finite responsibility");
            assert_eq!(late.canonical_path(), "$.a.b");
            drop(late);
            assert_eq!(
                tracker.current(),
                0,
                "last spec releases its creation and original binding owners"
            );
            own
        }
        assert_eq!(
            late_bytes(false),
            late_bytes(true),
            "unused timezone subtree and scratch never join the late spec owner"
        );
    }

    #[test]
    fn actual_variant_batch_owner_keeps_full_source_and_original_memory_control_causes() {
        use novarocks_execution::{
            exec::{
                chunk::{Chunk, ChunkSchema, ChunkSlotSchema},
                pipeline::{
                    operator::{
                        ConstructionFailure,
                        input_construction::try_runtime_input_construction_factory,
                    },
                    pipeline::representation_construction_profile,
                },
            },
            runtime::{
                fragment::{ExecutionFailure, ExecutionFailureCause},
                runtime_state::RuntimeErrorState,
            },
        };
        use std::sync::Arc;
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "actual variant batch creator",
        );
        let (plan, transform) = run_owned(
            false,
            false,
            &Control::default(),
            tracker.clone(),
            true,
            false,
        )
        .unwrap();
        let transform = transform.unwrap();
        let error = Arc::new(RuntimeErrorState::default());
        let error_weak = Arc::downgrade(&error);
        let factory = try_runtime_input_construction_factory(
            representation_construction_profile().unwrap(),
            tracker.clone(),
            error.clone(),
        )
        .unwrap();
        let read = Arc::clone(transform.read_schema());
        let source_owner = factory.begin_typed_construction(&read).unwrap();
        let source = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            read,
            vec![Arc::new(arrow::array::LargeBinaryArray::from(vec![
                None::<&[u8]>,
                None,
            ]))],
            2,
            source_owner.storage_lifetime(),
            |step| source_owner.observe(0, &[], step),
        )
        .unwrap();
        drop(source_owner);
        let output = transform.transform(source, factory.as_ref()).unwrap();
        assert_eq!(output.chunk_schema().slot_ids(), &[SlotId::new(2)]);
        assert_eq!(output.len(), 2);
        assert_eq!(output.columns()[0].null_count(), 2);
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type()
                .kind(),
            &novarocks_type_contract::ValueKind::String
        );
        let storage = ValueTypeStorageLifetime::pure();
        let wrong = FunctionValueType::new(DataType::LargeBinary, true);
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("v", DataType::LargeBinary, true)),
            wrong.typed_binding().clone(),
            None,
            storage.clone(),
        )
        .unwrap();
        let wrong_schema =
            Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap());
        let wrong = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            wrong_schema,
            vec![Arc::new(arrow::array::LargeBinaryArray::from(vec![
                None::<&[u8]>,
            ]))],
            1,
            storage,
            |_| Ok(()),
        )
        .unwrap();
        let rejected = transform.transform(wrong, factory.as_ref()).unwrap_err();
        assert!(matches!(
            rejected.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::InvalidSource(_))
        ));
        let stopped = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        error.set_error(stopped.clone());
        let before = tracker.current();
        let rejected = transform
            .transform(output.clone(), factory.as_ref())
            .unwrap_err();
        assert_eq!(
            rejected, stopped,
            "the caller's original failure is returned without a new wrapper"
        );
        assert_eq!(
            tracker.current(),
            before,
            "original stop refuses before construction and has no request tail"
        );
        drop(transform);
        drop(plan);
        drop(factory);
        drop(error);
        assert!(
            error_weak.upgrade().is_none(),
            "late batch storage does not retain original controller"
        );
        assert!(
            tracker.current() > 0,
            "late output keeps actual structural responsibility"
        );
        drop(output);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn actual_variant_batch_first_memory_refusal_rolls_back_original_tracker() {
        use novarocks_execution::{
            exec::{
                chunk::Chunk,
                pipeline::{
                    operator::{
                        ConstructionFailure,
                        input_construction::try_runtime_input_construction_factory,
                    },
                    pipeline::representation_construction_profile,
                },
            },
            runtime::{fragment::ExecutionFailureCause, runtime_state::RuntimeErrorState},
        };
        use std::sync::Arc;
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "variant runtime memory boundary",
        );
        let (plan, transform) = run_owned(
            false,
            false,
            &Control::default(),
            tracker.clone(),
            true,
            false,
        )
        .unwrap();
        let transform = transform.unwrap();
        let factory = try_runtime_input_construction_factory(
            representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let read = Arc::clone(transform.read_schema());
        let owner = factory.begin_typed_construction(&read).unwrap();
        let source = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            read,
            vec![Arc::new(arrow::array::LargeBinaryArray::from(vec![
                None::<&[u8]>,
            ]))],
            1,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap();
        drop(owner);
        let before = tracker.current();
        tracker
            .install_limit_once(before.checked_add(1).unwrap())
            .unwrap();
        let error = transform
            .transform(source.clone(), factory.as_ref())
            .unwrap_err();
        assert!(
            matches!(error.cause(),ExecutionFailureCause::Construction(ConstructionFailure::Memory(original)) if original.detail().contains("variant runtime memory boundary"))
        );
        assert_eq!(
            tracker.current(),
            before,
            "first actual tracker refusal rolls back and never runs the later kernel"
        );
        drop(source);
        drop(transform);
        drop(plan);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn strong_variant_path_preserves_each_original_control_refusal_without_tail() {
        let baseline = Control::default();
        run_owned(
            false,
            false,
            &baseline,
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                "variant handoff original controls",
            ),
            true,
            false,
        )
        .map(|_| ())
        .unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(
            trace
                .iter()
                .any(|(phase, _)| *phase == CompilePhase::LowerProgram)
        );
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((at, cause)),
                    ..Default::default()
                };
                let error = run_owned(
                    false,
                    false,
                    &control,
                    novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                        "variant handoff refused control",
                    ),
                    true,
                    false,
                )
                .map(|_| ())
                .unwrap_err();
                assert!(
                    matches!(error.construction_cause(),Some(NativeTypeConstructionError::Control(actual)) if *actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    #[derive(Debug)]
    enum ParserOwnerFailure {
        Source(String),
        Native(NativeTypeConstructionError),
    }
    impl From<String> for ParserOwnerFailure {
        fn from(error: String) -> Self {
            Self::Source(error)
        }
    }
    impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
        for ParserOwnerFailure
    {
        fn from(
            error: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        ) -> Self {
            Self::Native(NativeTypeConstructionError::Resource(error))
        }
    }
    fn parse_with_actual_scope(
        creator: &NativeTypeConstruction<'_>,
    ) -> Result<novarocks_types::value::variant::ObservedVariantPath, ParserOwnerFailure> {
        use novarocks_types::value::variant::{
            VariantPathConstructionStep, parse_variant_path_with_storage_observed,
        };
        let scope = creator.begin_scope().map_err(ParserOwnerFailure::Native)?;
        parse_variant_path_with_storage_observed(
            "$['a\\nlongkey'].one.two.three.four.five",
            scope.storage_lifetime(),
            |step| {
                match step {
                    VariantPathConstructionStep::Request(layout) => scope
                        .request_layout(layout)
                        .map_err(ParserOwnerFailure::Native)?,
                    VariantPathConstructionStep::Work(_) => scope
                        .checkpoint(CompilePhase::LowerProgram, 0)
                        .map_err(|cause| {
                            ParserOwnerFailure::Native(NativeTypeConstructionError::Control(cause))
                        })?,
                }
                Ok(())
            },
        )
    }
    #[test]
    fn actual_parsed_path_temporary_memory_refusal_releases_every_successful_prefix() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "actual parsed path constructor",
        );
        let control = Control::default();
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let initial = tracker.current();
        let parsed = parse_with_actual_scope(&creator).unwrap();
        assert_eq!(parsed.segments().len(), 6);
        let requests = tracker.current() - initial;
        assert!(requests > 1);
        drop(parsed);
        assert_eq!(
            tracker.current(),
            initial,
            "no parsed storage remains after its temporary value exits"
        );
        tracker.install_limit_once(initial + requests - 1).unwrap();
        let error = parse_with_actual_scope(&creator).unwrap_err();
        assert!(matches!(
            error,
            ParserOwnerFailure::Native(NativeTypeConstructionError::Capacity(_))
        ));
        assert_eq!(tracker.current(), initial);
        drop(creator);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn actual_path_scope_observes_each_original_control_prefix_without_tail() {
        let baseline = Control::default();
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "path original control baseline",
        );
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &baseline).unwrap();
        let parsed = parse_with_actual_scope(&creator).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        drop(parsed);
        drop(creator);
        assert_eq!(tracker.current(), 0);
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((at, cause)),
                    ..Default::default()
                };
                let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                    "path refused original control",
                );
                let outcome = NativeTypeConstruction::try_with_control(tracker.clone(), &control)
                    .map_err(ParserOwnerFailure::Native)
                    .and_then(|creator| parse_with_actual_scope(&creator));
                assert!(
                    matches!(outcome,Err(ParserOwnerFailure::Native(NativeTypeConstructionError::Control(actual))) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}
