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

//! Exact root occurrence provenance carried through the owned compiler phases.

use novarocks_physical_plan::ResultValueDomain;
use novarocks_result_contract::{ScalarField, ScalarSchema};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::common::OutputColumn;

#[derive(Debug)]
pub(crate) struct RootOutputSemantics {
    occurrences: Box<[(ColumnId, FunctionValueType, ResultValueDomain)]>,
    scalar_field: Option<ScalarField>,
    private_persistence_domain: bool,
}

impl RootOutputSemantics {
    pub(crate) fn capture(
        columns: &[OutputColumn],
        _factory: &ColumnRefFactory,
    ) -> Result<Self, String> {
        if columns.len() > novarocks_result_contract::RootProfileV1::MAX_COLUMNS {
            return Err("root output occurrence count exceeds its admitted profile".into());
        }
        // There can be only one scalar output. Borrow the full declared type
        // for preflight, then construct one bounded owned field; do not clone
        // a nested declaration once per repeated output occurrence.
        let scalar_field = match columns {
            [column] => super::root_scalar_type::scalar_field(&column.value_type).ok(),
            _ => None,
        };
        let private_persistence_domain =
            columns
                .iter()
                .filter(|column| !column.is_internal)
                .any(|column| {
                    crate::planning::mv::has_private_persistence_domain(
                        column.value_type.value_type(),
                    )
                });
        Ok(Self {
            scalar_field,
            private_persistence_domain,
            occurrences: columns
                .iter()
                .map(|column| {
                    let domain = match column.value_type.logical_type() {
                        ValueLogicalType::Json => ResultValueDomain::Json,
                        ValueLogicalType::Variant => ResultValueDomain::Variant,
                        ValueLogicalType::Hll => ResultValueDomain::Hll,
                        ValueLogicalType::Bitmap => ResultValueDomain::Bitmap,
                        ValueLogicalType::Object => ResultValueDomain::Object,
                        ValueLogicalType::Percentile => ResultValueDomain::Percentile,
                        _ => ResultValueDomain::Plain,
                    };
                    (column.column_id, column.value_type.clone(), domain)
                })
                .collect(),
        })
    }

    /// Retain a complete declared private-domain refusal independently of the
    /// scalar schema. Multi-column and unsupported scalar shapes still carry it.
    pub(crate) fn has_private_persistence_domain(&self) -> bool {
        self.private_persistence_domain
    }

    /// Optimizer aliases may repeat one value. They may not reorder or replace
    /// its semantic identity without an explicit compiler handoff.
    pub(crate) fn domains(
        &self,
        columns: &[OutputColumn],
    ) -> Result<Vec<ResultValueDomain>, String> {
        if self.occurrences.len() != columns.len() {
            return Err("root semantic occurrence count changed during optimization".into());
        }
        self.occurrences
            .iter()
            .zip(columns)
            .enumerate()
            .map(|(ordinal, ((id, captured, domain), column))| {
                if *id != column.column_id || !same_captured_binding(captured, &column.value_type) {
                    return Err(format!(
                        "root semantic occurrence changed at ordinal {ordinal}"
                    ));
                }
                Ok(*domain)
            })
            .collect()
    }

    pub(crate) fn scalar_schema(
        self,
        types: &[novarocks_physical_plan::ValueType],
    ) -> Option<ScalarSchema> {
        let [ty] = types else {
            return None;
        };
        let [(_, captured, _)] = self.occurrences.as_ref() else {
            return None;
        };
        if !same_captured_binding(captured, ty) {
            return None;
        }
        let mut field = self.scalar_field?;
        // Nullability is the final lowered value's fact. Move the one owned
        // type tree into the result port; no declaration or tree clone here.
        field.nullable = ty.nullable();
        if !novarocks_type_contract::result_scalar_type::scalar_field_matches_binding(
            &field,
            ty.typed_binding(),
        ) {
            return None;
        }
        ScalarSchema::try_new(field).ok()
    }
}

/// Preserve all captured semantic nodes and precise value layout; only the
/// final lowered root NULL promise may differ. This borrowed comparison creates
/// no projected declaration, field metadata or runtime allocation authority.
fn same_captured_binding(captured: &FunctionValueType, actual: &FunctionValueType) -> bool {
    if captured.value_type().shares_storage(actual.value_type())
        && captured
            .typed_binding()
            .layout()
            .shares_storage(actual.typed_binding().layout())
    {
        return true;
    }
    captured
        .same_physical_binding_ignoring_root_nullability_observed(actual, || {
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::completion::SqlCompletedPlan;
    use arrow::datatypes::{DataType as D, Field};
    use novarocks_physical_plan::ValueType;
    use novarocks_types::schema::SqlType;
    use std::sync::Arc;

    #[test]
    fn unified_root_value_domains_survive_without_a_legacy_declaration() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType as V};
        for (logical, storage, domain, private) in [
            (V::Physical, D::Binary, ResultValueDomain::Plain, false),
            (V::Json, D::Utf8, ResultValueDomain::Json, false),
            (
                V::Variant,
                D::LargeBinary,
                ResultValueDomain::Variant,
                false,
            ),
            (V::Hll, D::Binary, ResultValueDomain::Hll, false),
            (V::Bitmap, D::Binary, ResultValueDomain::Bitmap, false),
            (V::Object, D::Binary, ResultValueDomain::Object, true),
            (
                V::Percentile,
                D::Binary,
                ResultValueDomain::Percentile,
                true,
            ),
            (
                V::LargeInt,
                D::FixedSizeBinary(16),
                ResultValueDomain::Plain,
                false,
            ),
            (
                V::Uuid,
                D::FixedSizeBinary(16),
                ResultValueDomain::Plain,
                false,
            ),
        ] {
            let value_type =
                FunctionValueType::try_with_logical_type(storage, true, logical).unwrap();
            let mut factory = ColumnRefFactory::new();
            let column_id = factory.create(None, "value".into(), value_type.clone());
            assert!(factory.borrowed_logical_type(column_id).is_none());
            let column = OutputColumn {
                column_id,
                name: "value".into(),
                value_type,
                is_internal: false,
            };
            let semantics =
                RootOutputSemantics::capture(std::slice::from_ref(&column), &factory).unwrap();
            assert_eq!(
                semantics.domains(std::slice::from_ref(&column)).unwrap(),
                [domain]
            );
            assert_eq!(semantics.has_private_persistence_domain(), private);
            if logical == V::LargeInt {
                assert_eq!(
                    semantics.scalar_field.as_ref().unwrap().value_type,
                    novarocks_result_contract::ScalarValueType::LargeInt
                );
            }
            if logical == V::Uuid {
                // ScalarValueV1 has no UUID value: never reinterpret the equal
                // FixedSizeBinary(16) carrier as a LargeInt scalar.
                assert!(semantics.scalar_field.is_none());
            }
            if logical == V::Percentile {
                assert_eq!(
                    semantics.scalar_field.as_ref().unwrap().value_type,
                    novarocks_result_contract::ScalarValueType::Opaque(
                        novarocks_result_contract::ScalarOpaqueType::Percentile
                    )
                );
            }
        }
    }

    fn declared_map(nominal: bool) -> (ColumnRefFactory, OutputColumn) {
        use novarocks_type_contract::{
            ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime as Hold,
        };
        let key = ValueTypeRef::try_new(ValueKind::String, false, Hold::pure()).unwrap();
        let value = ValueTypeRef::try_new(
            if nominal {
                ValueKind::Nominal(ValueLogicalType::Hll)
            } else {
                ValueKind::Binary
            },
            true,
            Hold::pure(),
        )
        .unwrap();
        let value =
            ValueTypeRef::try_new(ValueKind::Map { key, value }, false, Hold::pure()).unwrap();
        let layout = D::Map(
            Arc::new(Field::new(
                "entries",
                D::Struct(
                    vec![
                        Arc::new(Field::new("key", D::Utf8, false)),
                        Arc::new(Field::new("value", D::Binary, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let value_type = FunctionValueType::try_from_value(
            value,
            ValueLayout::try_new(&layout, Hold::pure()).unwrap(),
        )
        .unwrap();
        let mut factory = ColumnRefFactory::new();
        let id = factory.create(None, "m".into(), value_type.clone());
        // Deliberately stale catalog summary; it cannot author the output.
        factory.set_logical_type(
            id,
            Some(SqlType::Map(
                Box::new(SqlType::String),
                Box::new(SqlType::Hll),
            )),
        );
        (
            factory,
            OutputColumn {
                column_id: id,
                name: "m".into(),
                value_type,
                is_internal: false,
            },
        )
    }

    fn completed(column: OutputColumn, semantics: RootOutputSemantics) -> SqlCompletedPlan {
        use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
        let physical = PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(crate::planner::payload::PlanValuesNode {
                rows: vec![],
                columns: vec![column.clone()],
            }),
            children: vec![],
            output_columns: vec![column],
            stats: crate::planner::physical::PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: crate::planner::physical::PlannerConfidence::Exact,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            probe_runtime_filters: vec![],
        };
        let builder =
            crate::planner::distributed::build::lower_final_physical_plan_with_root_semantics(
                &physical,
                novarocks_physical_plan::PlanVersionId::try_new([39; 16]).unwrap(),
                novarocks_physical_plan::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                None,
                semantics,
                crate::functions::test_function_catalog_snapshot(),
                false,
                crate::constant::test_constant_policy(),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        let request = crate::compiler::completion::SqlCompileRequest::pending(
            crate::compiler::completion::CompilerStep::ready(
                novarocks_physical_plan::PlanVersionId::try_new([39; 16]).unwrap(),
                builder,
                crate::compiler::SqlDisplayIntent::Execute,
                [],
            ),
            crate::compiler::DEFAULT_COMPLETION_LIMITS,
        );
        let crate::compiler::SqlCompileProgress::Complete(completed) =
            crate::compiler::SqlCompiler::start(
                request,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()
        else {
            panic!("unexpected observation");
        };
        completed
    }

    #[test]
    fn m07_scalar_complete_binding_is_authority_without_factory_declaration() {
        for nominal in [false, true] {
            let (factory, column) = declared_map(nominal);
            assert_eq!(
                column.value_type.data_type().to_string(),
                declared_map(!nominal).1.value_type.data_type().to_string()
            );
            let semantics =
                RootOutputSemantics::capture(std::slice::from_ref(&column), &factory).unwrap();
            let completed = completed(column, semantics);
            let schema = completed
                .scalar_schema()
                .expect("complete metadata-free Map binding");
            let novarocks_result_contract::ScalarValueType::Map { key, value } =
                &schema.field().value_type
            else {
                panic!("expected Map")
            };
            assert!(!key.nullable, "the actual declared Map key is required");
            assert_eq!(
                value.value_type,
                if nominal {
                    novarocks_result_contract::ScalarValueType::Opaque(
                        novarocks_result_contract::ScalarOpaqueType::Hll,
                    )
                } else {
                    novarocks_result_contract::ScalarValueType::Binary
                }
            );
        }
    }

    #[test]
    fn m07_scalar_repeated_nested_occurrences_do_not_clone_a_declaration_tree() {
        let (factory, column) = declared_map(true);
        let repeated = vec![column.clone(); novarocks_result_contract::RootProfileV1::MAX_COLUMNS];
        let semantics = RootOutputSemantics::capture(&repeated, &factory).unwrap();
        assert!(semantics.scalar_field.is_none());
        assert_eq!(semantics.occurrences.len(), repeated.len());
        assert!(
            semantics.occurrences.iter().all(|(_, captured, _)| {
                captured
                    .value_type()
                    .shares_storage(column.value_type.value_type())
                    && captured
                        .typed_binding()
                        .layout()
                        .shares_storage(column.value_type.typed_binding().layout())
            }),
            "repeated occurrences retain the original immutable author without cloning its tree"
        );
        assert!(
            semantics
                .scalar_schema(&vec![
                    ValueType::new(
                        column.value_type.data_type().clone(),
                        true
                    );
                    2
                ])
                .is_none()
        );
        let excessive = vec![column; novarocks_result_contract::RootProfileV1::MAX_COLUMNS + 1];
        assert!(RootOutputSemantics::capture(&excessive, &factory).is_err());
    }

    #[test]
    fn m07_scalar_final_complete_binding_must_keep_captured_nested_domain_and_nulls() {
        let (factory, original) = declared_map(true);
        for domain in [
            None,
            Some(ValueLogicalType::Bitmap),
            Some(ValueLogicalType::Hll),
        ] {
            use novarocks_type_contract::{
                ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime as Hold,
            };
            let key = ValueTypeRef::try_new(ValueKind::String, false, Hold::pure()).unwrap();
            let value = ValueTypeRef::try_new(
                domain.map_or(ValueKind::Binary, ValueKind::Nominal),
                true,
                Hold::pure(),
            )
            .unwrap();
            let value =
                ValueTypeRef::try_new(ValueKind::Map { key, value }, false, Hold::pure()).unwrap();
            let ty = FunctionValueType::try_from_value(
                value,
                ValueLayout::try_new(original.value_type.data_type(), Hold::pure()).unwrap(),
            )
            .unwrap();
            let semantics =
                RootOutputSemantics::capture(std::slice::from_ref(&original), &factory).unwrap();
            assert_eq!(
                semantics.scalar_schema(&[ty]).is_some(),
                domain == Some(ValueLogicalType::Hll)
            );
        }
        let semantics =
            RootOutputSemantics::capture(std::slice::from_ref(&original), &factory).unwrap();
        assert!(
            semantics
                .scalar_schema(&[original.value_type.with_nullable(true).unwrap()])
                .unwrap()
                .field()
                .nullable
        );
        let semantics =
            RootOutputSemantics::capture(std::slice::from_ref(&original), &factory).unwrap();
        let mut changed = original.clone();
        changed.value_type = declared_map(false).1.value_type;
        assert!(
            semantics.domains(&[changed]).is_err(),
            "optimizer cannot replace a captured nested domain"
        );
    }

    #[test]
    fn private_nested_domain_refusal_uses_complete_binding_not_catalog_fallback() {
        use novarocks_type_contract::{
            ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime as Hold,
        };
        let (factory, mut column) = declared_map(false);
        for domain in [ValueLogicalType::Object, ValueLogicalType::Percentile] {
            let key = ValueTypeRef::try_new(ValueKind::String, false, Hold::pure()).unwrap();
            let value =
                ValueTypeRef::try_new(ValueKind::Nominal(domain), true, Hold::pure()).unwrap();
            let value =
                ValueTypeRef::try_new(ValueKind::Map { key, value }, false, Hold::pure()).unwrap();
            column.value_type = FunctionValueType::try_from_value(
                value,
                ValueLayout::try_new(column.value_type.data_type(), Hold::pure()).unwrap(),
            )
            .unwrap();
            assert!(
                RootOutputSemantics::capture(std::slice::from_ref(&column), &factory)
                    .unwrap()
                    .has_private_persistence_domain()
            );
            column.is_internal = true;
            assert!(
                !RootOutputSemantics::capture(std::slice::from_ref(&column), &factory)
                    .unwrap()
                    .has_private_persistence_domain()
            );
            column.is_internal = false;
        }
    }
}
