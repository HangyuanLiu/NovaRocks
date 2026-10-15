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

//! Native filter-node decoding.

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::DecodedNode;
    use super::super::NativePlanDecodeContext;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_models::{common, expr, plan};
    use novarocks_types::SlotId;

    fn decode_node(
        node: &plan::DistributedNode,
        arena: &mut ExprArena,
        context: &NativePlanDecodeContext,
    ) -> Result<DecodedNode, crate::fragment_error::NativeFragmentDecodeError> {
        crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
            super::super::decode_node_with_types(node, arena, context, decoder)
        })
    }

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        crate::fragment_expression::fixture_sources::type_desc(data_type)
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        let literal = expr::LiteralExpr {
            value: Some(common::LiteralValue {
                value: Some(common::literal_value::Value::IntValue(value)),
            }),
        };
        expr::Expr {
            original_definition_id: None,
            r#type: Some(
                crate::fragment_expression::fixture_sources::type_desc_with_nullable(
                    &DataType::Int64,
                    false,
                ),
            ),
            nullable: false,
            kind: Some(expr::expr::Kind::ConstantReference(
                crate::fragment_expression::fixture_sources::constant_reference(
                    &DataType::Int64,
                    &literal,
                ),
            )),
        }
    }

    fn bool_literal(value: bool) -> expr::Expr {
        let literal = expr::LiteralExpr {
            value: Some(common::LiteralValue {
                value: Some(common::literal_value::Value::BoolValue(value)),
            }),
        };
        expr::Expr {
            original_definition_id: None,
            r#type: Some(
                crate::fragment_expression::fixture_sources::type_desc_with_nullable(
                    &DataType::Boolean,
                    false,
                ),
            ),
            nullable: false,
            kind: Some(expr::expr::Kind::ConstantReference(
                crate::fragment_expression::fixture_sources::constant_reference(
                    &DataType::Boolean,
                    &literal,
                ),
            )),
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_filter_limit_shape() {
        let filter = physical_node(
            20,
            plan::plan_node::Kind::Filter(plan::FilterNode {
                predicate: Some(bool_literal(true)),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let limit = physical_node(
            30,
            plan::plan_node::Kind::Limit(plan::LimitNode {
                limit: Some(5),
                offset: Some(1),
            }),
            Vec::new(),
            vec![filter],
        );

        let lowered = lower(&limit);
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 30);
        assert_eq!(limit.limit, Some(5));
        assert_eq!(limit.offset, 1);
        assert!(matches!(limit.input.kind, ExecNodeKind::Filter(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }
}
