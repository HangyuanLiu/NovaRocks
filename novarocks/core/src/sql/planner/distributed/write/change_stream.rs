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

//! SQL-owned row-mutation router topology.
//!
//! A sealed SQL plan carries only logical effects and opaque provider routes.
//! It neither names nor infers a table-format strategy, row identity layout,
//! or a physical data/delete branch.

use std::collections::{BTreeSet, HashMap, HashSet};

use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteCohortId,
    ConnectorWriteFieldToken, ConnectorWriteRouteId,
};

use crate::sql::analysis::OutputColumn;

use super::super::FragmentId;
use super::contract::SqlWritePlanInput;

/// One provider-signed route projected into a SQL router. The route id is
/// opaque; its effect set and token-to-output-ordinal mapping are the only
/// data needed by generic SQL topology construction.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteLayoutRoute {
    pub(crate) route_id: ConnectorWriteRouteId,
    pub(crate) cohort_id: ConnectorWriteCohortId,
    pub(crate) accepted_effects: Vec<ConnectorRowMutationEffect>,
    /// In provider input-field order, map each token to a producer output
    /// ordinal. SQL never recovers that mapping from an internal column name.
    pub(crate) input_ordinals: Vec<ConnectorMutationRouteInput>,
    /// The provider-selected subset of `input_ordinals` used for partitioning.
    pub(crate) partition_input_tokens: Vec<ConnectorWriteFieldToken>,
    pub(crate) sink: SqlWritePlanInput,
}

/// Planner-owned input for binding a provider-selected route set to one
/// immutable producer layout. The effect ordinal comes from the match
/// contract's effect field, not a reserved name lookup.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteLayoutRequest<'a> {
    pub(crate) producer_output_columns: &'a [OutputColumn],
    pub(crate) effect_output_ordinal: usize,
    pub(crate) routes: Vec<ChangeStreamWriteLayoutRoute>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteRouteSpec {
    pub(crate) route_id: ConnectorWriteRouteId,
    pub(crate) cohort_id: ConnectorWriteCohortId,
    pub(crate) accepted_effects: Vec<ConnectorRowMutationEffect>,
    pub(crate) input_ordinals: Vec<ConnectorMutationRouteInput>,
    pub(crate) output_partition_ordinals: Vec<usize>,
    pub(crate) sink: SqlWritePlanInput,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteDagSpec {
    pub(crate) effect_output_ordinal: usize,
    pub(crate) routes: Vec<ChangeStreamWriteRouteSpec>,
}

#[derive(Clone, Debug)]
pub struct ChangeStreamRouterSink {
    pub(crate) group_id: i32,
    pub(crate) effect_output_ordinal: usize,
    pub(crate) routes: Vec<ChangeStreamRoute>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamRoute {
    pub(crate) route_id: ConnectorWriteRouteId,
    pub(crate) cohort_id: ConnectorWriteCohortId,
    pub(crate) accepted_effects: Vec<ConnectorRowMutationEffect>,
    pub(crate) input_ordinals: Vec<ConnectorMutationRouteInput>,
    pub(crate) target_fragment_id: FragmentId,
    pub(crate) target_exchange_node_id: i32,
    pub(crate) output_partition_ordinals: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct SqlChangeStreamWriteTopology {
    pub(crate) writer_routes: Vec<SqlChangeStreamWriterRoute>,
}

#[derive(Clone, Debug)]
pub(crate) struct SqlChangeStreamWriterRoute {
    pub(crate) route_id: ConnectorWriteRouteId,
    pub(crate) cohort_id: ConnectorWriteCohortId,
    pub(crate) accepted_effects: Vec<ConnectorRowMutationEffect>,
    pub(crate) writer_fragment_id: FragmentId,
    pub(crate) sink: SqlWritePlanInput,
}

impl ChangeStreamWriteDagSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_route_set(&self.routes)
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        effect_output_ordinal: usize,
        routes: Vec<ChangeStreamWriteRouteSpec>,
    ) -> Self {
        Self {
            effect_output_ordinal,
            routes,
        }
    }
}

pub(crate) fn validate_route_set(routes: &[ChangeStreamWriteRouteSpec]) -> Result<(), String> {
    if routes.is_empty() {
        return Err("row-mutation router requires at least one route".to_string());
    }
    let mut route_ids = BTreeSet::new();
    for route in routes {
        if !route_ids.insert(route.route_id) {
            return Err("row-mutation router contains a duplicate opaque route id".to_string());
        }
        validate_effects(&route.accepted_effects)?;
        validate_input_ordinals(&route.input_ordinals)?;
    }
    Ok(())
}

/// Bind provider-signed route inputs to the immutable SQL producer layout.
/// No identifier string is interpreted as row identity, a physical branch, or
/// a data route at this boundary.
pub(crate) fn bind_change_stream_write_layout(
    mut request: ChangeStreamWriteLayoutRequest<'_>,
) -> Result<ChangeStreamWriteDagSpec, String> {
    if request.routes.is_empty() {
        return Err("row-mutation router requires at least one route".to_string());
    }
    validate_output_ordinal(
        request.producer_output_columns,
        request.effect_output_ordinal,
        "effect",
    )?;

    let mut routes = Vec::with_capacity(request.routes.len());
    for route in request.routes.drain(..) {
        validate_effects(&route.accepted_effects)?;
        validate_input_ordinals(&route.input_ordinals)?;
        validate_output_ordinals(
            request.producer_output_columns,
            &route
                .input_ordinals
                .iter()
                .map(|binding| binding.input_ordinal() as usize)
                .collect::<Vec<_>>(),
            "route input",
        )?;

        let by_token: HashMap<_, _> = route
            .input_ordinals
            .iter()
            .map(|binding| (binding.token(), binding.input_ordinal() as usize))
            .collect();
        let mut partition_tokens = HashSet::new();
        let output_partition_ordinals = route
            .partition_input_tokens
            .iter()
            .map(|token| {
                if !partition_tokens.insert(*token) {
                    return Err(
                        "row-mutation route has a duplicate partition input token".to_string()
                    );
                }
                by_token.get(token).copied().ok_or_else(|| {
                    "row-mutation route partition token is not bound to an input ordinal"
                        .to_string()
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        routes.push(ChangeStreamWriteRouteSpec {
            route_id: route.route_id,
            cohort_id: route.cohort_id,
            accepted_effects: route.accepted_effects,
            input_ordinals: route.input_ordinals,
            output_partition_ordinals,
            sink: route.sink,
        });
    }

    let dag = ChangeStreamWriteDagSpec {
        effect_output_ordinal: request.effect_output_ordinal,
        routes,
    };
    dag.validate()?;
    Ok(dag)
}

fn validate_effects(effects: &[ConnectorRowMutationEffect]) -> Result<(), String> {
    if effects.is_empty() {
        return Err("row-mutation route must accept at least one logical effect".to_string());
    }
    let mut seen = BTreeSet::new();
    if effects.iter().any(|effect| !seen.insert(*effect)) {
        return Err("row-mutation route has a duplicate accepted effect".to_string());
    }
    Ok(())
}

fn validate_input_ordinals(bindings: &[ConnectorMutationRouteInput]) -> Result<(), String> {
    if bindings.is_empty() {
        return Err("row-mutation route must bind at least one input token".to_string());
    }
    let mut tokens = HashSet::new();
    let mut ordinals = HashSet::new();
    if bindings
        .iter()
        .any(|binding| !tokens.insert(binding.token()) || !ordinals.insert(binding.input_ordinal()))
    {
        return Err("row-mutation route has duplicate input token or ordinal".to_string());
    }
    Ok(())
}

pub(crate) fn route_output_ordinals(route: &ChangeStreamWriteRouteSpec) -> Vec<usize> {
    route
        .input_ordinals
        .iter()
        .map(|binding| binding.input_ordinal() as usize)
        .collect()
}

pub(crate) fn validate_output_ordinal(
    output_columns: &[OutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<(), String> {
    if ordinal >= output_columns.len() {
        return Err(format!(
            "row-mutation {label} output ordinal {ordinal} is out of range for {} columns",
            output_columns.len()
        ));
    }
    Ok(())
}

pub(crate) fn validate_output_ordinals(
    output_columns: &[OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<(), String> {
    for ordinal in ordinals {
        validate_output_ordinal(output_columns, *ordinal, label)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::contract::ConnectorWriteInputBinding;
    use super::super::contract::test_support::simple_sql_write_plan_input;
    use super::*;

    fn output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: crate::sql::column_id::ColumnId::new_for_test(1),
                name: "before_value".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            OutputColumn {
                column_id: crate::sql::column_id::ColumnId::new_for_test(2),
                name: "effect".to_string(),
                data_type: DataType::Int8,
                nullable: false,
                is_internal: true,
            },
        ]
    }

    fn route(byte: u8, effects: Vec<ConnectorRowMutationEffect>) -> ChangeStreamWriteLayoutRoute {
        ChangeStreamWriteLayoutRoute {
            route_id: ConnectorWriteRouteId::from_bytes([byte; 32]),
            cohort_id: ConnectorWriteCohortId::from_bytes([byte.wrapping_add(1); 32]),
            accepted_effects: effects,
            input_ordinals: vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([byte; 32]),
                0,
            )],
            partition_input_tokens: Vec::new(),
            sink: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
        }
    }

    #[test]
    fn bind_layout_keeps_replace_fanout_as_two_opaque_routes() {
        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![
                route(1, vec![ConnectorRowMutationEffect::Replace]),
                route(2, vec![ConnectorRowMutationEffect::Replace]),
            ],
        })
        .expect("replace fanout is valid");
        assert_eq!(dag.routes.len(), 2);
        assert!(
            dag.routes
                .iter()
                .all(|route| { route.accepted_effects == [ConnectorRowMutationEffect::Replace] })
        );
    }

    #[test]
    fn bind_layout_rejects_duplicate_opaque_route_id() {
        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![
                route(1, vec![ConnectorRowMutationEffect::Delete]),
                route(1, vec![ConnectorRowMutationEffect::Insert]),
            ],
        })
        .expect_err("duplicate route id");
        assert!(error.contains("duplicate opaque route id"));
    }

    #[test]
    fn bind_layout_rejects_foreign_partition_token() {
        let mut route = route(1, vec![ConnectorRowMutationEffect::Delete]);
        route.partition_input_tokens = vec![ConnectorWriteFieldToken::from_bytes([9; 32])];
        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![route],
        })
        .expect_err("foreign partition token");
        assert!(error.contains("partition token is not bound"));
    }
}
