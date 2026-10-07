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

//! Slot-only JSON membership with exact producer/probe materialization.

use super::*;
use crate::planner::membership::PlanMembershipNode;
use novarocks_physical_plan::{MembershipSpec, ValueLogicalKind};

impl ContractLoweringVisitor {
    pub(super) fn lower_membership(
        &mut self,
        plan: &PhysicalPlanNode,
        spec: &PlanMembershipNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        require_output_shape("Membership", &plan.output_columns, &spec.output_columns)?;
        let probe = self.lower_node(&plan.children[0])?;
        let build = self.lower_node(&plan.children[1])?;
        if probe.fragment != self.current_fragment || build.fragment != self.current_fragment {
            return Err(invalid_write(
                "membership children escaped their owning fragment".into(),
            ));
        }
        let probe_value = probe
            .columns
            .get(&spec.probe)
            .copied()
            .ok_or(ContractLoweringError::UnknownColumnReference(spec.probe))?;
        let build_value = build
            .columns
            .get(&spec.build)
            .copied()
            .ok_or(ContractLoweringError::UnknownColumnReference(spec.build))?;
        for value in [probe_value, build_value] {
            if self.value_logical_kind_in(self.current_fragment, value)?
                != Some(ValueLogicalKind::Json)
            {
                return Err(invalid_write(
                    "membership operand lacks exact analyzed JSON evidence".into(),
                ));
            }
        }
        if spec.result.data_type != DataType::Boolean
            || !spec.result.nullable
            || probe.columns.contains_key(&spec.result.column_id)
            || build.columns.contains_key(&spec.result.column_id)
        {
            return Err(invalid_write(
                "membership result must be a fresh nullable Boolean".into(),
            ));
        }
        let node = self.fragment_mut().reserve_node_id()?;
        let result = self.fragment_mut().add_value(
            value_type(&spec.result),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: checked_ordinal("Membership result", probe.output.len())?,
            },
        )?;
        self.fragment_mut().add_membership(
            node,
            probe.node,
            build.node,
            MembershipSpec {
                probe: probe_value,
                build: build_value,
                result,
                negated: spec.negated,
                comparison: spec.comparison,
                distribution: spec.distribution,
            },
        )?;
        let mut columns = probe.columns;
        columns.insert(spec.result.column_id, result);
        let mut output = probe.output.into_vec();
        output.push(result);
        let mut display_names = probe.display_names.into_vec();
        display_names.push(spec.result.name.clone());
        if plan.output_columns.len() != output.len()
            || plan
                .output_columns
                .iter()
                .zip(&output)
                .any(|(c, v)| columns.get(&c.column_id) != Some(v))
        {
            return Err(invalid_write(
                "membership output must be the exact probe bag and fresh result".into(),
            ));
        }
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties: probe.properties,
            display_names: display_names.into_boxed_slice(),
        })
    }
}
