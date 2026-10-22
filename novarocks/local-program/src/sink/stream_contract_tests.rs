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

//! The sender must accept precisely the declared hierarchical key domain.

use super::*;
use crate::{StaticExprKind, StaticExprNode};
use arrow_schema::DataType;
use std::collections::HashMap;

fn stream(key_type: DataType, conditional: bool) -> Result<StaticSinkProgram, StaticSinkError> {
    let mut nodes = vec![StaticExprNode::new(
        StaticExprKind::SlotId(SlotId::new(1)),
        key_type,
        None,
    )];
    let routing = if conditional {
        nodes.push(StaticExprNode::new(
            StaticExprKind::SlotId(SlotId::new(2)),
            DataType::Boolean,
            None,
        ));
        StaticStreamRouting::ConditionalKeyV1 {
            applies: ProgramExprId::new(1),
        }
    } else {
        StaticStreamRouting::HierarchicalHashV1
    };
    let arena =
        Arc::new(ImmutableExpressions::try_new(nodes, false, HashMap::new(), None).unwrap());
    let branch = StaticStreamBranch::try_new(
        1,
        DataStreamPartitionType::HashPartitioned,
        vec![ProgramExprId::new(0)],
        vec![SlotId::new(1)],
        None,
        StaticStreamContract {
            role: ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            routing,
        },
    )
    .unwrap();
    StaticSinkProgram::try_data_stream(branch, arena)
}

#[test]
fn hierarchical_and_conditional_sender_refuse_keys_outside_their_signed_algorithm_domain() {
    for conditional in [false, true] {
        stream(DataType::Int64, conditional)
            .expect("a canonical native key is valid before fault injection");
        for bad in [
            DataType::UInt64,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            assert!(
                matches!(
                    stream(bad, conditional),
                    Err(StaticSinkError::InvalidExpression)
                ),
                "a same-shaped expression is insufficient evidence of supported key semantics"
            );
        }
    }
}
