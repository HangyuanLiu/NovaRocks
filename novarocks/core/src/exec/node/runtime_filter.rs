// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Kernel-local runtime-filter carriers.
//!
//! Runtime-filter semantics are frozen in `novarocks-execution`.  Core retains
//! only the expression and operator coordinates required to invoke that
//! contract from a pipeline kernel.

use std::sync::Arc;

pub use execution::RuntimeFilterExecutionContract;
pub use execution::RuntimeFilterReduction as RuntimeFilterExecutionReduction;
use novarocks_execution::runtime_filter as execution;

use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
use crate::runtime_filter::model::contract::{NullOrder, SortDirection};
use crate::runtime_filter::port::ordered_bound::RuntimeOrderKey;

pub(crate) fn core_order_keys(keys: &[execution::RuntimeOrderKey]) -> Arc<[RuntimeOrderKey]> {
    keys.iter()
        .map(|key| {
            RuntimeOrderKey::new(
                key.data_type().clone(),
                match key.direction() {
                    execution::RuntimeOrderSortDirection::Ascending => SortDirection::Ascending,
                    execution::RuntimeOrderSortDirection::Descending => SortDirection::Descending,
                },
                match key.null_order() {
                    execution::RuntimeOrderNullOrder::First => NullOrder::First,
                    execution::RuntimeOrderNullOrder::Last => NullOrder::Last,
                },
            )
        })
        .collect::<Vec<_>>()
        .into()
}

pub(crate) fn execution_order_keys(keys: &[RuntimeOrderKey]) -> Arc<[execution::RuntimeOrderKey]> {
    keys.iter()
        .map(|key| {
            execution::RuntimeOrderKey::new(
                key.data_type().clone(),
                match key.direction() {
                    SortDirection::Ascending => execution::RuntimeOrderSortDirection::Ascending,
                    SortDirection::Descending => execution::RuntimeOrderSortDirection::Descending,
                },
                match key.null_order() {
                    NullOrder::First => execution::RuntimeOrderNullOrder::First,
                    NullOrder::Last => execution::RuntimeOrderNullOrder::Last,
                },
            )
        })
        .collect::<Vec<_>>()
        .into()
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterConsumerBinding {
    pub(crate) expr_id: ExprId,
    pub(crate) contract: execution::RuntimeFilterConsumerContract,
    /// Present only for a connector scan whose FE-pinned source boundary is
    /// eligible for scan-unit pre-reader evaluation. Core carries this sealed
    /// value but does not interpret scan-domain facts or decisions.
    pub(crate) scan_domain: Option<execution::scan_domain::RuntimeFilterScanDomainBinding>,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterConsumerNode {
    pub(crate) input: Box<ExecNode>,
    pub(crate) owner_node_id: i32,
    pub(crate) bindings: Vec<RuntimeFilterConsumerBinding>,
}

impl RuntimeFilterConsumerNode {
    pub fn new(
        input: ExecNode,
        owner_node_id: i32,
        bindings: Vec<RuntimeFilterConsumerBinding>,
    ) -> Self {
        Self {
            input: Box::new(input),
            owner_node_id,
            bindings,
        }
    }

    pub fn input(&self) -> &ExecNode {
        &self.input
    }

    pub fn input_mut(&mut self) -> &mut ExecNode {
        &mut self.input
    }
}

impl RuntimeFilterConsumerBinding {
    pub const fn new(
        expr_id: ExprId,
        contract: execution::RuntimeFilterConsumerContract,
        scan_domain: Option<execution::scan_domain::RuntimeFilterScanDomainBinding>,
    ) -> Self {
        Self {
            expr_id,
            contract,
            scan_domain,
        }
    }

    pub const fn contract(&self) -> &execution::RuntimeFilterConsumerContract {
        &self.contract
    }

    pub const fn binding_id(&self) -> u32 {
        self.contract.binding_id().get()
    }

    pub const fn channel_id(&self) -> u32 {
        self.contract.channel_id().get()
    }

    pub const fn activation(&self) -> execution::ConsumerActivation {
        self.contract.activation()
    }

    pub const fn execution_contract(&self) -> &execution::RuntimeFilterExecutionContract {
        self.contract.contract()
    }
}

/// Builds a deliberately permissive consumer contract for Core-only legacy
/// fixtures. Production fragment decoding must use the role-specific
/// constructors in `novarocks-execution`.
#[cfg(test)]
pub(crate) fn test_consumer_contract(
    binding_id: u32,
    channel_id: u32,
    activation: crate::runtime_filter::model::contract::ConsumerActivation,
    contract: execution::RuntimeFilterExecutionContract,
) -> execution::RuntimeFilterConsumerContract {
    let activation = match activation {
        crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot => {
            execution::ConsumerActivation::BlockingSnapshot
        }
        crate::runtime_filter::model::contract::ConsumerActivation::NonBlockingLive {
            late_apply,
        } => execution::ConsumerActivation::NonBlockingLive {
            late_apply: match late_apply {
                crate::runtime_filter::model::contract::LateApplyGranularity::Row => {
                    execution::RuntimeFilterLateApplyGranularity::Row
                }
                crate::runtime_filter::model::contract::LateApplyGranularity::Batch => {
                    execution::RuntimeFilterLateApplyGranularity::Batch
                }
                crate::runtime_filter::model::contract::LateApplyGranularity::RowGroup => {
                    execution::RuntimeFilterLateApplyGranularity::RowGroup
                }
                crate::runtime_filter::model::contract::LateApplyGranularity::Split => {
                    execution::RuntimeFilterLateApplyGranularity::Split
                }
                crate::runtime_filter::model::contract::LateApplyGranularity::File => {
                    execution::RuntimeFilterLateApplyGranularity::File
                }
            },
        },
    };
    execution::RuntimeFilterConsumerContract::new(
        execution::RuntimeFilterBindingId::new(binding_id),
        execution::RuntimeFilterChannelId::new(channel_id),
        activation,
        contract,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        ConsumerActivation, RuntimeFilterBindingId, RuntimeFilterChannelId,
        RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
    };

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprArena, ExprNode};

    #[test]
    fn consumer_carrier_retains_only_kernel_coordinate_and_execution_contract() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let contract = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from([1_u8]),
                schema_digest: [2; 32],
            },
        )
        .expect("membership consumer contract");
        let binding = RuntimeFilterConsumerBinding::new(expr_id, contract, None);

        assert_eq!(binding.expr_id, expr_id);
        assert_eq!(binding.contract().binding_id().get(), 1);
        assert_eq!(binding.contract().channel_id().get(), 2);
        assert_eq!(
            binding.contract().activation(),
            ConsumerActivation::BlockingSnapshot
        );
    }
}
