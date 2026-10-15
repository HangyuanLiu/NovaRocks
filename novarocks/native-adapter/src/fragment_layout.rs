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

//! Native output-layout and exchange-input contract projections.
//!
//! All values here are immutable projections from the native plan wire.  The
//! Backend remains the owner of plan lowering and fragment lifecycle.

use std::sync::Arc;

use novarocks_execution::exec::chunk::{ChunkSchemaRef, ChunkSlotSchema};
#[cfg(test)]
use novarocks_proto_codec::FieldPath;
#[cfg(test)]
use novarocks_proto_models::{common, plan};
use novarocks_types::SlotId;

#[cfg(test)]
use crate::fragment_error::NativeFragmentDecodeError;

mod typed;
pub use typed::{
    NativeExchangeContracts, NativeLayoutProjectionError,
    chunk_schema_from_output_columns_typed_observed, decode_exchange_contracts_typed_observed,
    decode_fragment_output_layout_typed_observed, decode_output_layout_typed_observed,
    decode_output_layout_typed_with_factory_in, slot_schema_from_binding_typed_observed,
};

#[derive(Clone, Debug)]
pub struct NativeOutputLayout(Arc<NativeOutputLayoutStorage>);

#[derive(Debug)]
struct NativeOutputLayoutStorage {
    slot_ids: Vec<SlotId>,
    chunk_schema: ChunkSchemaRef,
    slot_schemas: Vec<ChunkSlotSchema>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

impl NativeOutputLayout {
    fn allocation_layout() -> Result<
        std::alloc::Layout,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            NativeOutputLayoutStorage,
        >())
    }

    fn new_with_storage(
        slot_ids: Vec<SlotId>,
        chunk_schema: ChunkSchemaRef,
        slot_schemas: Vec<ChunkSlotSchema>,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        Self(Arc::new(NativeOutputLayoutStorage {
            slot_ids,
            chunk_schema,
            slot_schemas,
            _storage: storage,
        }))
    }
    pub fn slot_ids(&self) -> &[SlotId] {
        &self.0.slot_ids
    }
    pub fn chunk_schema(&self) -> ChunkSchemaRef {
        self.0.chunk_schema.clone()
    }
    pub fn slot_schemas(&self) -> &[ChunkSlotSchema] {
        &self.0.slot_schemas
    }
}

#[cfg(test)]
pub(crate) fn decode_output_layout(
    columns: &[common::OutputColumn],
    path: FieldPath,
) -> Result<NativeOutputLayout, NativeFragmentDecodeError> {
    crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
        decoder.output_layout(columns, path)
    })
}
#[cfg(test)]
fn decode_exchange_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<
    novarocks_execution::exec::fragment::program::ExchangeInputContracts,
    NativeFragmentDecodeError,
> {
    crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
        decoder.exchange_contracts(root, path)
    })
}
#[cfg(test)]
mod tests {
    use super::{decode_exchange_contracts, decode_output_layout};
    use novarocks_proto_codec::FieldPath;
    #[cfg(test)]
    use novarocks_proto_models::{common, plan};
    use novarocks_types::SlotId;

    fn int_column(column_id: u32, name: &str) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            nullable: true,
            r#type: Some(crate::fragment_expression::fixture_sources::type_desc(
                &arrow::datatypes::DataType::Int32,
            )),
            is_internal: false,
        }
    }

    #[test]
    fn decodes_layout_slots_schema_and_slot_schemas_together() {
        let layout = decode_output_layout(&[int_column(3, "id")], FieldPath::root("columns"))
            .expect("layout decodes");
        assert_eq!(layout.slot_ids(), &[SlotId::new(3)]);
        assert_eq!(layout.chunk_schema().slots().len(), 1);
        assert_eq!(layout.slot_schemas()[0].slot_id(), SlotId::new(3));
    }

    #[test]
    fn preserves_duplicate_slot_error_path() {
        let error = decode_output_layout(
            &[int_column(3, "id"), int_column(3, "id")],
            FieldPath::root("columns"),
        )
        .expect_err("duplicate slots must fail");
        assert_eq!(
            error.to_string(),
            "native protocol error at columns[1].column_id (inconsistent fields): duplicate OutputColumn.column_id 3 at index 1 (first seen at index 0)"
        );
    }

    fn exchange_node(
        node_id: i32,
        output_columns: Vec<common::OutputColumn>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    output_columns,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    #[test]
    fn decodes_exchange_schema_without_backend_decoder() {
        let root = exchange_node(7, vec![int_column(3, "id")]);
        let contracts =
            decode_exchange_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect("decode exchange contract");
        let schema = contracts
            .get(&novarocks_execution::exec::fragment::program::FragmentNodeId::new(7))
            .expect("exchange contract for node");
        assert_eq!(schema.expected_schema().slots().len(), 1);
        assert_eq!(
            schema.expected_schema().slots()[0].slot_id(),
            SlotId::new(3)
        );
    }

    #[test]
    fn preserves_exchange_output_column_error_path() {
        let root = exchange_node(
            7,
            vec![common::OutputColumn {
                column_id: 3,
                name: "id".to_string(),
                nullable: true,
                r#type: None,
                is_internal: false,
            }],
        );
        let error =
            decode_exchange_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect_err("missing output type must fail");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root.payload.exchange.output_columns[0].type (missing field): OutputColumn.type missing for column_id=3 name='id' at index 0"
        );
    }
}
