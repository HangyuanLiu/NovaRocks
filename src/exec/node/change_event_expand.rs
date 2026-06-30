use crate::common::ids::SlotId;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::sql::common::ChangeStreamBranchKind;

use super::ExecNode;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChangeEventExpandNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub events: Vec<ChangeEventRuntimeSpec>,
    pub output_slot_ids: Vec<SlotId>,
    pub output_chunk_schema: ChunkSchemaRef,
    pub change_op_slot_id: SlotId,
    pub data_route_slot_id: Option<SlotId>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChangeEventRuntimeSpec {
    pub predicate: Option<ExprId>,
    pub branch_kind: ChangeStreamBranchKind,
    pub assignments: Vec<ChangeEventRuntimeOutputExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChangeEventRuntimeOutputExpr {
    pub output_slot_id: SlotId,
    pub expr: Option<ExprId>,
}
