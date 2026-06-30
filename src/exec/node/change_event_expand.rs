use crate::common::ids::SlotId;
use crate::exec::expr::ExprId;
use crate::sql::common::ChangeStreamBranchKind;

use super::ExecNode;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChangeEventExpandNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub events: Vec<ChangeEventExecSpec>,
    pub output_slot_ids: Vec<SlotId>,
    pub change_op_slot_id: SlotId,
    pub data_route_slot_id: Option<SlotId>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChangeEventExecSpec {
    pub predicate: Option<ExprId>,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub assignments: Vec<ChangeEventExecOutputExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChangeEventExecOutputExpr {
    pub output_slot_id: SlotId,
    pub expr: Option<ExprId>,
}
