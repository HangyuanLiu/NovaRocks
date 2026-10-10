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

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::optimizer::memo::{GroupId, MExpr, Memo};
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;

/// Copy a construction tree in source post-order using the request's control.
/// Partial groups remain private to the caller if observation fails; no root is
/// returned until the final pending work has been observed.
pub(crate) fn opt_expr_to_memo(
    expr: &OptExpr,
    memo: &mut Memo,
    control: &dyn PureCompileControl,
) -> Result<GroupId, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let mut next_id = super::next_expr_id_observed(memo, &mut work)?;
    struct Frame<'a> {
        expr: &'a OptExpr,
        next_child: usize,
        children: Vec<GroupId>,
    }
    let mut stack = vec![Frame {
        expr,
        next_child: 0,
        children: Vec::new(),
    }];
    let root = loop {
        let frame = stack.last_mut().expect("copy stack contains its root");
        if let Some(child) = frame.expr.children.get(frame.next_child) {
            work.step()?;
            frame.next_child += 1;
            stack.push(Frame {
                expr: child,
                next_child: 0,
                children: Vec::new(),
            });
            continue;
        }
        let frame = stack.pop().expect("finished copy frame");
        work.step()?;
        // Operator payloads still use their existing opaque clone. These
        // observations bound interruption around it, not its internal work.
        control.checkpoint(CompilePhase::Validate, 0)?;
        let op = frame.expr.op.clone();
        control.checkpoint(CompilePhase::Validate, 0)?;
        let mexpr = MExpr {
            id: next_id,
            op,
            children: frame.children,
        };
        next_id = next_id
            .checked_add(1)
            .ok_or(SqlCompileError::ResourceExhausted)?;
        let group = memo.new_group(mexpr);
        if let Operator::LogicalCTEProduce(op) = &frame.expr.op {
            memo.cte_produce_groups.insert(op.cte_id, group);
        }
        match stack.last_mut() {
            Some(parent) => parent.children.push(group),
            None => break group,
        }
    };
    work.finish()?;
    Ok(root)
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use crate::optimizer::operator::{CTEProduceOp, UnionOp, ValuesOp};
    use novarocks_type_contract::CompileControlError;
    use std::sync::{Arc, Mutex};

    struct Control {
        units: Mutex<Vec<u32>>,
        fail_at: Option<u32>,
        error: CompileControlError,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.units.lock().unwrap().push(units);
            if self.fail_at == Some(units) {
                Err(self.error)
            } else {
                Ok(())
            }
        }
    }
    fn control(fail_at: Option<u32>, error: CompileControlError) -> Control {
        Control {
            units: Mutex::new(Vec::new()),
            fail_at,
            error,
        }
    }
    fn leaf() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }
    fn wide() -> OptExpr {
        OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: vec![vec![]; 320],
            }),
            (0..320).map(|_| leaf()).collect(),
        )
    }
    fn assert_class(error: SqlCompileError, expected: CompileControlError) {
        assert!(matches!(
            (error, expected),
            (SqlCompileError::Cancelled, CompileControlError::Cancelled)
                | (
                    SqlCompileError::DeadlineExceeded,
                    CompileControlError::DeadlineExceeded
                )
                | (
                    SqlCompileError::ResourceExhausted,
                    CompileControlError::ResourceExhausted
                )
        ));
    }
    #[test]
    fn memo_copy_control_entry_batch_and_finish_preserve_typed_errors() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for (expr, fail_at) in [(leaf(), 0), (wide(), 256), (leaf(), 1)] {
                let owner = control(Some(fail_at), error);
                let mut memo = Memo::new();
                assert_class(
                    opt_expr_to_memo(&expr, &mut memo, &owner).unwrap_err(),
                    error,
                );
                if fail_at == 0 {
                    assert!(memo.groups.is_empty());
                }
                assert!(owner.units.lock().unwrap().contains(&fail_at));
            }
        }
    }
    #[test]
    fn memo_copy_control_preserves_postorder_ids_cte_index_and_drops_owner() {
        let owner = Arc::new(control(None, CompileControlError::Cancelled));
        let weak = Arc::downgrade(&owner);
        let mut memo = Memo::new();
        let root = opt_expr_to_memo(&wide(), &mut memo, owner.as_ref()).unwrap();
        assert_eq!(root, 320);
        assert_eq!(
            memo.groups[root].logical_exprs[0].children,
            (0..320).collect::<Vec<_>>()
        );
        for (id, group) in memo.groups.iter().enumerate() {
            assert_eq!(group.logical_exprs[0].id, id);
        }
        let cte = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: 17,
                output_columns: vec![],
            }),
            vec![leaf()],
        );
        let group = opt_expr_to_memo(&cte, &mut memo, owner.as_ref()).unwrap();
        assert_eq!(memo.cte_produce_groups[&17], group);
        assert_eq!(memo.groups[group].logical_exprs[0].id, 322);
        let units = owner.units.lock().unwrap().clone();
        assert_eq!(
            &units
                .iter()
                .copied()
                .filter(|u| *u > 0)
                .take(3)
                .collect::<Vec<_>>(),
            &[256, 256, 129]
        );
        drop(owner);
        assert!(weak.upgrade().is_none());
        assert_eq!(memo.groups.len(), 323);
    }
}
