use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum JoinRefreshMode {
    Full,
    AppendOnly,
    Coalesce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinRefreshBranchSide {
    LeftDeltaRightSnapshot,
    LeftSnapshotRightDelta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshBranchDescriptor {
    pub side: JoinRefreshBranchSide,
    pub action_column_id: ColumnId,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinRefreshDescriptor {
    pub mode: JoinRefreshMode,
    pub left_base_fqn: String,
    pub right_base_fqn: String,
    pub left_row_id_column: OutputColumn,
    pub right_row_id_column: OutputColumn,
    pub action_column: OutputColumn,
    pub join_apply_key_column: OutputColumn,
    pub payload_columns: Vec<OutputColumn>,
    pub branches: Vec<JoinRefreshBranchDescriptor>,
    pub needs_target_locator: bool,
}

impl PartialEq for JoinRefreshDescriptor {
    fn eq(&self, other: &Self) -> bool {
        self.mode == other.mode
            && self.left_base_fqn == other.left_base_fqn
            && self.right_base_fqn == other.right_base_fqn
            && output_column_eq(&self.left_row_id_column, &other.left_row_id_column)
            && output_column_eq(&self.right_row_id_column, &other.right_row_id_column)
            && output_column_eq(&self.action_column, &other.action_column)
            && output_column_eq(&self.join_apply_key_column, &other.join_apply_key_column)
            && output_columns_eq(&self.payload_columns, &other.payload_columns)
            && self.branches == other.branches
            && self.needs_target_locator == other.needs_target_locator
    }
}

impl Eq for JoinRefreshDescriptor {}

impl JoinRefreshDescriptor {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self
            .left_base_fqn
            .eq_ignore_ascii_case(&self.right_base_fqn)
        {
            return Err(
                "join refresh descriptor requires distinct left and right bases".to_string(),
            );
        }
        if self.payload_columns.is_empty() {
            return Err("join refresh descriptor requires at least one payload column".to_string());
        }
        if !self
            .action_column
            .name
            .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
            || self.action_column.data_type != DataType::Int8
            || self.action_column.nullable
            || !self.action_column.is_internal
        {
            return Err("join refresh descriptor has invalid action column".to_string());
        }
        if !self.join_apply_key_column.name.eq_ignore_ascii_case(
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        ) || self.join_apply_key_column.data_type != DataType::Utf8
            || self.join_apply_key_column.nullable
            || !self.join_apply_key_column.is_internal
        {
            return Err("join refresh descriptor has invalid join apply-key column".to_string());
        }
        if matches!(self.mode, JoinRefreshMode::Coalesce) && !self.needs_target_locator {
            return Err("coalescing join refresh requires target locator".to_string());
        }
        self.validate_branches()?;
        Ok(())
    }

    fn validate_branches(&self) -> Result<(), String> {
        if self.branches.is_empty() {
            return Err("join refresh descriptor requires at least one branch".to_string());
        }

        for branch in &self.branches {
            if branch.action_column_id != self.action_column.column_id {
                return Err(format!(
                    "join refresh descriptor branch action column id {} does not match action column id {}",
                    branch.action_column_id, self.action_column.column_id
                ));
            }
        }

        if matches!(self.mode, JoinRefreshMode::Coalesce) {
            let left_delta_count = self
                .branches
                .iter()
                .filter(|branch| branch.side == JoinRefreshBranchSide::LeftDeltaRightSnapshot)
                .count();
            if left_delta_count != 1 {
                return Err(format!(
                    "coalescing join refresh requires exactly one LeftDeltaRightSnapshot branch, found {left_delta_count}"
                ));
            }

            let right_delta_count = self
                .branches
                .iter()
                .filter(|branch| branch.side == JoinRefreshBranchSide::LeftSnapshotRightDelta)
                .count();
            if right_delta_count != 1 {
                return Err(format!(
                    "coalescing join refresh requires exactly one LeftSnapshotRightDelta branch, found {right_delta_count}"
                ));
            }
        }

        Ok(())
    }
}

fn output_columns_eq(left: &[OutputColumn], right: &[OutputColumn]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| output_column_eq(left, right))
}

fn output_column_eq(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.data_type == right.data_type
        && left.nullable == right.nullable
        && left.is_internal == right.is_internal
}

#[cfg(test)]
mod tests {
    use super::*;

    fn out(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            name: name.to_string(),
            data_type,
            nullable,
            column_id: ColumnId(id),
            is_internal,
        }
    }

    fn valid_descriptor() -> JoinRefreshDescriptor {
        JoinRefreshDescriptor {
            mode: JoinRefreshMode::Coalesce,
            left_base_fqn: "ice.db.left_t".to_string(),
            right_base_fqn: "ice.db.right_t".to_string(),
            left_row_id_column: out(1, "_row_id", DataType::Int64, false, true),
            right_row_id_column: out(2, "_row_id", DataType::Int64, false, true),
            action_column: out(
                3,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
            join_apply_key_column: out(
                4,
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
                DataType::Utf8,
                false,
                true,
            ),
            payload_columns: vec![out(5, "k", DataType::Int64, false, false)],
            branches: vec![
                JoinRefreshBranchDescriptor {
                    side: JoinRefreshBranchSide::LeftDeltaRightSnapshot,
                    action_column_id: ColumnId(3),
                },
                JoinRefreshBranchDescriptor {
                    side: JoinRefreshBranchSide::LeftSnapshotRightDelta,
                    action_column_id: ColumnId(3),
                },
            ],
            needs_target_locator: true,
        }
    }

    #[test]
    fn validates_coalescing_join_refresh_descriptor() {
        valid_descriptor()
            .validate()
            .expect("descriptor should validate");
    }

    #[test]
    fn rejects_coalescing_descriptor_without_locator() {
        let mut desc = valid_descriptor();
        desc.needs_target_locator = false;
        let err = desc.validate().expect_err("coalesce requires locator");
        assert!(err.contains("requires target locator"));
    }

    #[test]
    fn rejects_descriptor_with_same_base_on_both_sides() {
        let mut desc = valid_descriptor();
        desc.right_base_fqn = "ICE.DB.LEFT_T".to_string();
        assert_invalid(desc, "requires distinct left and right bases");
    }

    #[test]
    fn rejects_descriptor_without_payload_columns() {
        let mut desc = valid_descriptor();
        desc.payload_columns.clear();
        assert_invalid(desc, "requires at least one payload column");
    }

    #[test]
    fn rejects_descriptor_with_invalid_action_column() {
        let mut desc = valid_descriptor();
        desc.action_column.data_type = DataType::Int64;
        assert_invalid(desc, "invalid action column");
    }

    #[test]
    fn rejects_descriptor_with_invalid_join_apply_key_column() {
        let mut desc = valid_descriptor();
        desc.join_apply_key_column.nullable = true;
        assert_invalid(desc, "invalid join apply-key column");
    }

    #[test]
    fn rejects_descriptor_without_branches() {
        let mut desc = valid_descriptor();
        desc.branches.clear();
        assert_invalid(desc, "requires at least one branch");
    }

    #[test]
    fn rejects_descriptor_with_branch_action_column_mismatch() {
        let mut desc = valid_descriptor();
        desc.branches[0].action_column_id = ColumnId(99);
        assert_invalid(desc, "does not match action column id");
    }

    #[test]
    fn rejects_coalescing_descriptor_missing_left_delta_branch() {
        let mut desc = valid_descriptor();
        desc.branches.remove(0);
        assert_invalid(desc, "exactly one LeftDeltaRightSnapshot branch");
    }

    #[test]
    fn rejects_coalescing_descriptor_missing_right_delta_branch() {
        let mut desc = valid_descriptor();
        desc.branches.pop();
        assert_invalid(desc, "exactly one LeftSnapshotRightDelta branch");
    }

    #[test]
    fn rejects_coalescing_descriptor_duplicate_delta_side() {
        let mut desc = valid_descriptor();
        desc.branches[1].side = JoinRefreshBranchSide::LeftDeltaRightSnapshot;
        assert_invalid(desc, "exactly one LeftDeltaRightSnapshot branch");
    }

    fn assert_invalid(desc: JoinRefreshDescriptor, expected: &str) {
        let err = desc.validate().expect_err("descriptor should be invalid");
        assert!(
            err.contains(expected),
            "expected error to contain {expected:?}, got {err:?}"
        );
    }
}
