use std::collections::BTreeSet;

use crate::sql::codegen::FragmentId;
use crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec;
use crate::thrift::data_sinks;
use crate::thrift::partitions;

pub(crate) const CHANGE_OP_DELETE: i32 = -1;
pub(crate) const CHANGE_OP_INSERT: i32 = 1;
pub(crate) const DATA_ROUTE_REUSE: i32 = 1;
pub(crate) const DATA_ROUTE_FRESH: i32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ChangeStreamWriteBranchKind {
    DeleteDv,
    ReuseData,
    FreshData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ChangeStreamRouteKey {
    pub(crate) change_op: i32,
    pub(crate) data_route: Option<i32>,
}

impl ChangeStreamWriteBranchKind {
    pub(crate) fn route_key(self) -> ChangeStreamRouteKey {
        match self {
            Self::DeleteDv => ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            },
            Self::ReuseData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_REUSE),
            },
            Self::FreshData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_FRESH),
            },
        }
    }

    pub(crate) fn to_thrift(self) -> data_sinks::TIcebergChangeStreamRouterBranchKind {
        match self {
            Self::DeleteDv => data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
            Self::ReuseData => data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA,
            Self::FreshData => data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA,
        }
    }

    pub(crate) fn from_thrift(
        value: data_sinks::TIcebergChangeStreamRouterBranchKind,
    ) -> Result<Self, String> {
        match value {
            data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV => Ok(Self::DeleteDv),
            data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA => Ok(Self::ReuseData),
            data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA => Ok(Self::FreshData),
            _ => Err(format!(
                "unsupported Iceberg change-stream router branch kind {}",
                value.0
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteBranchSpec {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamWriteBranchKind,
    pub(crate) stream_output_slots: Vec<i32>,
    pub(crate) output_partition: partitions::TDataPartition,
    pub(crate) sink_spec: IcebergWriteSinkSpec,
    pub(crate) writer_fragment_id: Option<FragmentId>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamWriteDagSpec {
    pub(crate) change_op_slot: i32,
    pub(crate) data_route_slot: Option<i32>,
    pub(crate) branches: Vec<ChangeStreamWriteBranchSpec>,
}

pub(crate) fn validate_branch_set(branches: &[ChangeStreamWriteBranchSpec]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for branch in branches {
        if !seen.insert(branch.branch_kind) {
            return Err(format!(
                "duplicate change-stream branch kind {:?}",
                branch.branch_kind
            ));
        }
    }
    Ok(())
}

impl IcebergChangeStreamWriteDagSpec {
    pub(crate) fn validate(&mut self) -> Result<(), String> {
        validate_branch_set(&self.branches)?;
        if self.branches.iter().any(|b| {
            matches!(
                b.branch_kind,
                ChangeStreamWriteBranchKind::ReuseData | ChangeStreamWriteBranchKind::FreshData
            )
        }) && self.data_route_slot.is_none()
        {
            return Err("data_route_slot is required when data branches are declared".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
impl ChangeStreamWriteBranchSpec {
    pub(crate) fn for_test(branch_id: i32, branch_kind: ChangeStreamWriteBranchKind) -> Self {
        Self {
            branch_id,
            branch_kind,
            stream_output_slots: Vec::new(),
            output_partition: partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::thrift::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            sink_spec: crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec(),
            writer_fragment_id: None,
        }
    }

    pub(crate) fn delete_dv_for_test(stream_output_slots: Vec<i32>) -> Self {
        Self::for_test_with_slots(
            0,
            ChangeStreamWriteBranchKind::DeleteDv,
            stream_output_slots,
        )
    }

    pub(crate) fn reuse_data_for_test(stream_output_slots: Vec<i32>) -> Self {
        Self::for_test_with_slots(
            1,
            ChangeStreamWriteBranchKind::ReuseData,
            stream_output_slots,
        )
    }

    pub(crate) fn fresh_data_for_test(stream_output_slots: Vec<i32>) -> Self {
        Self::for_test_with_slots(
            2,
            ChangeStreamWriteBranchKind::FreshData,
            stream_output_slots,
        )
    }

    fn for_test_with_slots(
        branch_id: i32,
        branch_kind: ChangeStreamWriteBranchKind,
        stream_output_slots: Vec<i32>,
    ) -> Self {
        use arrow::datatypes::DataType;

        let mut branch = Self::for_test(branch_id, branch_kind);
        branch.stream_output_slots = stream_output_slots;
        branch.sink_spec.mode = match branch_kind {
            ChangeStreamWriteBranchKind::DeleteDv => {
                crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::DeletionVectors
            }
            ChangeStreamWriteBranchKind::ReuseData => {
                crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::RowLineageData
            }
            ChangeStreamWriteBranchKind::FreshData => {
                crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::Data
            }
        };
        branch.sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::codegen::iceberg_write_sink::test_support::unpartitioned_metadata_json(),
        );

        let target_columns = branch
            .stream_output_slots
            .iter()
            .enumerate()
            .map(|(idx, _)| crate::sql::catalog::ColumnDef {
                name: format!("c{}", idx + 1),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            })
            .collect::<Vec<_>>();
        if !target_columns.is_empty() {
            branch.sink_spec.target_columns = target_columns.clone();
            branch.sink_spec.target_table.columns = target_columns;
        }
        branch
    }
}

#[cfg(test)]
impl IcebergChangeStreamWriteDagSpec {
    pub(crate) fn for_test(
        change_op_slot: i32,
        data_route_slot: Option<i32>,
        branches: Vec<ChangeStreamWriteBranchSpec>,
    ) -> Self {
        Self {
            change_op_slot,
            data_route_slot,
            branches,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_key_for_branch_kind_is_typed_and_fixed() {
        assert_eq!(
            ChangeStreamWriteBranchKind::DeleteDv.route_key(),
            ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            }
        );
        assert_eq!(
            ChangeStreamWriteBranchKind::ReuseData.route_key(),
            ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_REUSE),
            }
        );
        assert_eq!(
            ChangeStreamWriteBranchKind::FreshData.route_key(),
            ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_FRESH),
            }
        );
    }

    #[test]
    fn from_thrift_accepts_known_branch_kinds() {
        assert_eq!(
            ChangeStreamWriteBranchKind::from_thrift(
                data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
            )
            .expect("DELETE_DV"),
            ChangeStreamWriteBranchKind::DeleteDv
        );
        assert_eq!(
            ChangeStreamWriteBranchKind::from_thrift(
                data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA,
            )
            .expect("REUSE_DATA"),
            ChangeStreamWriteBranchKind::ReuseData
        );
        assert_eq!(
            ChangeStreamWriteBranchKind::from_thrift(
                data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA,
            )
            .expect("FRESH_DATA"),
            ChangeStreamWriteBranchKind::FreshData
        );
    }

    #[test]
    fn from_thrift_rejects_unknown_branch_kind_without_panic() {
        let err = ChangeStreamWriteBranchKind::from_thrift(
            data_sinks::TIcebergChangeStreamRouterBranchKind(99),
        )
        .expect_err("unknown branch kind");
        assert!(err.contains("unsupported Iceberg change-stream router branch kind 99"));
    }

    #[test]
    fn validate_rejects_duplicate_branch_kind() {
        let branches = vec![
            ChangeStreamWriteBranchSpec::for_test(0, ChangeStreamWriteBranchKind::DeleteDv),
            ChangeStreamWriteBranchSpec::for_test(1, ChangeStreamWriteBranchKind::DeleteDv),
        ];
        let err = validate_branch_set(&branches).expect_err("duplicate branch kind");
        assert!(err.contains("duplicate change-stream branch kind DeleteDv"));
    }

    #[test]
    fn validate_requires_data_route_when_data_branch_exists() {
        let mut spec = IcebergChangeStreamWriteDagSpec::for_test(
            10,
            None,
            vec![ChangeStreamWriteBranchSpec::for_test(
                0,
                ChangeStreamWriteBranchKind::ReuseData,
            )],
        );
        let err = spec.validate().expect_err("missing data_route");
        assert!(err.contains("data_route_slot is required when data branches are declared"));
    }

    #[test]
    fn validate_allows_delete_only_without_data_route() {
        let mut spec = IcebergChangeStreamWriteDagSpec::for_test(
            10,
            None,
            vec![ChangeStreamWriteBranchSpec::for_test(
                0,
                ChangeStreamWriteBranchKind::DeleteDv,
            )],
        );
        spec.validate()
            .expect("delete-only does not require data route");
    }
}
