use crate::connector::iceberg::changes::{ChangeError, IcebergChangeBatch, plan_changes};

pub(crate) fn validate_change_batch_current_snapshot(
    batch: &IcebergChangeBatch,
    expected_current_snapshot_id: i64,
) -> Result<(), String> {
    if batch.current_snapshot_id != expected_current_snapshot_id {
        return Err(format!(
            "iceberg change batch current snapshot mismatch: expected {expected_current_snapshot_id}, got {}",
            batch.current_snapshot_id
        ));
    }
    Ok(())
}

pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let batch = plan_changes(base_table, previous_snapshot_id, pk_columns)?;
    validate_change_batch_current_snapshot(&batch, expected_current_snapshot_id)
        .map_err(ChangeError::InternalInconsistency)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use crate::connector::iceberg::changes::IcebergChangeBatch;

    use super::validate_change_batch_current_snapshot;

    #[test]
    fn validate_change_batch_current_snapshot_rejects_mismatch() {
        let batch = IcebergChangeBatch {
            previous_snapshot_id: 10,
            current_snapshot_id: 12,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
            deleted_data_files: Vec::new(),
        };

        let err = validate_change_batch_current_snapshot(&batch, 13).expect_err("mismatch");

        assert_eq!(
            err,
            "iceberg change batch current snapshot mismatch: expected 13, got 12"
        );
    }
}
