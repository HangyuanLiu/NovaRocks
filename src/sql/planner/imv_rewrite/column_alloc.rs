use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;

pub(crate) fn allocate_imv_column(
    ctx: &RewriteContext,
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> Result<ColumnId, String> {
    let factory = ctx
        .column_ref_factory()
        .ok_or_else(|| "IMV rewrite requires ColumnRefFactory in RewriteContext".to_string())?;
    Ok(factory
        .borrow_mut()
        .create(None, name.to_string(), data_type, nullable))
}

pub(crate) fn allocate_imv_output_column(
    ctx: &RewriteContext,
    name: &str,
    data_type: DataType,
    nullable: bool,
    is_internal: bool,
) -> Result<OutputColumn, String> {
    let column_id = allocate_imv_column(ctx, name, data_type.clone(), nullable)?;
    Ok(OutputColumn {
        column_id,
        name: name.to_string(),
        data_type,
        nullable,
        is_internal,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::column_id::ColumnRefFactory;

    #[test]
    fn allocate_imv_column_requires_factory_in_context() {
        let ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let err = allocate_imv_column(&ctx, "__imv_probe", DataType::Int64, false)
            .expect_err("missing factory should be a hard IMV rewrite error");

        assert_eq!(
            err,
            "IMV rewrite requires ColumnRefFactory in RewriteContext"
        );
    }

    #[test]
    fn allocate_imv_output_column_records_real_factory_metadata() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::clone(&factory));

        let output = allocate_imv_output_column(&ctx, "__imv_branch", DataType::Int32, false, true)
            .expect("factory-backed allocation should succeed");

        assert_eq!(output.column_id.0, 1);
        assert_eq!(output.name, "__imv_branch");
        assert_eq!(output.data_type, DataType::Int32);
        assert!(!output.nullable);
        assert!(output.is_internal);

        let meta = factory.borrow().get(output.column_id).clone();
        assert_eq!(meta.name, "__imv_branch");
        assert_eq!(meta.data_type, DataType::Int32);
        assert!(!meta.nullable);
        assert_eq!(factory.borrow().peek_next_id(), 2);
    }
}
