use crate::sql::parser::ast::{SqlType, TableColumnDef};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StarRocksPhysicalColumn {
    pub(crate) column: TableColumnDef,
    pub(crate) visible: bool,
    pub(crate) is_key: bool,
}

pub(crate) fn starrocks_physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> StarRocksPhysicalColumn {
    StarRocksPhysicalColumn {
        column: TableColumnDef {
            name,
            data_type,
            nullable,
            aggregation: None,
            default: None,
        },
        visible,
        is_key,
    }
}
