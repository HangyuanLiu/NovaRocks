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

use arrow::datatypes::DataType;

use crate::thrift::{descriptors, exprs, types};

pub(crate) struct DescriptorTableBuilder {
    slots: Vec<descriptors::TSlotDescriptor>,
    tuples: Vec<descriptors::TTupleDescriptor>,
    tables: Vec<descriptors::TTableDescriptor>,
}

impl DescriptorTableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            slots: Vec::new(),
            tuples: Vec::new(),
            tables: Vec::new(),
        }
    }

    pub(crate) fn add_slot(
        &mut self,
        slot_id: i32,
        tuple_id: i32,
        name: &str,
        data_type: &DataType,
        nullable: bool,
        col_pos: i32,
    ) {
        let primitive = match data_type {
            DataType::Int8 => types::TPrimitiveType::TINYINT,
            DataType::Int16 => types::TPrimitiveType::SMALLINT,
            DataType::Int32 => types::TPrimitiveType::INT,
            DataType::Int64 => types::TPrimitiveType::BIGINT,
            DataType::Utf8 => types::TPrimitiveType::VARCHAR,
            other => panic!("unsupported test descriptor type {other:?}"),
        };
        self.slots.push(descriptors::TSlotDescriptor::new(
            Some(slot_id),
            Some(tuple_id),
            Some(crate::lower::compat::type_lowering::scalar_type_desc(
                primitive,
            )),
            Some(col_pos),
            Some(0),
            Some(0),
            Some(0),
            Some(name.to_string()),
            Some(col_pos),
            Some(true),
            Some(true),
            Some(nullable),
            None::<i32>,
            None::<String>,
            None::<bool>,
        ));
    }

    pub(crate) fn add_tuple(&mut self, tuple_id: i32, table_id: Option<i64>) {
        self.tuples.push(descriptors::TTupleDescriptor::new(
            Some(tuple_id),
            Some(0),
            Some(0),
            table_id,
            Some(0),
        ));
    }

    pub(crate) fn add_table(&mut self, table_id: i64, db: &str, table: &str, cols: i32) {
        self.tables.push(descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::OLAP_TABLE,
            cols,
            0,
            table.to_string(),
            db.to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            None::<descriptors::TIcebergTable>,
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        ));
    }

    pub(crate) fn build(self) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            Some(self.slots),
            self.tuples,
            (!self.tables.is_empty()).then_some(self.tables),
            None::<bool>,
        )
    }
}

pub(crate) fn build_slot_ref_texpr(
    slot_id: i32,
    tuple_id: i32,
    type_desc: types::TTypeDesc,
) -> exprs::TExpr {
    exprs::TExpr::new(vec![exprs::TExprNode {
        node_type: exprs::TExprNodeType::SLOT_REF,
        type_: type_desc,
        opcode: None,
        num_children: 0,
        agg_expr: None,
        bool_literal: None,
        case_expr: None,
        date_literal: None,
        float_literal: None,
        int_literal: None,
        in_predicate: None,
        is_null_pred: None,
        like_pred: None,
        literal_pred: None,
        slot_ref: Some(exprs::TSlotRef { slot_id, tuple_id }),
        string_literal: None,
        tuple_is_null_pred: None,
        info_func: None,
        decimal_literal: None,
        output_scale: 0,
        fn_call_expr: None,
        large_int_literal: None,
        output_column: None,
        output_type: None,
        vector_opcode: None,
        fn_: None,
        vararg_start_idx: None,
        child_type: None,
        vslot_ref: None,
        used_subfield_names: None,
        binary_literal: None,
        copy_flag: None,
        check_is_out_of_bounds: None,
        use_vectorized: None,
        has_nullable_child: None,
        is_nullable: None,
        child_type_desc: None,
        is_monotonic: None,
        dict_query_expr: None,
        dictionary_get_expr: None,
        is_index_only_filter: None,
        is_nondeterministic: None,
    }])
}
