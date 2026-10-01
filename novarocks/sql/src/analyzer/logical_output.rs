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

//! Read-only SQL projections of the expression's authored value domain.

use arrow::datatypes::DataType;
use novarocks_parser::ast;
use novarocks_types::schema::SqlType;

use super::{AnalyzerContext, scope::AnalyzerScope};
use crate::analysis::TypedExpr;

impl AnalyzerContext<'_> {
    pub(super) fn logical_output_type(
        &self,
        _source: Option<&ast::Expr>,
        expression: &TypedExpr,
        _scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        super::helpers::sql_logical_projection(expression.value_type.logical_type)
    }

    pub(super) fn json_list_provenance(
        &self,
        _source: Option<&ast::Expr>,
        expression: &TypedExpr,
        _scope: &AnalyzerScope,
    ) -> bool {
        match &expression.value_type.data_type {
            DataType::List(item) => {
                novarocks_type_contract::field_logical_type(item)
                    == Ok(novarocks_type_contract::ValueLogicalType::Json)
            }
            _ => false,
        }
    }
}
