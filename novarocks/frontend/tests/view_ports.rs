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

use std::collections::HashMap;

use novarocks_frontend::common::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};
use novarocks_frontend::view::{
    CreateExternalViewRequest, ResolvedExternalView, ViewColumnDefinition, ViewEngine,
    ViewRequestContext, ViewService, ViewStatementResult, ViewTarget,
};
use novarocks_parser::{
    Span,
    ast::{Ident, ObjectName, TypeName},
};

fn native_bigint_type() -> TypeName {
    let span = Span::new(0, 0);
    TypeName {
        name: ObjectName {
            parts: vec![Ident {
                value: "BIGINT".to_string(),
                quoted: false,
                quote_style: None,
                span,
            }],
            span,
        },
        arguments: Vec::new(),
        argument_separator_spaces: Vec::new(),
        span,
    }
}

#[test]
fn frontend_facing_view_ports_and_dtos_are_publicly_nameable() {
    let target = ViewTarget {
        catalog: "rest".to_string(),
        database: "analytics".to_string(),
        view: "daily_sales".to_string(),
    };
    let request = CreateExternalViewRequest {
        target: target.clone(),
        columns: vec![ViewColumnDefinition {
            name: "sale_count".to_string(),
            data_type: native_bigint_type(),
            nullable: false,
        }],
        definition: PersistedQueryDefinition::new(
            "SELECT COUNT(*) AS sale_count FROM sales",
            PersistedQueryDialect::StarRocks,
            "rest",
            "analytics",
        )
        .unwrap(),
        comment: Some("Daily sales".to_string()),
        or_replace: false,
        if_not_exists: false,
        properties: vec![],
    };
    let resolved = ResolvedExternalView {
        definition: request.definition.clone(),
        column_names: vec!["sale_count".to_string()],
        comment: request.comment.clone(),
        properties: HashMap::new(),
    };
    let context = ViewRequestContext {
        current_catalog: Some("rest"),
        current_database: "analytics",
        connector_context: None,
    };

    fn ports_are_object_safe(_service: &dyn ViewService, _engine: &dyn ViewEngine) {}
    let _ = ports_are_object_safe;
    let _ = (request, resolved, context, ViewStatementResult::Ok);
}
