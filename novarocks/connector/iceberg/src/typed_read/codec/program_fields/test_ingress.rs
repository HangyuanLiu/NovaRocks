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

//! Test source authors keep format facts separate from execution declarations.
use arrow::datatypes::{DataType, FieldRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::{collections::HashMap, sync::Arc};
pub(crate) fn source_field(
    field: &FieldRef,
    path: &mut Vec<u32>,
    facts: &mut Vec<novarocks_connector_contract::ConnectorSourceFieldFacts>,
) -> FieldRef {
    source_field_inner(field, path, facts, true)
}
pub(crate) fn system_source_field(
    field: &FieldRef,
    path: &mut Vec<u32>,
    facts: &mut Vec<novarocks_connector_contract::ConnectorSourceFieldFacts>,
) -> FieldRef {
    source_field_inner(field, path, facts, false)
}
fn source_field_inner(
    field: &FieldRef,
    path: &mut Vec<u32>,
    facts: &mut Vec<novarocks_connector_contract::ConnectorSourceFieldFacts>,
    format_ids: bool,
) -> FieldRef {
    use novarocks_connector_contract::{
        ConnectorError, ConnectorSourceFieldFacts, ConnectorSourceInitialDefault,
    };
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let storage = ValueTypeStorageLifetime::pure();
    let id = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .filter(|_| format_ids)
        .map(|id| id.parse().unwrap());
    let default = field
        .metadata()
        .get(crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY)
        .filter(|_| format_ids)
        .map(|json| ConnectorSourceInitialDefault::json(Arc::from(json.as_str()), storage.clone()));
    let entries = field
        .metadata()
        .iter()
        .filter(|(key, _)| {
            key.as_str() != PARQUET_FIELD_ID_META_KEY
                && key.as_str() != crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY
        })
        .collect::<Vec<_>>();
    // This fixture is the actual one-reserve creator. Its request geometry
    // and each String::clone backing are known before the map is handed off.
    let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
        String,
        String,
    >(entries.len())
    .unwrap();
    let mut attributes = HashMap::with_capacity(entries.len());
    let mut invoice = table.request_bytes_upper_bound;
    for (key, value) in entries {
        invoice += key.len() + value.len();
        attributes.insert(key.clone(), value.clone());
    }
    if id.is_some() || default.is_some() || !attributes.is_empty() {
        facts.push(
            ConnectorSourceFieldFacts::try_new_observed(
                path.clone().into_boxed_slice(),
                id,
                default,
                attributes,
                &[],
                storage,
                invoice,
                |_| Ok::<_, ConnectorError>(()),
                |_| Ok(()),
            )
            .unwrap(),
        );
    }
    let mut child = |field: &FieldRef, ordinal| {
        path.push(ordinal);
        let field = source_field_inner(field, path, facts, format_ids);
        path.pop();
        field
    };
    let ty = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| child(f, i as u32))
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::List(element) => DataType::List(child(element, 0)),
        DataType::LargeList(element) => DataType::LargeList(child(element, 0)),
        DataType::FixedSizeList(element, n) => DataType::FixedSizeList(child(element, 0), *n),
        DataType::Map(entries, sorted) => DataType::Map(child(entries, 0), *sorted),
        other => other.clone(),
    };
    Arc::new(arrow::datatypes::Field::new(
        field.name(),
        ty,
        field.is_nullable(),
    ))
}
