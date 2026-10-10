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

//! The provider-authored public schema of one frozen read.

use std::sync::Arc;

use arrow::datatypes::Schema;
use novarocks_type_contract::ValueLogicalType;

use crate::connector::{ConnectorError, ConnectorErrorKind};

/// The exact public fields a frozen read produces: one field per assignment
/// column, in assignment order, each paired with its value logical type.
///
/// Only the provider that owns the read authors this. A field's name, NULL
/// contract, nested domain and source metadata (field IDs, initial defaults)
/// are that provider's external facts, and its pure read compiler checks a
/// frozen read's public schema against the same field author. No role derives
/// one from SQL names or engine types, and nothing downstream may re-author
/// it: the value is carried unchanged into the frozen read's public facts.
///
/// A read projecting no column has the empty schema. Whether such a read can
/// be frozen into a program is decided by the program's own laws, not here.
#[derive(Clone, Debug)]
pub struct ConnectorReadPublicSchema {
    schema: Arc<Schema>,
    logical_types: Vec<ValueLogicalType>,
}

impl ConnectorReadPublicSchema {
    pub fn try_new(
        schema: Arc<Schema>,
        logical_types: Vec<ValueLogicalType>,
    ) -> Result<Self, ConnectorError> {
        if schema.fields().len() != logical_types.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "public read schema has {} fields but {} logical types",
                    schema.fields().len(),
                    logical_types.len()
                ),
            ));
        }
        Ok(Self {
            schema,
            logical_types,
        })
    }

    pub const fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn logical_types(&self) -> &[ValueLogicalType] {
        &self.logical_types
    }

    pub fn into_parts(self) -> (Arc<Schema>, Vec<ValueLogicalType>) {
        (self.schema, self.logical_types)
    }
}

/// Field metadata is part of the fact: two schemas that differ only in a field
/// ID are different reads, which Arrow's own `Field` equality would not say for
/// every attribute.
impl PartialEq for ConnectorReadPublicSchema {
    fn eq(&self, other: &Self) -> bool {
        self.logical_types == other.logical_types
            && novarocks_type_contract::arrow_schemas_exact(&self.schema, &other.schema)
    }
}

impl Eq for ConnectorReadPublicSchema {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn field(id: &str) -> Field {
        Field::new("id", DataType::Int64, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), id.into())]))
    }

    #[test]
    fn every_field_has_exactly_one_logical_type() {
        let schema = Arc::new(Schema::new(vec![field("1")]));
        let error = ConnectorReadPublicSchema::try_new(Arc::clone(&schema), vec![])
            .expect_err("a field without a logical type is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let error = ConnectorReadPublicSchema::try_new(
            Arc::clone(&schema),
            vec![ValueLogicalType::Physical; 2],
        )
        .expect_err("a logical type without a field is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);

        let empty = ConnectorReadPublicSchema::try_new(Arc::new(Schema::empty()), vec![])
            .expect("a read projecting no column has the empty schema");
        assert!(empty.schema().fields().is_empty());
    }

    #[test]
    fn field_metadata_is_part_of_the_published_fact() {
        let one = ConnectorReadPublicSchema::try_new(
            Arc::new(Schema::new(vec![field("1")])),
            vec![ValueLogicalType::Physical],
        )
        .expect("schema");
        let same = ConnectorReadPublicSchema::try_new(
            Arc::new(Schema::new(vec![field("1")])),
            vec![ValueLogicalType::Physical],
        )
        .expect("schema");
        let other_id = ConnectorReadPublicSchema::try_new(
            Arc::new(Schema::new(vec![field("2")])),
            vec![ValueLogicalType::Physical],
        )
        .expect("schema");
        assert_eq!(one, same);
        assert_ne!(one, other_id);

        let (schema, logical_types) = one.into_parts();
        assert_eq!(schema.field(0).metadata()["PARQUET:field_id"], "1");
        assert_eq!(logical_types, vec![ValueLogicalType::Physical]);
    }
}
