// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Explicit provider property declarations, frozen once into source field IDs.
//! This owner never reads Arrow annotations or derives a domain from bytes.

use crate::iceberg::spec::{PrimitiveType, Schema, TableMetadata, Type};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::ValueLogicalType;
use std::collections::{BTreeMap, HashMap, HashSet};

const PREFIX: &str = "novarocks.logical_type.";
const MAX_FIELDS: usize = 16384;
const MAX_DECLARATION_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NominalSourceDomain {
    Hll,
    Bitmap,
    LargeInt,
}
pub(crate) type NominalSourceDeclarations = BTreeMap<i32, NominalSourceDomain>;

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
impl NominalSourceDomain {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Hll => "hll",
            Self::Bitmap => "bitmap",
            Self::LargeInt => "largeint",
        }
    }
    pub(crate) fn parse(name: &str) -> Result<Self, ConnectorError> {
        match name {
            "hll" => Ok(Self::Hll),
            "bitmap" => Ok(Self::Bitmap),
            "largeint" => Ok(Self::LargeInt),
            _ => Err(corrupt("Iceberg frozen nominal declaration is invalid")),
        }
    }
    pub(crate) fn logical_type(self) -> ValueLogicalType {
        match self {
            Self::Hll => ValueLogicalType::Hll,
            Self::Bitmap => ValueLogicalType::Bitmap,
            Self::LargeInt => ValueLogicalType::LargeInt,
        }
    }
    pub(crate) fn validate_storage(self, ty: &Type) -> Result<(), ConnectorError> {
        let valid = match self {
            Self::Hll | Self::Bitmap => matches!(ty, Type::Primitive(PrimitiveType::Binary)),
            Self::LargeInt => matches!(ty, Type::Primitive(PrimitiveType::Fixed(16))),
        };
        if valid {
            Ok(())
        } else {
            Err(corrupt(
                "Iceberg nominal declaration conflicts with its authoritative storage type",
            ))
        }
    }
}

/// The exact LoadedTable metadata properties author the declaration. Names
/// resolve against that same current schema, and only stable IDs are retained.
pub(crate) fn from_metadata(
    metadata: &TableMetadata,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    let declarations = from_properties(metadata.current_schema(), metadata.properties())?;
    for schema in metadata.schemas_iter() {
        validate_schema(schema, &declarations)?;
    }
    Ok(declarations)
}

fn from_properties(
    schema: &Schema,
    properties: &HashMap<String, String>,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    let mut declarations = BTreeMap::new();
    let mut names = HashSet::new();
    let mut bytes = 0usize;
    for (key, value) in properties {
        let Some(name) = key.strip_prefix(PREFIX) else {
            continue;
        };
        bytes = bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or_else(|| corrupt("Iceberg nominal declaration extent exceeds address space"))?;
        if bytes > MAX_DECLARATION_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg nominal declarations exceed the hard limit",
            ));
        }
        let value = value.to_ascii_lowercase();
        if !matches!(value.as_str(), "hll" | "bitmap" | "largeint") {
            // Other existing property authors (integer/date/decimal) retain
            // their own exact contracts; this leaf grants no such domain.
            continue;
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(corrupt(
                "Iceberg nominal source declaration has ambiguous names",
            ));
        }
        let mut candidates = schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.name.eq_ignore_ascii_case(name));
        let field = candidates
            .next()
            .ok_or_else(|| corrupt("Iceberg nominal declaration names no current source field"))?;
        if candidates.next().is_some() {
            return Err(corrupt(
                "Iceberg nominal source declaration has ambiguous names",
            ));
        }
        let domain = NominalSourceDomain::parse(&value)?;
        domain.validate_storage(field.field_type.as_ref())?;
        if declarations.insert(field.id, domain).is_some() || declarations.len() > MAX_FIELDS {
            return Err(corrupt(
                "Iceberg nominal source declarations have invalid field IDs or count",
            ));
        }
    }
    Ok(declarations)
}

/// A historical schema may omit a currently declared field. Every occurrence
/// with that stable ID must still satisfy the exact declared storage contract.
pub(crate) fn validate_schema(
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<(), ConnectorError> {
    if declarations.len() > MAX_FIELDS || declarations.keys().any(|id| *id <= 0) {
        return Err(corrupt(
            "Iceberg nominal source declarations have invalid field IDs or count",
        ));
    }
    for field in schema.as_struct().fields() {
        if let Some(domain) = declarations.get(&field.id) {
            domain.validate_storage(field.field_type.as_ref())?;
        }
    }
    Ok(())
}

pub(crate) fn restrict_to_schema(
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    validate_schema(schema, declarations)?;
    Ok(schema
        .as_struct()
        .fields()
        .iter()
        .filter_map(|field| {
            declarations
                .get(&field.id)
                .copied()
                .map(|domain| (field.id, domain))
        })
        .collect())
}

pub(crate) fn validate_frozen_schema(
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<(), ConnectorError> {
    validate_schema(schema, declarations)?;
    for id in declarations.keys() {
        if !schema
            .as_struct()
            .fields()
            .iter()
            .any(|field| field.id == *id)
        {
            return Err(corrupt(
                "Iceberg frozen nominal declaration names no root source field",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::NestedField;
    use std::sync::Arc;
    #[test]
    fn declarations_freeze_exact_ids_and_do_not_promote_equal_width_storage() {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    7,
                    "h",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "n",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
            ])
            .build()
            .unwrap();
        assert!(
            from_properties(&schema, &HashMap::new())
                .unwrap()
                .is_empty()
        );
        let properties = HashMap::from([
            ("novarocks.logical_type.h".into(), "hll".into()),
            ("novarocks.logical_type.n".into(), "largeint".into()),
        ]);
        assert_eq!(
            from_properties(&schema, &properties).unwrap(),
            BTreeMap::from([
                (7, NominalSourceDomain::Hll),
                (8, NominalSourceDomain::LargeInt)
            ])
        );
        for invalid in [
            HashMap::from([("novarocks.logical_type.h".into(), "largeint".into())]),
            HashMap::from([("novarocks.logical_type.absent".into(), "bitmap".into())]),
            HashMap::from([
                ("novarocks.logical_type.h".into(), "hll".into()),
                ("novarocks.logical_type.H".into(), "bitmap".into()),
            ]),
        ] {
            assert!(from_properties(&schema, &invalid).is_err());
        }
    }
}
