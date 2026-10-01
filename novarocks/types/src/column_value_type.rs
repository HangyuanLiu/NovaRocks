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

//! Value domains authored by the catalog's explicit column declaration.

use crate::schema::{ColumnDef, SqlType};
use arrow_schema::{DataType, Field, Fields};
use novarocks_type_contract::{
    FunctionValueType, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, NR_LOGICAL_TYPE_KEY,
    ValueLogicalType, ValueTypeError, field_logical_type, validate_nested_logical_types_observed,
};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnValueTypeError {
    Type(ValueTypeError),
    DeclarationMismatch,
    UnrepresentableRootDomain(ValueLogicalType),
    UnrepresentableCarrier(DataType),
}
impl From<ValueTypeError> for ColumnValueTypeError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl fmt::Display for ColumnValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Type(error) => error.fmt(f),
            Self::DeclarationMismatch => {
                f.write_str("column logical declaration conflicts with its Arrow field")
            }
            Self::UnrepresentableCarrier(carrier) => {
                write!(f, "column schema cannot represent the {carrier:?} carrier")
            }
            Self::UnrepresentableRootDomain(domain) => write!(
                f,
                "column schema cannot represent the {domain:?} root domain"
            ),
        }
    }
}
impl std::error::Error for ColumnValueTypeError {}

/// Project an authored value domain into a SQL column declaration. The full
/// Arrow layout remains in the source value type; this never infers a logical
/// identity from a binary or text carrier.
pub fn sql_type_from_value_type(
    value: &FunctionValueType,
) -> Result<SqlType, ColumnValueTypeError> {
    value.validate()?;
    sql_declaration(&value.data_type, value.logical_type)
}
fn sql_declaration(
    data_type: &DataType,
    domain: ValueLogicalType,
) -> Result<SqlType, ColumnValueTypeError> {
    Ok(match domain {
        ValueLogicalType::Json => SqlType::Json,
        ValueLogicalType::Variant => SqlType::Variant,
        ValueLogicalType::Hll => SqlType::Hll,
        ValueLogicalType::Bitmap => SqlType::Bitmap,
        ValueLogicalType::LargeInt => SqlType::LargeInt,
        ValueLogicalType::Physical => match data_type {
            DataType::Boolean => SqlType::Boolean,
            DataType::Int8 => SqlType::TinyInt,
            DataType::Int16 => SqlType::SmallInt,
            DataType::Int32 => SqlType::Int,
            DataType::Int64 => SqlType::BigInt,
            DataType::Float32 => SqlType::Float,
            DataType::Float64 => SqlType::Double,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => SqlType::String,
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => SqlType::Binary,
            DataType::Date32 => SqlType::Date,
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
            DataType::Timestamp(_, _) => SqlType::DateTime,
            DataType::Time64(_) => SqlType::Time,
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                SqlType::Decimal {
                    precision: *precision,
                    scale: *scale,
                }
            }
            DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
                SqlType::Array(Box::new(sql_field_declaration(item)?))
            }
            DataType::Struct(fields) => SqlType::Struct(
                fields
                    .iter()
                    .map(|field| Ok((field.name().clone(), sql_field_declaration(field)?)))
                    .collect::<Result<_, ColumnValueTypeError>>()?,
            ),
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(ColumnValueTypeError::DeclarationMismatch);
                };
                if fields.len() != 2 {
                    return Err(ColumnValueTypeError::DeclarationMismatch);
                }
                SqlType::Map(
                    Box::new(sql_field_declaration(&fields[0])?),
                    Box::new(sql_field_declaration(&fields[1])?),
                )
            }
            data_type => {
                return Err(ColumnValueTypeError::UnrepresentableCarrier(
                    data_type.clone(),
                ));
            }
        },
        domain => return Err(ColumnValueTypeError::UnrepresentableRootDomain(domain)),
    })
}
fn sql_field_declaration(field: &Field) -> Result<SqlType, ColumnValueTypeError> {
    sql_declaration(field.data_type(), field_logical_type(field)?)
}

fn root(declaration: &SqlType) -> ValueLogicalType {
    match declaration {
        SqlType::Json => ValueLogicalType::Json,
        SqlType::Variant => ValueLogicalType::Variant,
        SqlType::Hll => ValueLogicalType::Hll,
        SqlType::Bitmap => ValueLogicalType::Bitmap,
        SqlType::LargeInt => ValueLogicalType::LargeInt,
        _ => ValueLogicalType::Physical,
    }
}

impl ColumnDef {
    /// Freeze an already-authored value domain into the catalog schema model.
    /// Unsupported schema domains fail instead of becoming a plain carrier.
    pub fn from_value_type(
        name: String,
        value_type: FunctionValueType,
        write_default: Option<crate::schema::ColumnDefault>,
    ) -> Result<Self, ColumnValueTypeError> {
        value_type.validate()?;
        let logical_type = match value_type.logical_type {
            ValueLogicalType::Physical => None,
            ValueLogicalType::Json => Some(SqlType::Json),
            ValueLogicalType::Variant => Some(SqlType::Variant),
            ValueLogicalType::Hll => Some(SqlType::Hll),
            ValueLogicalType::Bitmap => Some(SqlType::Bitmap),
            ValueLogicalType::LargeInt => Some(SqlType::LargeInt),
            domain => return Err(ColumnValueTypeError::UnrepresentableRootDomain(domain)),
        };
        Ok(Self {
            name,
            data_type: value_type.data_type,
            nullable: value_type.nullable,
            logical_type,
            write_default,
        })
    }

    pub fn declared_value_type(&self) -> Result<FunctionValueType, ColumnValueTypeError> {
        self.declared_value_type_observed(|| Ok(()))
    }

    /// The original caller observes each actual source node and field. An
    /// absent declaration keeps the Arrow domain, never a guessed carrier tag.
    pub fn declared_value_type_observed<E: From<ColumnValueTypeError> + From<ValueTypeError>>(
        &self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<FunctionValueType, E> {
        validate_nested_logical_types_observed(&self.data_type, &mut observe)?;
        // Bound the declaration itself before recursive copying. It can be
        // larger than the Arrow source when the two shapes disagree.
        let mut pending = Vec::new();
        if let Some(declaration) = &self.logical_type {
            pending.push((declaration, 1usize));
        }
        let mut count = 0usize;
        while let Some((declaration, depth)) = pending.pop() {
            observe()?;
            count += 1;
            if depth > MAX_VALUE_TYPE_DEPTH || count > MAX_VALUE_TYPE_NODES {
                return Err(E::from(ColumnValueTypeError::Type(
                    if depth > MAX_VALUE_TYPE_DEPTH {
                        ValueTypeError::TooDeep
                    } else {
                        ValueTypeError::TooManyNodes
                    },
                )));
            }
            match declaration {
                SqlType::Array(item) => pending.push((item, depth + 1)),
                SqlType::Map(key, value) => {
                    pending.push((value, depth + 1));
                    pending.push((key, depth + 1));
                }
                SqlType::Struct(fields) => {
                    for (_, item) in fields.iter().rev() {
                        observe()?;
                        pending.push((item, depth + 1));
                    }
                }
                _ => {}
            }
        }
        let value = match &self.logical_type {
            None => FunctionValueType::new(self.data_type.clone(), self.nullable),
            Some(declaration) => FunctionValueType {
                data_type: project(&self.data_type, declaration, &mut observe)?,
                nullable: self.nullable,
                logical_type: root(declaration),
            },
        };
        observe()?;
        value
            .logical_type
            .validate_carrier(&value.data_type)
            .map_err(ColumnValueTypeError::from)
            .map_err(E::from)?;
        validate_nested_logical_types_observed(&value.data_type, &mut observe)?;
        observe()?;
        Ok(value)
    }
}

fn project<E: From<ColumnValueTypeError> + From<ValueTypeError>>(
    source: &DataType,
    declaration: &SqlType,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<DataType, E> {
    observe()?;
    root(declaration)
        .validate_carrier(source)
        .map_err(ColumnValueTypeError::from)
        .map_err(E::from)?;
    let mismatch = || E::from(ColumnValueTypeError::DeclarationMismatch);
    Ok(match (source, declaration) {
        (DataType::List(item), SqlType::Array(declaration)) => {
            DataType::List(project_field(item, declaration, observe)?)
        }
        (DataType::LargeList(item), SqlType::Array(declaration)) => {
            DataType::LargeList(project_field(item, declaration, observe)?)
        }
        (DataType::FixedSizeList(item, size), SqlType::Array(declaration)) => {
            DataType::FixedSizeList(project_field(item, declaration, observe)?, *size)
        }
        (DataType::Map(entries, sorted), SqlType::Map(key, value)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(mismatch());
            };
            if fields.len() != 2 {
                return Err(mismatch());
            }
            let fields = Fields::from(vec![
                project_field(&fields[0], key, observe)?,
                project_field(&fields[1], value, observe)?,
            ]);
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(fields)),
                ),
                *sorted,
            )
        }
        (DataType::Struct(fields), SqlType::Struct(declarations)) => {
            if fields.len() != declarations.len() {
                return Err(mismatch());
            }
            let mut projected = Vec::with_capacity(fields.len());
            for (field, (name, declaration)) in fields.iter().zip(declarations) {
                observe()?;
                if field.name() != name {
                    return Err(mismatch());
                }
                projected.push(project_field(field, declaration, observe)?);
            }
            DataType::Struct(Fields::from(projected))
        }
        (_, SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_)) => return Err(mismatch()),
        (
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_),
            _,
        ) => return Err(mismatch()),
        _ => source.clone(),
    })
}
fn project_field<E: From<ColumnValueTypeError> + From<ValueTypeError>>(
    source: &Field,
    declaration: &SqlType,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<Arc<Field>, E> {
    observe()?;
    let expected = root(declaration);
    let actual = field_logical_type(source)
        .map_err(ColumnValueTypeError::from)
        .map_err(E::from)?;
    if source.metadata().contains_key(NR_LOGICAL_TYPE_KEY) && actual != expected {
        return Err(E::from(ColumnValueTypeError::DeclarationMismatch));
    }
    let data_type = project(source.data_type(), declaration, observe)?;
    let mut metadata = source.metadata().clone();
    if let Some(tag) = expected.metadata_value() {
        metadata.insert(NR_LOGICAL_TYPE_KEY.to_owned(), tag.to_owned());
    }
    observe()?;
    Ok(Arc::new(
        source
            .clone()
            .with_data_type(data_type)
            .with_metadata(metadata),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn column(data_type: DataType, declaration: Option<SqlType>) -> ColumnDef {
        ColumnDef {
            name: "source".into(),
            data_type,
            nullable: true,
            logical_type: declaration,
            write_default: None,
        }
    }
    #[test]
    fn explicit_roots_are_authored_without_carrier_inference() {
        for (carrier, declaration, expected) in [
            (DataType::Utf8, SqlType::Json, ValueLogicalType::Json),
            (
                DataType::LargeBinary,
                SqlType::Variant,
                ValueLogicalType::Variant,
            ),
            (DataType::Binary, SqlType::Hll, ValueLogicalType::Hll),
            (DataType::Binary, SqlType::Bitmap, ValueLogicalType::Bitmap),
            (
                DataType::FixedSizeBinary(16),
                SqlType::LargeInt,
                ValueLogicalType::LargeInt,
            ),
        ] {
            let physical = column(carrier.clone(), None).declared_value_type().unwrap();
            let declared = column(carrier, Some(declaration))
                .declared_value_type()
                .unwrap();
            assert_eq!(physical.logical_type, ValueLogicalType::Physical);
            assert_eq!(declared.logical_type, expected);
            assert_ne!(physical, declared);
        }
        assert!(
            column(DataType::Int64, Some(SqlType::Json))
                .declared_value_type()
                .is_err()
        );
    }
    #[test]
    fn frozen_value_domain_roundtrips_without_carrier_inference() {
        for (carrier, domain) in [
            (DataType::Utf8, ValueLogicalType::Physical),
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::LargeBinary, ValueLogicalType::Variant),
            (DataType::Binary, ValueLogicalType::Hll),
            (DataType::Binary, ValueLogicalType::Bitmap),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        ] {
            let original = FunctionValueType {
                data_type: carrier,
                nullable: false,
                logical_type: domain,
            };
            let frozen =
                ColumnDef::from_value_type("value".into(), original.clone(), None).unwrap();
            assert_eq!(frozen.declared_value_type().unwrap(), original);
        }
        let child = Field::new("source_item", DataType::Utf8, false).with_metadata(
            [
                (NR_LOGICAL_TYPE_KEY.into(), "json".into()),
                ("provider.id".into(), "73".into()),
            ]
            .into(),
        );
        let original = FunctionValueType::new(DataType::LargeList(Arc::new(child)), true);
        let frozen = ColumnDef::from_value_type("nested".into(), original.clone(), None).unwrap();
        assert_eq!(frozen.declared_value_type().unwrap(), original);
        let uuid = FunctionValueType {
            data_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: ValueLogicalType::Uuid,
        };
        assert_eq!(
            ColumnDef::from_value_type("uuid".into(), uuid, None).unwrap_err(),
            ColumnValueTypeError::UnrepresentableRootDomain(ValueLogicalType::Uuid)
        );
    }
    #[test]
    fn declared_children_preserve_source_shape_and_provider_facts() {
        let item = Field::new("provider_element", DataType::Utf8, false)
            .with_metadata([("provider.id".into(), "37".into())].into());
        let source = column(
            DataType::LargeList(Arc::new(item)),
            Some(SqlType::Array(Box::new(SqlType::Json))),
        );
        let ty = source.declared_value_type().unwrap();
        let DataType::LargeList(item) = ty.data_type else {
            panic!("source layout changed");
        };
        assert_eq!(item.name(), "provider_element");
        assert!(!item.is_nullable());
        assert_eq!(item.metadata()["provider.id"], "37");
        assert_eq!(field_logical_type(&item).unwrap(), ValueLogicalType::Json);
        assert_eq!(ty.logical_type, ValueLogicalType::Physical);
    }
    #[test]
    fn existing_nested_domain_conflicts_and_unknown_tags_are_rejected() {
        for tag in ["json", "not_a_domain"] {
            let item = Field::new("item", DataType::Utf8, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), tag.into())].into());
            assert!(
                column(
                    DataType::List(Arc::new(item)),
                    Some(SqlType::Array(Box::new(SqlType::String)))
                )
                .declared_value_type()
                .is_err()
            );
        }
        let item = Field::new("item", DataType::Utf8, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "json".into())].into());
        let ty = column(DataType::List(Arc::new(item)), None)
            .declared_value_type()
            .unwrap();
        let DataType::List(item) = ty.data_type else {
            panic!("source layout changed")
        };
        assert_eq!(field_logical_type(&item).unwrap(), ValueLogicalType::Json);
    }
    #[test]
    fn malformed_declaration_shapes_fail_before_a_value_is_returned() {
        for source in [
            column(
                DataType::Int64,
                Some(SqlType::Array(Box::new(SqlType::BigInt))),
            ),
            column(
                DataType::Struct(
                    vec![Arc::new(Field::new("actual", DataType::Int64, true))].into(),
                ),
                Some(SqlType::Struct(vec![("other".into(), SqlType::BigInt)])),
            ),
        ] {
            assert!(source.declared_value_type().is_err());
        }
    }
    #[test]
    fn wide_declaration_uses_the_callers_observer_through_finish() {
        #[derive(Debug, PartialEq)]
        enum Error {
            Source,
            Stop,
        }
        impl From<ColumnValueTypeError> for Error {
            fn from(_: ColumnValueTypeError) -> Self {
                Self::Source
            }
        }
        impl From<ValueTypeError> for Error {
            fn from(_: ValueTypeError) -> Self {
                Self::Source
            }
        }
        let fields = (0..320)
            .map(|i| Arc::new(Field::new(format!("c{i}"), DataType::Utf8, true)))
            .collect::<Vec<_>>();
        let declarations = (0..320)
            .map(|i| (format!("c{i}"), SqlType::Json))
            .collect::<Vec<_>>();
        let source = column(
            DataType::Struct(fields.into()),
            Some(SqlType::Struct(declarations)),
        );
        let mut calls = 0;
        source
            .declared_value_type_observed::<Error>(|| {
                calls += 1;
                Ok(())
            })
            .unwrap();
        assert!(calls > 640);
        for stop in [1, 256, calls] {
            let mut seen = 0;
            let error = source
                .declared_value_type_observed::<Error>(|| {
                    seen += 1;
                    if seen == stop {
                        Err(Error::Stop)
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert_eq!(error, Error::Stop);
            assert_eq!(seen, stop);
        }
    }
}
