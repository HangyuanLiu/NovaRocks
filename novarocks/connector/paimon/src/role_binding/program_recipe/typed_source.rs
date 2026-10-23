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

//! Complete PAI-1 declarations from the actual frozen SDK column author.

use std::collections::HashMap;

use super::*;
use crate::schema::PaimonDataType;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use novarocks_connector_contract::{
    ConnectorReadExecutionSchema, ConnectorSourceFieldFacts, ConnectorSourceSchemaFacts,
    ConnectorSourceSchemaShape,
};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_type_contract::{FunctionValueType, ValueKind, ValueLayout, ValueTypeRef};

type CreationFailure = PureProviderCompileError<ConnectorError>;

fn invalid_creation(message: &'static str) -> CreationFailure {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
}
fn resource_creation() -> CreationFailure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Paimon typed source construction extent exceeds address space",
    )
    .into()
}

pub(super) fn publish(
    table_columns: &[PaimonColumn],
    columns: &[&PaimonColumn],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, CreationFailure> {
    creation.work(1)?;
    if columns.len() > novarocks_connector_contract::MAX_CONNECTOR_RECIPE_COLUMNS {
        return Err(resource_creation());
    }
    let storage = creation.storage();
    let mut fields = creation.reserve::<Arc<Field>>(columns.len())?;
    let mut bindings = creation.reserve::<FunctionValueType>(columns.len())?;
    let mut facts = creation.reserve::<ConnectorSourceFieldFacts>(columns.len())?;
    for (ordinal, column) in columns.iter().enumerate() {
        let mut found = false;
        for candidate in table_columns {
            creation.work(1)?;
            creation.work(column.name().len())?;
            if candidate == *column {
                found = true;
                break;
            }
        }
        if !found {
            return Err(invalid_creation(
                "Paimon source column differs from its frozen SDK table",
            ));
        }
        let data_type = data_type(column.data_type());
        // PAI-1 admits only these declared primitives. The physical carrier
        // does not establish an opaque or nominal domain.
        let kind = match &data_type {
            DataType::Utf8 => ValueKind::String,
            DataType::Binary => ValueKind::Binary,
            _ => ValueKind::Scalar(data_type.clone()),
        };
        creation
            .request(ValueTypeRef::storage_allocation_layout().map_err(|_| resource_creation())?)?;
        creation.work(1)?;
        let semantic =
            ValueTypeRef::try_new(kind, column.nullable(), storage.clone()).map_err(|_| {
                invalid_creation("Paimon SDK column has an invalid complete declaration")
            })?;
        creation
            .request(ValueLayout::storage_allocation_layout().map_err(|_| resource_creation())?)?;
        let layout = ValueLayout::try_from_normalized_arrow_observed(
            data_type.clone(),
            storage.clone(),
            |units| creation.work(units).map_err(LayoutFailure::Caller),
        )
        .map_err(|cause: LayoutFailure| cause.into_creation())?;
        creation.work(1)?;
        let binding = FunctionValueType::try_from_value(semantic, layout).map_err(|_| {
            invalid_creation("Paimon SDK declaration differs from its execution layout")
        })?;
        let name = creation.text(column.name())?;
        let field = creation.arc(Field::new(name, data_type, column.nullable()))?;
        fields.push(field);
        bindings.push(binding);
        let mut path = creation.reserve::<u32>(1)?;
        creation.work(1)?;
        path.push(u32::try_from(ordinal).map_err(|_| resource_creation())?);
        let (request, work) = creation.observers();
        let fact = ConnectorSourceFieldFacts::try_new_observed(
            path.into_boxed_slice(),
            Some(column.field_id()),
            None,
            HashMap::new(),
            &[FIELD_ID_KEY],
            storage.clone(),
            0,
            |layout| request(layout).map_err(CreationFailure::Control),
            |units| work(units).map_err(CreationFailure::Control),
        )?;
        facts.push(fact);
    }
    let fields = creation.arrow_fields(fields)?;
    let schema = creation.arc(Schema::new(fields))?;
    let shape =
        ConnectorSourceSchemaShape::try_new_observed(schema.clone(), storage.clone(), |units| {
            creation.work(units)
        })?;
    let (request, work) = creation.observers();
    let source = ConnectorSourceSchemaFacts::try_new_observed(
        shape,
        HashMap::new(),
        facts.into_boxed_slice(),
        storage.clone(),
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )?;
    let (request, work) = creation.observers();
    let execution = ConnectorReadExecutionSchema::try_new_observed(
        schema,
        bindings.into_boxed_slice(),
        storage.clone(),
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )?;
    let (request, work) = creation.observers();
    ConnectorReadPublicSchema::try_new_typed_observed(
        &execution,
        &source,
        storage,
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )
}

enum LayoutFailure {
    Caller(CreationFailure),
    Source,
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for LayoutFailure {
    fn from(_: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Source
    }
}
impl LayoutFailure {
    fn into_creation(self) -> CreationFailure {
        match self {
            Self::Caller(cause) => cause,
            Self::Source => invalid_creation("Paimon SDK column has an invalid value layout"),
        }
    }
}

pub(super) fn data_type(declaration: PaimonDataType) -> DataType {
    match declaration {
        PaimonDataType::Boolean => DataType::Boolean,
        PaimonDataType::Int8 => DataType::Int8,
        PaimonDataType::Int16 => DataType::Int16,
        PaimonDataType::Int32 => DataType::Int32,
        PaimonDataType::Int64 => DataType::Int64,
        PaimonDataType::Float32 => DataType::Float32,
        PaimonDataType::Float64 => DataType::Float64,
        PaimonDataType::Decimal128 { precision, scale } => {
            DataType::Decimal128(precision, scale as i8)
        }
        PaimonDataType::Utf8 => DataType::Utf8,
        PaimonDataType::Binary => DataType::Binary,
        PaimonDataType::Date32 => DataType::Date32,
        PaimonDataType::TimestampMillis { .. } => DataType::Timestamp(TimeUnit::Millisecond, None),
        PaimonDataType::TimestampMicros { .. } => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}
