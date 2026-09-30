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

use crate::{ConnectorError, ConnectorErrorKind};
use arrow_schema::{DataType, Field};

/// Rebuild bounded physical fields without retaining excess collection/string
/// capacity or unrelated backing through nested Arc fields. Callers validate
/// the aggregate schema before invoking this copy.
pub(crate) fn owned_field(field: &Field) -> Result<Field, ConnectorError> {
    // This constructor is required to preserve Arrow's physical dictionary ID
    // and ordering; ordinary Field equality/constructors omit those attributes.
    #[allow(deprecated)]
    let copy = Field::new_dict(
        field.name().to_owned(),
        owned_type(field.data_type())?,
        field.is_nullable(),
        field.dict_id().unwrap_or(0),
        field.dict_is_ordered().unwrap_or(false),
    );
    Ok(copy.with_metadata(
        field
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    ))
}

fn owned_type(data_type: &DataType) -> Result<DataType, ConnectorError> {
    use std::sync::Arc;
    let field = |field: &Field| owned_field(field).map(Arc::new);
    Ok(match data_type {
        DataType::Timestamp(unit, zone) => DataType::Timestamp(
            *unit,
            zone.as_ref().map(|zone| Arc::<str>::from(zone.as_ref())),
        ),
        DataType::List(item) => DataType::List(field(item)?),
        DataType::ListView(item) => DataType::ListView(field(item)?),
        DataType::LargeList(item) => DataType::LargeList(field(item)?),
        DataType::LargeListView(item) => DataType::LargeListView(field(item)?),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(field(item)?, *size),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|item| field(item))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::Union(fields, mode) => {
            let (ids, fields): (Vec<_>, Vec<_>) = fields
                .iter()
                .map(|(id, item)| field(item).map(|item| (id, item)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            DataType::Union(
                arrow_schema::UnionFields::try_new(ids, fields).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "frozen schema has invalid union field identities",
                    )
                })?,
                *mode,
            )
        }
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(Box::new(owned_type(key)?), Box::new(owned_type(value)?))
        }
        DataType::Map(entries, sorted) => DataType::Map(field(entries)?, *sorted),
        DataType::RunEndEncoded(runs, values) => {
            DataType::RunEndEncoded(field(runs)?, field(values)?)
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => data_type.clone(),
    })
}
