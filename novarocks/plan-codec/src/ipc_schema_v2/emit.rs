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

//! Exact borrowed Arrow Field projection into standard IPC FlatBuffer builders.
//! The parent admits grammar, depth, occurrence, string and builder extents.
//! Library copies/growth remain opaque; this leaf does not authorize host memory.

use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
use arrow::ipc;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::cmp::Ordering;

type E = TypeCodecError;
type FieldOffset<'a> = WIPOffset<ipc::Field<'a>>;
type TypeOffset = WIPOffset<UnionWIPOffset>;

fn opaque<T>(work: &mut CompileCheckpoints<'_>, action: impl FnOnce() -> T) -> Result<T, E> {
    work.flush()?;
    let result = action();
    work.flush()?;
    Ok(result)
}

fn reserve<T>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, E> {
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| E::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(values)
}

fn compare_keys(left: &str, right: &str, work: &mut CompileCheckpoints<'_>) -> Result<Ordering, E> {
    for (left, right) in left.bytes().zip(right.bytes()) {
        let order = left.cmp(&right);
        work.step()?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    let order = left.len().cmp(&right.len());
    work.step()?;
    Ok(order)
}

pub(super) fn emit_field<'a>(
    field: &Field,
    builder: &mut FlatBufferBuilder<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FieldOffset<'a>, E> {
    let mut sorted = reserve(field.metadata().len(), work)?;
    for (key, value) in field.metadata() {
        sorted.push((key.as_str(), value.as_str()));
        work.step()?;
        let mut index = sorted.len() - 1;
        while index > 0 {
            if compare_keys(sorted[index - 1].0, sorted[index].0, work)? != Ordering::Greater {
                break;
            }
            sorted.swap(index - 1, index);
            work.step()?;
            index -= 1;
        }
    }
    let mut metadata = reserve(sorted.len(), work)?;
    for (key, value) in sorted {
        let key = opaque(work, || builder.create_string(key))?;
        let value = opaque(work, || builder.create_string(value))?;
        let entry = opaque(work, || {
            ipc::KeyValue::create(
                builder,
                &ipc::KeyValueArgs {
                    key: Some(key),
                    value: Some(value),
                },
            )
        })?;
        metadata.push(entry);
        work.step()?;
    }
    let custom_metadata = if metadata.is_empty() {
        None
    } else {
        Some(opaque(work, || builder.create_vector(&metadata))?)
    };
    let name = opaque(work, || builder.create_string(field.name()))?;
    let (type_type, type_, children) = emit_type(field.data_type(), builder, work)?;
    let dictionary = match field.data_type() {
        DataType::Dictionary(key, _) => {
            let (width, signed) =
                integer(key).ok_or(E::InvalidShape("dictionary key must be an integer carrier"))?;
            #[allow(deprecated)]
            let id = field
                .dict_id()
                .ok_or(E::InvalidShape("dictionary field ID is missing"))?;
            let ordered = field
                .dict_is_ordered()
                .ok_or(E::InvalidShape("dictionary field ordering is missing"))?;
            let index = opaque(work, || {
                ipc::Int::create(
                    builder,
                    &ipc::IntArgs {
                        bitWidth: width,
                        is_signed: signed,
                    },
                )
            })?;
            Some(opaque(work, || {
                ipc::DictionaryEncoding::create(
                    builder,
                    &ipc::DictionaryEncodingArgs {
                        id,
                        indexType: Some(index),
                        isOrdered: ordered,
                        dictionaryKind: ipc::DictionaryKind::DenseArray,
                    },
                )
            })?)
        }
        _ => None,
    };
    let children = opaque(work, || builder.create_vector(&children))?;
    let result = opaque(work, || {
        ipc::Field::create(
            builder,
            &ipc::FieldArgs {
                name: Some(name),
                nullable: field.is_nullable(),
                type_type,
                type_: Some(type_),
                dictionary,
                children: Some(children),
                custom_metadata,
            },
        )
    })?;
    work.step()?;
    Ok(result)
}

fn integer(data_type: &DataType) -> Option<(i32, bool)> {
    match data_type {
        DataType::Int8 => Some((8, true)),
        DataType::Int16 => Some((16, true)),
        DataType::Int32 => Some((32, true)),
        DataType::Int64 => Some((64, true)),
        DataType::UInt8 => Some((8, false)),
        DataType::UInt16 => Some((16, false)),
        DataType::UInt32 => Some((32, false)),
        DataType::UInt64 => Some((64, false)),
        _ => None,
    }
}
fn unit(value: &TimeUnit) -> ipc::TimeUnit {
    match value {
        TimeUnit::Second => ipc::TimeUnit::SECOND,
        TimeUnit::Millisecond => ipc::TimeUnit::MILLISECOND,
        TimeUnit::Microsecond => ipc::TimeUnit::MICROSECOND,
        TimeUnit::Nanosecond => ipc::TimeUnit::NANOSECOND,
    }
}

fn emit_type<'a>(
    data_type: &DataType,
    builder: &mut FlatBufferBuilder<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ipc::Type, TypeOffset, Vec<FieldOffset<'a>>), E> {
    let mut children = Vec::new();
    // Tables have fixed schema-defined size. Library builder growth is opaque.
    macro_rules! empty {
        ($kind:ident, $table:ident) => {{
            (
                ipc::Type::$kind,
                opaque(work, || ipc::$table::create(builder, &Default::default()))?
                    .as_union_value(),
            )
        }};
    }
    let (kind, value) = match data_type {
        DataType::Null => empty!(Null, Null),
        DataType::Boolean => empty!(Bool, Bool),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let (width, signed) =
                integer(data_type).ok_or(E::InvalidShape("invalid integer carrier"))?;
            (
                ipc::Type::Int,
                opaque(work, || {
                    ipc::Int::create(
                        builder,
                        &ipc::IntArgs {
                            bitWidth: width,
                            is_signed: signed,
                        },
                    )
                })?
                .as_union_value(),
            )
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let precision = match data_type {
                DataType::Float16 => ipc::Precision::HALF,
                DataType::Float32 => ipc::Precision::SINGLE,
                _ => ipc::Precision::DOUBLE,
            };
            (
                ipc::Type::FloatingPoint,
                opaque(work, || {
                    ipc::FloatingPoint::create(builder, &ipc::FloatingPointArgs { precision })
                })?
                .as_union_value(),
            )
        }
        DataType::Binary => empty!(Binary, Binary),
        DataType::LargeBinary => empty!(LargeBinary, LargeBinary),
        DataType::BinaryView => empty!(BinaryView, BinaryView),
        DataType::Utf8 => empty!(Utf8, Utf8),
        DataType::LargeUtf8 => empty!(LargeUtf8, LargeUtf8),
        DataType::Utf8View => empty!(Utf8View, Utf8View),
        DataType::FixedSizeBinary(width) => (
            ipc::Type::FixedSizeBinary,
            opaque(work, || {
                ipc::FixedSizeBinary::create(
                    builder,
                    &ipc::FixedSizeBinaryArgs { byteWidth: *width },
                )
            })?
            .as_union_value(),
        ),
        DataType::Date32 | DataType::Date64 => {
            let unit = if matches!(data_type, DataType::Date32) {
                ipc::DateUnit::DAY
            } else {
                ipc::DateUnit::MILLISECOND
            };
            (
                ipc::Type::Date,
                opaque(work, || ipc::Date::create(builder, &ipc::DateArgs { unit }))?
                    .as_union_value(),
            )
        }
        DataType::Time32(time_unit) | DataType::Time64(time_unit) => {
            let bit_width = if matches!(data_type, DataType::Time32(_)) {
                32
            } else {
                64
            };
            (
                ipc::Type::Time,
                opaque(work, || {
                    ipc::Time::create(
                        builder,
                        &ipc::TimeArgs {
                            unit: unit(time_unit),
                            bitWidth: bit_width,
                        },
                    )
                })?
                .as_union_value(),
            )
        }
        DataType::Timestamp(time_unit, timezone) => {
            // Presence is significant: Some("") must not collapse to None.
            let timezone = match timezone {
                Some(zone) => Some(opaque(work, || builder.create_string(zone))?),
                None => None,
            };
            (
                ipc::Type::Timestamp,
                opaque(work, || {
                    ipc::Timestamp::create(
                        builder,
                        &ipc::TimestampArgs {
                            unit: unit(time_unit),
                            timezone,
                        },
                    )
                })?
                .as_union_value(),
            )
        }
        DataType::Duration(time_unit) => (
            ipc::Type::Duration,
            opaque(work, || {
                ipc::Duration::create(
                    builder,
                    &ipc::DurationArgs {
                        unit: unit(time_unit),
                    },
                )
            })?
            .as_union_value(),
        ),
        DataType::Interval(interval) => {
            let unit = match interval {
                IntervalUnit::YearMonth => ipc::IntervalUnit::YEAR_MONTH,
                IntervalUnit::DayTime => ipc::IntervalUnit::DAY_TIME,
                IntervalUnit::MonthDayNano => ipc::IntervalUnit::MONTH_DAY_NANO,
            };
            (
                ipc::Type::Interval,
                opaque(work, || {
                    ipc::Interval::create(builder, &ipc::IntervalArgs { unit })
                })?
                .as_union_value(),
            )
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let width = match data_type {
                DataType::Decimal32(..) => 32,
                DataType::Decimal64(..) => 64,
                DataType::Decimal128(..) => 128,
                _ => 256,
            };
            (
                ipc::Type::Decimal,
                opaque(work, || {
                    ipc::Decimal::create(
                        builder,
                        &ipc::DecimalArgs {
                            precision: i32::from(*precision),
                            scale: i32::from(*scale),
                            bitWidth: width,
                        },
                    )
                })?
                .as_union_value(),
            )
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            children = reserve(1, work)?;
            children.push(emit_field(field, builder, work)?);
            work.step()?;
            match data_type {
                DataType::List(_) => empty!(List, List),
                DataType::LargeList(_) => empty!(LargeList, LargeList),
                DataType::ListView(_) => empty!(ListView, ListView),
                DataType::LargeListView(_) => empty!(LargeListView, LargeListView),
                DataType::FixedSizeList(_, size) => (
                    ipc::Type::FixedSizeList,
                    opaque(work, || {
                        ipc::FixedSizeList::create(
                            builder,
                            &ipc::FixedSizeListArgs { listSize: *size },
                        )
                    })?
                    .as_union_value(),
                ),
                DataType::Map(_, sorted) => (
                    ipc::Type::Map,
                    opaque(work, || {
                        ipc::Map::create(
                            builder,
                            &ipc::MapArgs {
                                keysSorted: *sorted,
                            },
                        )
                    })?
                    .as_union_value(),
                ),
                _ => return Err(E::InvalidShape("invalid single-child carrier")),
            }
        }
        DataType::Struct(fields) => {
            children = reserve(fields.len(), work)?;
            for field in fields {
                children.push(emit_field(field, builder, work)?);
                work.step()?;
            }
            empty!(Struct_, Struct_)
        }
        DataType::RunEndEncoded(run_ends, values) => {
            children = reserve(2, work)?;
            children.push(emit_field(run_ends, builder, work)?);
            work.step()?;
            children.push(emit_field(values, builder, work)?);
            work.step()?;
            empty!(RunEndEncoded, RunEndEncoded)
        }
        DataType::Union(fields, mode) => {
            children = reserve(fields.len(), work)?;
            let mut ids = reserve(fields.len(), work)?;
            for (id, field) in fields.iter() {
                children.push(emit_field(field, builder, work)?);
                ids.push(i32::from(id));
                work.step()?;
            }
            let ids = opaque(work, || builder.create_vector(&ids))?;
            let mode = match mode {
                UnionMode::Sparse => ipc::UnionMode::Sparse,
                UnionMode::Dense => ipc::UnionMode::Dense,
            };
            (
                ipc::Type::Union,
                opaque(work, || {
                    ipc::Union::create(
                        builder,
                        &ipc::UnionArgs {
                            mode,
                            typeIds: Some(ids),
                        },
                    )
                })?
                .as_union_value(),
            )
        }
        DataType::Dictionary(_, value) => {
            if matches!(value.as_ref(), DataType::Dictionary(..)) {
                return Err(E::InvalidShape(
                    "bare dictionary-of-dictionary IPC representation is pending",
                ));
            }
            let result = emit_type(value, builder, work)?;
            work.step()?;
            return Ok(result);
        }
    };
    work.step()?;
    Ok((kind, value, children))
}
