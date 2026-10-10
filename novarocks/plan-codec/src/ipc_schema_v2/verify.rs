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

//! Borrowed standard IPC field verification against the validated source owner.
//! No Arrow schema materialization or independent schema vocabulary is used.

use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
use arrow::ipc;
use novarocks_type_contract::CompileCheckpoints;

type E = TypeCodecError;
const MISMATCH: &str = "IPC field differs from exact authored source";

fn require(matches: bool, work: &mut CompileCheckpoints<'_>) -> Result<(), E> {
    work.step()?;
    if matches {
        Ok(())
    } else {
        Err(E::InvalidShape(MISMATCH))
    }
}

// Length is checked before touching possibly large untrusted strings. Each
// completed slice comparison has a fixed 1024-byte maximum extent.
fn strings_equal(left: &str, right: &str, work: &mut CompileCheckpoints<'_>) -> Result<bool, E> {
    let same_length = left.len() == right.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (left, right) in left
        .as_bytes()
        .chunks(1024)
        .zip(right.as_bytes().chunks(1024))
    {
        let equal = left == right;
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn verify_field(
    expected: &Field,
    actual: ipc::Field<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    require(actual.name().is_some(), work)?;
    let name = actual.name().ok_or(E::InvalidShape(MISMATCH))?;
    let equal = strings_equal(expected.name(), name, work)?;
    require(equal && expected.is_nullable() == actual.nullable(), work)?;
    let entries = actual.custom_metadata();
    require(
        entries.as_ref().map_or(0, |entries| entries.len()) == expected.metadata().len(),
        work,
    )?;
    if let Some(entries) = entries {
        for index in 0..entries.len() {
            let entry = entries.get(index);
            require(entry.key().is_some() && entry.value().is_some(), work)?;
            let key = entry.key().ok_or(E::InvalidShape(MISMATCH))?;
            let value = entry.value().ok_or(E::InvalidShape(MISMATCH))?;
            // Borrowed key comparisons avoid hashing/copying untrusted text.
            // Duplicate association rejection is independent of metadata order.
            for previous in 0..index {
                let previous = entries.get(previous);
                let previous_key = previous.key().ok_or(E::InvalidShape(MISMATCH))?;
                let duplicate = strings_equal(key, previous_key, work)?;
                require(!duplicate, work)?;
            }
            let mut found = false;
            for (expected_key, expected_value) in expected.metadata() {
                let same_key = strings_equal(key, expected_key, work)?;
                if same_key {
                    let same_value = strings_equal(value, expected_value, work)?;
                    require(same_value, work)?;
                    found = true;
                    break;
                }
            }
            require(found, work)?;
        }
    }
    match expected.data_type() {
        DataType::Dictionary(key, value) => {
            if matches!(value.as_ref(), DataType::Dictionary(..)) {
                return Err(E::InvalidShape(
                    "bare dictionary-of-dictionary IPC representation is pending",
                ));
            }
            require(actual.dictionary().is_some(), work)?;
            let dictionary = actual.dictionary().ok_or(E::InvalidShape(MISMATCH))?;
            #[allow(deprecated)]
            let expected_id = expected.dict_id();
            require(
                Some(dictionary.id()) == expected_id
                    && Some(dictionary.isOrdered()) == expected.dict_is_ordered()
                    && dictionary.dictionaryKind() == ipc::DictionaryKind::DenseArray,
                work,
            )?;
            require(dictionary.indexType().is_some(), work)?;
            let index = dictionary.indexType().ok_or(E::InvalidShape(MISMATCH))?;
            let (width, signed) = integer(key).ok_or(E::InvalidShape(MISMATCH))?;
            require(
                index.bitWidth() == width && index.is_signed() == signed,
                work,
            )?;
            verify_type(value, actual, work)?;
        }
        data_type => {
            require(actual.dictionary().is_none(), work)?;
            verify_type(data_type, actual, work)?;
        }
    }
    Ok(())
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
fn unit(unit: &TimeUnit) -> ipc::TimeUnit {
    match unit {
        TimeUnit::Second => ipc::TimeUnit::SECOND,
        TimeUnit::Millisecond => ipc::TimeUnit::MILLISECOND,
        TimeUnit::Microsecond => ipc::TimeUnit::MICROSECOND,
        TimeUnit::Nanosecond => ipc::TimeUnit::NANOSECOND,
    }
}
fn child_count(
    actual: ipc::Field<'_>,
    count: usize,
    required: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    let children = actual.children();
    require(!required || children.is_some(), work)?;
    require(
        children.as_ref().map_or(0, |fields| fields.len()) == count,
        work,
    )
}
fn child(
    expected: &Field,
    actual: ipc::Field<'_>,
    index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    let children = actual.children().ok_or(E::InvalidShape(MISMATCH))?;
    // The expected-author count was checked before indexing; retain a local
    // guard so this helper cannot become an unchecked accessor on reuse.
    require(index < children.len(), work)?;
    verify_field(expected, children.get(index), work)
}

macro_rules! payload {
    ($actual:ident, $work:ident, $kind:ident, $accessor:ident) => {{
        require($actual.type_type() == ipc::Type::$kind, $work)?;
        require($actual.$accessor().is_some(), $work)?;
        $actual.$accessor().ok_or(E::InvalidShape(MISMATCH))?
    }};
}
// Keep nonrecursive payload temporaries out of each live recursive frame.
// The original child checks and their completed observations stay in order.
fn verify_type(
    expected: &DataType,
    actual: ipc::Field<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    match expected {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            match expected {
                DataType::List(_) => {
                    payload!(actual, work, List, type_as_list);
                }
                DataType::LargeList(_) => {
                    payload!(actual, work, LargeList, type_as_large_list);
                }
                DataType::ListView(_) => {
                    payload!(actual, work, ListView, type_as_list_view);
                }
                DataType::LargeListView(_) => {
                    payload!(actual, work, LargeListView, type_as_large_list_view);
                }
                DataType::FixedSizeList(_, size) => {
                    let value = payload!(actual, work, FixedSizeList, type_as_fixed_size_list);
                    require(value.listSize() == *size, work)?;
                }
                DataType::Map(_, sorted) => {
                    let value = payload!(actual, work, Map, type_as_map);
                    require(value.keysSorted() == *sorted, work)?;
                }
                _ => return Err(E::InvalidShape(MISMATCH)),
            }
            child_count(actual, 1, true, work)?;
            child(field, actual, 0, work)?;
            return Ok(());
        }
        DataType::Struct(fields) => {
            payload!(actual, work, Struct_, type_as_struct_);
            child_count(actual, fields.len(), false, work)?;
            for (index, field) in fields.iter().enumerate() {
                child(field, actual, index, work)?;
                work.step()?;
            }
            return Ok(());
        }
        DataType::RunEndEncoded(run_ends, values) => {
            payload!(actual, work, RunEndEncoded, type_as_run_end_encoded);
            child_count(actual, 2, true, work)?;
            child(run_ends, actual, 0, work)?;
            child(values, actual, 1, work)?;
            return Ok(());
        }
        DataType::Union(fields, mode) => {
            let union = payload!(actual, work, Union, type_as_union);
            let mode = match mode {
                UnionMode::Sparse => ipc::UnionMode::Sparse,
                UnionMode::Dense => ipc::UnionMode::Dense,
            };
            require(union.mode() == mode, work)?;
            child_count(actual, fields.len(), false, work)?;
            require(union.typeIds().is_some(), work)?;
            let ids = union.typeIds().ok_or(E::InvalidShape(MISMATCH))?;
            require(ids.len() == fields.len(), work)?;
            for (index, (id, field)) in fields.iter().enumerate() {
                require(ids.get(index) == i32::from(id), work)?;
                child(field, actual, index, work)?;
                work.step()?;
            }
            return Ok(());
        }
        DataType::Dictionary(..) => {
            return Err(E::InvalidShape(
                "bare dictionary-of-dictionary IPC representation is pending",
            ));
        }
        _ => verify_leaf_type(expected, actual, work)?,
    }
    Ok(())
}

#[inline(never)]
fn verify_leaf_type(
    expected: &DataType,
    actual: ipc::Field<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    match expected {
        DataType::Null => {
            payload!(actual, work, Null, type_as_null);
        }
        DataType::Boolean => {
            payload!(actual, work, Bool, type_as_bool);
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let integer_payload = payload!(actual, work, Int, type_as_int);
            let (width, signed) = integer(expected).ok_or(E::InvalidShape(MISMATCH))?;
            require(
                integer_payload.bitWidth() == width && integer_payload.is_signed() == signed,
                work,
            )?;
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let floating = payload!(actual, work, FloatingPoint, type_as_floating_point);
            let precision = match expected {
                DataType::Float16 => ipc::Precision::HALF,
                DataType::Float32 => ipc::Precision::SINGLE,
                _ => ipc::Precision::DOUBLE,
            };
            require(floating.precision() == precision, work)?;
        }
        DataType::Binary => {
            payload!(actual, work, Binary, type_as_binary);
        }
        DataType::LargeBinary => {
            payload!(actual, work, LargeBinary, type_as_large_binary);
        }
        DataType::BinaryView => {
            payload!(actual, work, BinaryView, type_as_binary_view);
        }
        DataType::Utf8 => {
            payload!(actual, work, Utf8, type_as_utf_8);
        }
        DataType::LargeUtf8 => {
            payload!(actual, work, LargeUtf8, type_as_large_utf_8);
        }
        DataType::Utf8View => {
            payload!(actual, work, Utf8View, type_as_utf_8_view);
        }
        DataType::FixedSizeBinary(width) => {
            let binary = payload!(actual, work, FixedSizeBinary, type_as_fixed_size_binary);
            require(binary.byteWidth() == *width, work)?;
        }
        DataType::Date32 | DataType::Date64 => {
            let date = payload!(actual, work, Date, type_as_date);
            let unit = if matches!(expected, DataType::Date32) {
                ipc::DateUnit::DAY
            } else {
                ipc::DateUnit::MILLISECOND
            };
            require(date.unit() == unit, work)?;
        }
        DataType::Time32(time_unit) | DataType::Time64(time_unit) => {
            let time = payload!(actual, work, Time, type_as_time);
            let width = if matches!(expected, DataType::Time32(_)) {
                32
            } else {
                64
            };
            require(
                time.unit() == unit(time_unit) && time.bitWidth() == width,
                work,
            )?;
        }
        DataType::Timestamp(time_unit, timezone) => {
            let timestamp = payload!(actual, work, Timestamp, type_as_timestamp);
            require(timestamp.unit() == unit(time_unit), work)?;
            match (timezone.as_deref(), timestamp.timezone()) {
                (None, None) => {
                    work.step()?;
                }
                (Some(expected), Some(actual)) => {
                    let equal = strings_equal(expected, actual, work)?;
                    require(equal, work)?;
                }
                _ => {
                    require(false, work)?;
                }
            }
        }
        DataType::Duration(time_unit) => {
            let duration = payload!(actual, work, Duration, type_as_duration);
            require(duration.unit() == unit(time_unit), work)?;
        }
        DataType::Interval(interval) => {
            let value = payload!(actual, work, Interval, type_as_interval);
            let unit = match interval {
                IntervalUnit::YearMonth => ipc::IntervalUnit::YEAR_MONTH,
                IntervalUnit::DayTime => ipc::IntervalUnit::DAY_TIME,
                IntervalUnit::MonthDayNano => ipc::IntervalUnit::MONTH_DAY_NANO,
            };
            require(value.unit() == unit, work)?;
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let decimal = payload!(actual, work, Decimal, type_as_decimal);
            let width = match expected {
                DataType::Decimal32(..) => 32,
                DataType::Decimal64(..) => 64,
                DataType::Decimal128(..) => 128,
                _ => 256,
            };
            require(
                decimal.precision() == i32::from(*precision)
                    && decimal.scale() == i32::from(*scale)
                    && decimal.bitWidth() == width,
                work,
            )?;
        }
        // Only verify_type calls this helper, after handling recursive carriers.
        _ => return Err(E::InvalidShape(MISMATCH)),
    }
    child_count(actual, 0, false, work)
}
