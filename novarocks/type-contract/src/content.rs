// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_schema::{DataType, TimeUnit};

/// Canonical public identity shared by result encoding and durable MV eligibility.
pub const NATIVE_RESULT_CONTENT_V1_CANONICAL_BYTES: &[u8] =
    b"novarocks.mv.result-content.NativeResultContentV1";

/// Result-content equality is independent of SQL grouping and join equality.
/// Float leaves preserve every IEEE bit, including signed zero and NaN payloads.
/// Dictionaries represent logical values; list/map order and nested NULL matter.
/// The frozen native mapping reserves LargeBinary for typed VARIANT content.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResultContentEquivalence {
    NativeResultContentV1,
}

impl ResultContentEquivalence {
    pub const fn canonical_bytes(self) -> &'static [u8] {
        match self {
            Self::NativeResultContentV1 => NATIVE_RESULT_CONTENT_V1_CANONICAL_BYTES,
        }
    }

    pub fn supports(self, data_type: &DataType) -> bool {
        match data_type {
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
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(..)
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => true,
            DataType::Time32(unit) => matches!(unit, TimeUnit::Second | TimeUnit::Millisecond),
            DataType::Time64(unit) => matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond),
            DataType::FixedSizeBinary(width) => *width >= 0,
            DataType::List(field) | DataType::LargeList(field) => self.supports(field.data_type()),
            DataType::FixedSizeList(field, width) => {
                *width >= 0 && self.supports(field.data_type())
            }
            DataType::Struct(fields) => fields.iter().all(|field| self.supports(field.data_type())),
            DataType::Map(entries, _) => {
                matches!(entries.data_type(), DataType::Struct(fields) if fields.len() == 2 && fields.iter().all(|field| self.supports(field.data_type())))
            }
            DataType::Dictionary(key, value) => {
                matches!(
                    key.as_ref(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                ) && self.supports(value)
            }
            _ => false,
        }
    }
}

/// Conservative intersection of bit-exact result content and the native
/// scalar runtime-filter vocabulary. Unsupported fields keep the quota scan
/// complete; this predicate never changes content-key support or equality.
pub fn quota_content_runtime_filter_type_supported(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::FixedSizeBinary(16)
            | DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Decimal128(..)
    ) && ResultContentEquivalence::NativeResultContentV1.supports(ty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn content_contract_covers_logical_leaf_and_recursive_output_types() {
        let contract = ResultContentEquivalence::NativeResultContentV1;
        for ty in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::FixedSizeBinary(16),
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, 4),
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Time64(TimeUnit::Microsecond),
        ] {
            assert!(contract.supports(&ty), "{ty}");
            assert!(contract.supports(&DataType::List(Arc::new(Field::new(
                "item",
                ty.clone(),
                true
            )))));
            assert!(contract.supports(&DataType::Struct(
                vec![Field::new("field", ty, true)].into()
            )));
        }
        assert!(contract.supports(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        assert!(!contract.supports(&DataType::Duration(TimeUnit::Second)));
        assert!(!contract.supports(&DataType::Time32(TimeUnit::Nanosecond)));
        assert_eq!(
            contract.canonical_bytes(),
            NATIVE_RESULT_CONTENT_V1_CANONICAL_BYTES
        );
    }
}
