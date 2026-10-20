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
use crate::exec::expr::expression_contract;
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use base64::Engine;
use std::sync::Arc;

pub(super) use novarocks_functions::builtin::md5_shared::{
    cast_output, to_owned_bytes_array, to_owned_bytes_array_with_varchar_cast,
};

pub(super) fn to_i64_array(
    array: &ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> ExecutionResult<Int64Array> {
    let casted = cast(array, &DataType::Int64).map_err(|source| {
        ExecutionFailure::from_domain(EncryptionIntegerCast {
            label: novarocks_failure_contract::BoundedDetail::truncating(fn_name),
            argument: Some(arg_idx),
            source,
        })
    })?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| {
            expression_contract(format_args!("{}: arg{} is not BIGINT", fn_name, arg_idx))
        })
}

pub(super) use novarocks_functions::builtin::bytes_output::{
    build_bytes_output_latin1, build_bytes_output_lossy,
};

#[derive(Clone, Copy)]
pub(super) enum BinaryFormatType {
    Hex,
    Encode64,
    Utf8,
}

pub(super) fn parse_binary_format(format: Option<&str>) -> BinaryFormatType {
    let Some(format) = format else {
        return BinaryFormatType::Hex;
    };

    match format.to_ascii_lowercase().as_str() {
        "encode64" => BinaryFormatType::Encode64,
        "utf8" => BinaryFormatType::Utf8,
        _ => BinaryFormatType::Hex,
    }
}

pub(super) fn decode_base64(input: &[u8]) -> Option<Vec<u8>> {
    match base64::engine::general_purpose::STANDARD.decode(input) {
        Ok(bytes) => Some(bytes),
        // These are exactly the SDK's nullable malformed-data outcomes. An SDK
        // variant added in future must be reviewed rather than silently masked.
        Err(
            base64::DecodeError::InvalidByte(_, _)
            | base64::DecodeError::InvalidLength(_)
            | base64::DecodeError::InvalidLastSymbol(_, _)
            | base64::DecodeError::InvalidPadding,
        ) => None,
    }
}

pub(super) use novarocks_functions::builtin::to_base64_shared::encode_base64;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, LargeBinaryArray, LargeStringArray};

    #[test]
    fn to_owned_bytes_accepts_large_binary_iceberg_layout() {
        // Iceberg VARBINARY columns arrive as LargeBinary; from_binary/sha2/aes
        // must accept them, not reject with "must be VARCHAR or VARBINARY".
        let arr: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_ref()),
            None,
            Some(b"\x00\x01".as_ref()),
        ]));
        let owned = to_owned_bytes_array(arr, "from_binary", 0).expect("LargeBinary accepted");
        assert_eq!(owned.len(), 3);
        assert_eq!(owned.bytes(0), b"abc");
        assert!(owned.is_null(1));
        assert_eq!(owned.bytes(2), b"\x00\x01");
    }

    #[test]
    fn to_owned_bytes_accepts_large_utf8_layout() {
        let arr: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("hi"), None]));
        let owned = to_owned_bytes_array(arr, "from_binary", 0).expect("LargeUtf8 accepted");
        assert_eq!(owned.bytes(0), b"hi");
        assert!(owned.is_null(1));
    }

    #[test]
    fn to_owned_bytes_still_rejects_non_byte_types() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert!(to_owned_bytes_array(arr, "from_binary", 0).is_err());
    }
}

#[derive(Debug)]
struct EncryptionIntegerCast {
    label: novarocks_failure_contract::BoundedDetail,
    argument: Option<usize>,
    source: arrow::error::ArrowError,
}
impl std::fmt::Display for EncryptionIntegerCast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.argument {
            Some(argument) => write!(
                f,
                "{}: failed to cast arg{argument} to BIGINT: {}",
                self.label, self.source
            ),
            None => write!(
                f,
                "{}: failed to cast TIMESTAMP to BIGINT: {}",
                self.label, self.source
            ),
        }
    }
}
impl std::error::Error for EncryptionIntegerCast {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for EncryptionIntegerCast {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        match &self.source {
            arrow::error::ArrowError::MemoryError(_) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::EncryptionIntegerAllocation.reason(),
            ),
            _ => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::EncryptionIntegerContract.reason(),
            ),
        }
    }
}

#[cfg(test)]
mod source_properties {
    use super::*;
    use arrow::array::StructArray;
    use arrow::datatypes::Field;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn actual_integer_cast_failure_preserves_sdk_source_and_releases_array_and_fields() {
        let field = Arc::new(Field::new("ResourceExhausted", DataType::Int64, false));
        let weak_field = Arc::downgrade(&field);
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![field].into(),
            vec![Arc::new(Int64Array::from(vec![1]))],
            None,
        ));
        let weak = Arc::downgrade(&input);
        let failure = to_i64_array(&input, "ResourceExhausted", 1).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 522);
        assert!(failure.failure_fact().resource().is_none());
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<arrow::error::ArrowError>()
        );
    }
}

pub(super) fn timestamp_as_i64(
    array: &ArrayRef,
    label: &'static str,
) -> ExecutionResult<Int64Array> {
    let casted = cast(array, &DataType::Int64).map_err(|source| {
        ExecutionFailure::from_domain(EncryptionIntegerCast {
            label: novarocks_failure_contract::BoundedDetail::truncating(label),
            argument: None,
            source,
        })
    })?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| {
            expression_contract(format_args!("{label}: failed to downcast timestamp cast"))
        })
}
