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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use arrow_schema::DataType;

use crate::LARGEINT_BYTE_WIDTH;

/// Complete row-hash identity shared by planner, codec and execution.
///
/// A variant freezes value canonicalization, NULL handling, multi-column
/// combination and partition reduction. Any change requires a new variant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PartitionHashAlgorithm {
    /// Native exchange FNV-based row combination followed by unsigned modulo.
    NativeExchangeV1,
    /// Native bucket-shuffle CRC32 combination followed by bucket modulo.
    NativeBucketCrc32V1,
}

impl PartitionHashAlgorithm {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::NativeExchangeV1 => "novarocks.native-exchange.v1",
            Self::NativeBucketCrc32V1 => "novarocks.native-bucket-crc32.v1",
        }
    }

    pub const fn nulls_equal(self) -> bool {
        match self {
            Self::NativeExchangeV1 | Self::NativeBucketCrc32V1 => true,
        }
    }

    /// Reports the exact Arrow key domain for which revision 1 hashing is
    /// total and consistent with SQL equality.
    ///
    /// Floating-point keys are excluded because the current kernels hash
    /// signed zero by bits while SQL compares `-0.0` and `+0.0` as equal.
    /// Complex and unsigned types remain outside v1 until their equality and
    /// canonical encoding are frozen under a new algorithm identity.
    pub fn supports_partition_key(self, data_type: &DataType) -> bool {
        let scalar = matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Date32
                | DataType::Timestamp(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH)
        );
        let list = matches!(
            data_type,
            DataType::List(field)
                if matches!(field.data_type(), DataType::Utf8 | DataType::Int32)
        );
        match self {
            Self::NativeExchangeV1 | Self::NativeBucketCrc32V1 => scalar || list,
        }
    }
}

/// Stable identity of one logical partition space.
///
/// Equal hash definitions and counts do not prove co-partitioning: separately
/// produced layouts may share the same shape. Every co-partition boundary must
/// therefore compare this identity along with the full definition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionSpaceId([u8; 32]);

impl PartitionSpaceId {
    pub fn try_new(value: [u8; 32]) -> Result<Self, PartitionSpaceIdentityError> {
        if value == [0; 32] {
            return Err(PartitionSpaceIdentityError::Zero);
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionSpaceIdentityError {
    Zero,
}

impl fmt::Display for PartitionSpaceIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("partition-space identity must be non-zero")
    }
}

impl std::error::Error for PartitionSpaceIdentityError {}

/// Stable symbol instantiated to a concrete count by task assignment.
///
/// The final plan carries this identity and its admissible domain, never a live
/// topology count or mutable route.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionCountParameterId([u8; 32]);

impl PartitionCountParameterId {
    pub fn try_new(value: [u8; 32]) -> Result<Self, PartitionCountParameterIdentityError> {
        if value == [0; 32] {
            return Err(PartitionCountParameterIdentityError::Zero);
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionCountParameterIdentityError {
    Zero,
}

impl fmt::Display for PartitionCountParameterIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("partition-count parameter identity must be non-zero")
    }
}

impl std::error::Error for PartitionCountParameterIdentityError {}

/// Closed identity of the bucket-to-ordinal layout consumed by execution.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BucketLayoutAlgorithm {
    /// Every bucket owns exactly one ordinal in `[0, bucket_count)`.
    DenseZeroBasedV1,
}

impl BucketLayoutAlgorithm {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::DenseZeroBasedV1 => "novarocks.bucket-layout.dense-zero-based.v1",
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::{
        BucketLayoutAlgorithm, PartitionCountParameterId, PartitionCountParameterIdentityError,
        PartitionHashAlgorithm, PartitionSpaceId, PartitionSpaceIdentityError,
    };

    #[test]
    fn native_exchange_hash_identity_includes_revision_and_null_semantics() {
        let algorithm = PartitionHashAlgorithm::NativeExchangeV1;
        assert_eq!(algorithm.stable_name(), "novarocks.native-exchange.v1");
        assert!(algorithm.nulls_equal());
        assert!(algorithm.supports_partition_key(&DataType::Int64));
        assert!(!algorithm.supports_partition_key(&DataType::Float64));
    }

    #[test]
    fn bucket_hash_and_layout_have_distinct_closed_identities() {
        assert_eq!(
            PartitionHashAlgorithm::NativeBucketCrc32V1.stable_name(),
            "novarocks.native-bucket-crc32.v1"
        );
        assert_eq!(
            BucketLayoutAlgorithm::DenseZeroBasedV1.stable_name(),
            "novarocks.bucket-layout.dense-zero-based.v1"
        );
        assert_ne!(
            PartitionHashAlgorithm::NativeBucketCrc32V1,
            PartitionHashAlgorithm::NativeExchangeV1
        );
    }

    #[test]
    fn partition_space_identity_rejects_the_absent_identity() {
        assert_eq!(
            PartitionSpaceId::try_new([0; 32]),
            Err(PartitionSpaceIdentityError::Zero)
        );
        assert_eq!(
            PartitionSpaceId::try_new([7; 32]).unwrap().as_bytes(),
            [7; 32]
        );
        assert_eq!(
            PartitionCountParameterId::try_new([0; 32]),
            Err(PartitionCountParameterIdentityError::Zero)
        );
    }
}
