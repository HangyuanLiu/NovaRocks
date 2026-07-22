#![allow(dead_code)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PrimitiveType {
    Invalid,
    Null,
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Int256,
    Float,
    Double,
    Date,
    DateTime,
    Time,
    Decimal,
    DecimalV2,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Char,
    Varchar,
    Binary,
    Varbinary,
    Json,
    Hll,
    Object,
    Percentile,
    Function,
    Variant,
}

impl PrimitiveType {
    pub fn is_opaque_binary(self) -> bool {
        matches!(self, Self::Hll | Self::Object | Self::Percentile)
    }

    pub fn is_json(self) -> bool {
        matches!(self, Self::Json)
    }

    pub fn is_largeint(self) -> bool {
        matches!(self, Self::LargeInt)
    }

    pub fn is_time(self) -> bool {
        matches!(self, Self::Time)
    }
}

#[cfg(test)]
mod tests {
    use super::PrimitiveType;

    #[test]
    fn primitive_type_marks_opaque_binary_family() {
        assert!(PrimitiveType::Hll.is_opaque_binary());
        assert!(PrimitiveType::Object.is_opaque_binary());
        assert!(PrimitiveType::Percentile.is_opaque_binary());
        assert!(!PrimitiveType::Varbinary.is_opaque_binary());
    }

    #[test]
    fn primitive_type_classifies_rendering_helpers() {
        assert!(PrimitiveType::Json.is_json());
        assert!(PrimitiveType::LargeInt.is_largeint());
        assert!(PrimitiveType::Time.is_time());
        assert!(!PrimitiveType::Int256.is_largeint());
        assert!(!PrimitiveType::Int256.is_json());
        assert!(!PrimitiveType::Int256.is_time());
    }
}
