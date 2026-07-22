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

use arrow::datatypes::Field;

pub(crate) const NR_LOGICAL_TYPE_KEY: &str = "nr_logical_type";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogicalType {
    Json,
    Hll,
    Bitmap,
    Object,
    Percentile,
}

impl LogicalType {
    pub(crate) fn metadata_value(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Hll => "hll",
            Self::Bitmap => "bitmap",
            Self::Object => "object",
            Self::Percentile => "percentile",
        }
    }

    pub(crate) fn from_metadata_value(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "json" => Some(Self::Json),
            "hll" => Some(Self::Hll),
            "bitmap" => Some(Self::Bitmap),
            "object" => Some(Self::Object),
            "percentile" => Some(Self::Percentile),
            _ => None,
        }
    }
}

pub fn logical_type_of_field(field: &Field) -> Option<LogicalType> {
    field
        .metadata()
        .get(NR_LOGICAL_TYPE_KEY)
        .and_then(|value| LogicalType::from_metadata_value(value))
}

pub fn field_with_logical_type(field: Field, logical_type: LogicalType) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        NR_LOGICAL_TYPE_KEY.to_string(),
        logical_type.metadata_value().to_string(),
    );
    field.with_metadata(metadata)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn json_metadata_round_trips_through_field() {
        let field = field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        );

        assert_eq!(
            field
                .metadata()
                .get(NR_LOGICAL_TYPE_KEY)
                .map(String::as_str),
            Some("json")
        );
        assert_eq!(logical_type_of_field(&field), Some(LogicalType::Json));
    }

    #[test]
    fn hll_and_object_metadata_are_recognized() {
        let hll = Field::new("hll", DataType::Binary, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.to_string(), "hll".to_string())].into());
        let object = Field::new("object", DataType::LargeBinary, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.to_string(), "object".to_string())].into());

        assert_eq!(logical_type_of_field(&hll), Some(LogicalType::Hll));
        assert_eq!(logical_type_of_field(&object), Some(LogicalType::Object));
    }

    #[test]
    fn field_without_logical_metadata_has_no_logical_type() {
        let field = Field::new("plain", DataType::Utf8, true);

        assert_eq!(logical_type_of_field(&field), None);
    }
}
