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

//! Complete immutable value types, independent of provider identity and encoding.

use arrow_schema::TimeUnit;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalValue {
    pub data_type: LogicalType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalField {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
}

/// Array and map child values deliberately have no bookkeeping name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    LargeInt,
    Float32,
    Float64,
    Decimal {
        bits: u16,
        precision: u8,
        scale: i8,
    },
    Utf8,
    Binary,
    FixedSizeBinary(u32),
    Uuid,
    Json,
    Bitmap,
    Hll,
    Object,
    Percentile,
    Variant,
    Date32,
    Date64,
    Time {
        bits: u8,
        unit: TimeUnit,
    },
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
    },
    Array {
        element: Box<LogicalValue>,
        fixed_length: Option<u32>,
    },
    Map {
        key: Box<LogicalValue>,
        value: Box<LogicalValue>,
    },
    Struct(Vec<LogicalField>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalTypeLimits {
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_text_bytes: usize,
}

impl Default for LogicalTypeLimits {
    fn default() -> Self {
        Self {
            max_depth: 64,
            max_nodes: 4096,
            max_text_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogicalTypeUsage {
    pub nodes: usize,
    pub text_bytes: usize,
}

impl LogicalType {
    /// Validate an already constructed tree without a recursive call stack.
    /// Decoders must additionally apply their limits before constructing it.
    pub fn validate(&self, limits: LogicalTypeLimits) -> Result<LogicalTypeUsage, String> {
        let mut pending = vec![(self, 1)];
        let mut usage = LogicalTypeUsage::default();
        while let Some((ty, depth)) = pending.pop() {
            if depth > limits.max_depth || usage.nodes >= limits.max_nodes {
                return Err("logical type exceeds its depth or node budget".into());
            }
            usage.nodes += 1;
            let children = match ty {
                Self::Array {
                    element,
                    fixed_length,
                } => {
                    if fixed_length.is_some_and(|n| n > i32::MAX as u32) {
                        return Err("logical array fixed length exceeds its carrier domain".into());
                    }
                    pending.push((&element.data_type, depth + 1));
                    1
                }
                Self::Map { key, value } => {
                    pending.push((&key.data_type, depth + 1));
                    pending.push((&value.data_type, depth + 1));
                    2
                }
                Self::Struct(fields) => {
                    if fields.len() > limits.max_nodes.saturating_sub(usage.nodes) {
                        return Err("logical struct exceeds its node budget".into());
                    }
                    let mut names = std::collections::HashSet::new();
                    for field in fields {
                        if field.name.is_empty() || !names.insert(field.name.as_str()) {
                            return Err(
                                "logical struct has an empty or duplicate field name".into()
                            );
                        }
                        usage.text_bytes = usage
                            .text_bytes
                            .checked_add(field.name.len())
                            .ok_or("logical type text budget overflow")?;
                        pending.push((&field.data_type, depth + 1));
                    }
                    fields.len()
                }
                Self::Decimal {
                    bits,
                    precision,
                    scale,
                } => {
                    let max = match bits {
                        32 => 9,
                        64 => 18,
                        128 => 38,
                        256 => 76,
                        _ => return Err("logical decimal has an invalid width".into()),
                    };
                    if *precision == 0 || *precision > max || *scale > *precision as i8 {
                        return Err("logical decimal has invalid precision or scale".into());
                    }
                    0
                }
                Self::Time { bits, unit } => {
                    if !matches!(
                        (bits, unit),
                        (32, TimeUnit::Second | TimeUnit::Millisecond)
                            | (64, TimeUnit::Microsecond | TimeUnit::Nanosecond)
                    ) {
                        return Err("logical time has an incompatible width and unit".into());
                    }
                    0
                }
                Self::Timestamp { timezone, .. } => {
                    if let Some(zone) = timezone {
                        if zone.is_empty() {
                            return Err("logical timestamp timezone is empty".into());
                        }
                        usage.text_bytes = usage
                            .text_bytes
                            .checked_add(zone.len())
                            .ok_or("logical type text budget overflow")?;
                    }
                    0
                }
                Self::FixedSizeBinary(size) if *size > i32::MAX as u32 => {
                    return Err("logical binary fixed length exceeds its carrier domain".into());
                }
                _ => 0,
            };
            if children > limits.max_nodes.saturating_sub(usage.nodes)
                || pending.len() > limits.max_nodes.saturating_sub(usage.nodes)
                || usage.text_bytes > limits.max_text_bytes
            {
                return Err("logical type exceeds its node or text budget".into());
            }
        }
        Ok(usage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_nullability_names_order_and_parameters_are_semantic() {
        let child = LogicalValue {
            data_type: LogicalType::Int64,
            nullable: false,
        };
        let required = LogicalType::Array {
            element: Box::new(child.clone()),
            fixed_length: None,
        };
        let optional = LogicalType::Array {
            element: Box::new(LogicalValue {
                nullable: true,
                ..child
            }),
            fixed_length: None,
        };
        assert_ne!(required, optional);
        let fields = vec![
            LogicalField {
                name: "a".into(),
                data_type: required,
                nullable: true,
            },
            LogicalField {
                name: "b".into(),
                data_type: optional,
                nullable: false,
            },
        ];
        let original = LogicalType::Struct(fields.clone());
        let mut reversed = fields;
        reversed.reverse();
        assert_ne!(original, LogicalType::Struct(reversed));
        assert!(original.validate(LogicalTypeLimits::default()).is_ok());
        assert_ne!(
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None
            },
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None
            }
        );
        assert_ne!(LogicalType::LargeInt, LogicalType::Uuid);
        assert_ne!(LogicalType::Uuid, LogicalType::FixedSizeBinary(16));
    }

    #[test]
    fn rejects_invalid_parameters_and_each_budget_without_recursive_traversal() {
        assert!(
            LogicalType::Decimal {
                bits: 128,
                precision: 0,
                scale: 0
            }
            .validate(LogicalTypeLimits::default())
            .is_err()
        );
        assert!(
            LogicalType::Time {
                bits: 32,
                unit: TimeUnit::Nanosecond
            }
            .validate(LogicalTypeLimits::default())
            .is_err()
        );
        let ty = LogicalType::Struct(vec![LogicalField {
            name: "wide_name".into(),
            data_type: LogicalType::Int32,
            nullable: true,
        }]);
        for limits in [
            LogicalTypeLimits {
                max_nodes: 1,
                ..Default::default()
            },
            LogicalTypeLimits {
                max_depth: 1,
                ..Default::default()
            },
            LogicalTypeLimits {
                max_text_bytes: 2,
                ..Default::default()
            },
        ] {
            assert!(ty.validate(limits).is_err());
        }
        let duplicate = LogicalType::Struct(vec![
            LogicalField {
                name: "x".into(),
                data_type: LogicalType::Int64,
                nullable: true
            };
            2
        ]);
        assert!(duplicate.validate(LogicalTypeLimits::default()).is_err());
    }
}
