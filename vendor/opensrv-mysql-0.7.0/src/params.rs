// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::myc;
use crate::{StatementData, Value};

/// A `ParamParser` decodes query parameters included in a client's `EXECUTE` command given
/// type information for the expected parameters.
///
/// Users should invoke [`iter`](struct.ParamParser.html#method.iter) method to iterate over the
/// provided parameters.
pub struct ParamParser<'a> {
    pub(crate) params: u16,
    nullmap: &'a [u8],
    pub(crate) bytes: &'a [u8],
    pub(crate) long_data: &'a crate::input::LongData,
    pub(crate) bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

impl<'a> ParamParser<'a> {
    pub(crate) fn new(input: &'a [u8], stmt: &'a mut StatementData) -> std::io::Result<Self> {
        let invalid = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid prepared parameter payload",
            )
        };
        let count = stmt.params as usize;
        if count > crate::ProtocolLimits::default().columns {
            return Err(invalid());
        }
        if count == 0 {
            if !input.is_empty() {
                return Err(invalid());
            }
            return Ok(Self {
                params: 0,
                bytes: input,
                nullmap: &[],
                long_data: &stmt.long_data,
                bound_types: &mut stmt.bound_types,
            });
        }
        let (nullmap, rest) = input
            .split_at_checked(count.div_ceil(8))
            .ok_or_else(invalid)?;
        let (&flag, mut values) = rest.split_first().ok_or_else(invalid)?;
        match flag {
            1 => {
                let (types, rest) = values.split_at_checked(2 * count).ok_or_else(invalid)?;
                for pair in types.chunks_exact(2) {
                    myc::constants::ColumnType::try_from(pair[0]).map_err(|_| invalid())?;
                }
                stmt.bound_types.clear();
                stmt.bound_types.try_reserve_exact(count).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::OutOfMemory,
                        "parameter type allocation failed",
                    )
                })?;
                for pair in types.chunks_exact(2) {
                    stmt.bound_types.push((
                        myc::constants::ColumnType::try_from(pair[0]).map_err(|_| invalid())?,
                        pair[1] & 128 != 0,
                    ));
                }
                values = rest;
            }
            0 if stmt.bound_types.len() == count => {}
            _ => return Err(invalid()),
        }
        let bytes = values;
        for (index, &(kind, unsigned)) in stmt.bound_types.iter().enumerate() {
            if nullmap[index / 8] & (1 << (index % 8)) == 0 {
                if stmt.long_data.contains_key(&(index as u16)) {
                    use crate::ColumnType::*;
                    if !matches!(
                        kind,
                        MYSQL_TYPE_STRING
                            | MYSQL_TYPE_VAR_STRING
                            | MYSQL_TYPE_VARCHAR
                            | MYSQL_TYPE_BLOB
                            | MYSQL_TYPE_TINY_BLOB
                            | MYSQL_TYPE_MEDIUM_BLOB
                            | MYSQL_TYPE_LONG_BLOB
                            | MYSQL_TYPE_SET
                            | MYSQL_TYPE_ENUM
                            | MYSQL_TYPE_DECIMAL
                            | MYSQL_TYPE_NEWDECIMAL
                            | MYSQL_TYPE_BIT
                            | MYSQL_TYPE_GEOMETRY
                            | MYSQL_TYPE_JSON
                    ) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "long data is incompatible with parameter type",
                        ));
                    }
                } else {
                    validate_temporal_parameter(Value::parse_from(&mut values, kind, unsigned)?)?;
                }
            }
        }
        if !values.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            params: stmt.params,
            bytes,
            nullmap,
            long_data: &stmt.long_data,
            bound_types: &mut stmt.bound_types,
        })
    }
}

impl<'a> IntoIterator for ParamParser<'a> {
    type IntoIter = Params<'a>;
    type Item = ParamValue<'a>;
    fn into_iter(self) -> Params<'a> {
        Params {
            params: self.params,
            input: self.bytes,
            nullmap: Some(self.nullmap),
            col: 0,
            long_data: self.long_data,
            bound_types: self.bound_types,
        }
    }
}

/// An iterator over parameters provided by a client in an `EXECUTE` command.
pub struct Params<'a> {
    params: u16,
    input: &'a [u8],
    nullmap: Option<&'a [u8]>,
    col: u16,
    long_data: &'a crate::input::LongData,
    bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

/// A single parameter value provided by a client when issuing an `EXECUTE` command.
pub struct ParamValue<'a> {
    /// The value provided for this parameter.
    pub value: Value<'a>,
    /// The column type assigned to this parameter.
    pub coltype: myc::constants::ColumnType,
}

impl<'a> Iterator for Params<'a> {
    type Item = ParamValue<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.col >= self.params {
            return None;
        }
        let pt = self.bound_types.get(self.col as usize)?;

        // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
        // NULL-bitmap-byte = ((field-pos + offset) / 8)
        // NULL-bitmap-bit  = ((field-pos + offset) % 8)
        if let Some(nullmap) = self.nullmap {
            let byte = self.col as usize / 8;
            if byte >= nullmap.len() {
                return None;
            }
            if (nullmap[byte] & 1u8 << (self.col % 8)) != 0 {
                self.col += 1;
                return Some(ParamValue {
                    value: Value::null(),
                    coltype: pt.0,
                });
            }
        } else {
            return None;
        }

        let v = if let Some(data) = self.long_data.get(&self.col) {
            Value::bytes(&data[..])
        } else {
            Value::parse_from(&mut self.input, pt.0, pt.1).ok()?
        };
        self.col += 1;
        Some(ParamValue {
            value: v,
            coltype: pt.0,
        })
    }
}

/// Prepared parameters must be safe for the existing infallible temporal
/// conversion APIs before a shim can observe them. No zero date is invented.
fn validate_temporal_parameter(value: Value<'_>) -> std::io::Result<()> {
    let invalid = |message| std::io::Error::new(std::io::ErrorKind::InvalidData, message);
    match value.into_inner() {
        crate::ValueInner::Date(bytes) => {
            if !matches!(bytes.len(), 0 | 4) {
                return Err(invalid("invalid DATE parameter length"));
            }
            if bytes.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "wire-valid zero DATE is unsupported by temporal conversions",
                ));
            }
            let year = u16::from_le_bytes([bytes[0], bytes[1]]) as i32;
            chrono::NaiveDate::from_ymd_opt(year, bytes[2] as u32, bytes[3] as u32)
                .ok_or_else(|| invalid("invalid DATE parameter content"))?;
        }
        crate::ValueInner::Datetime(bytes) => {
            if !matches!(bytes.len(), 0 | 4 | 7 | 11) {
                return Err(invalid("invalid DATETIME parameter length"));
            }
            if bytes.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "wire-valid zero DATETIME is unsupported by temporal conversions",
                ));
            }
            if bytes.len() >= 7 && (bytes[4] >= 24 || bytes[5] >= 60 || bytes[6] >= 60) {
                return Err(invalid("invalid DATETIME parameter content"));
            }
            if bytes.len() == 11
                && u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]) >= 1_000_000
            {
                return Err(invalid("invalid DATETIME parameter microseconds"));
            }
            crate::to_naive_datetime(value)?;
        }
        crate::ValueInner::Time(bytes) => {
            if !matches!(bytes.len(), 0 | 8 | 12) {
                return Err(invalid("invalid TIME parameter length"));
            }
            if !bytes.is_empty() {
                if bytes[0] == 1 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "negative TIME parameters are unsupported",
                    ));
                }
                if bytes[0] != 0 {
                    return Err(invalid("invalid TIME parameter sign"));
                }
                if bytes[5] >= 24 || bytes[6] >= 60 || bytes[7] >= 60 {
                    return Err(invalid("invalid TIME parameter content"));
                }
                if bytes.len() == 12
                    && u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) >= 1_000_000
                {
                    return Err(invalid("invalid TIME parameter microseconds"));
                }
            }
        }
        _ => {}
    }
    Ok(())
}
