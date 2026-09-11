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

//! MySQL scalar wire values.

use std::io::{self, Write};

use chrono::{NaiveDate, NaiveDateTime};
use mysql_common::value::Value as MySqlValue;
use opensrv_mysql::{Column, ToMysqlValue};

#[derive(Clone, Debug, PartialEq)]
pub enum MysqlResultValue {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

impl ToMysqlValue for MysqlResultValue {
    fn to_mysql_text<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::Null => None::<u8>.to_mysql_text(writer),
            Self::Bytes(bytes) => bytes.to_mysql_text(writer),
            Self::Int(value) => value.to_mysql_text(writer),
            Self::UInt(value) => value.to_mysql_text(writer),
            Self::Float(value) => value.to_mysql_text(writer),
            Self::Double(value) => value.to_mysql_text(writer),
            Self::Date(value) => value.to_mysql_text(writer),
            Self::DateTime(value) => value.to_mysql_text(writer),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_text(writer),
        }
    }

    fn to_mysql_bin<W: Write>(&self, writer: &mut W, column: &Column) -> io::Result<()> {
        match self {
            Self::Null => unreachable!("NULL payloads are handled by the row null bitmap"),
            Self::Bytes(bytes) => bytes.to_mysql_bin(writer, column),
            Self::Int(value) => value.to_mysql_bin(writer, column),
            Self::UInt(value) => value.to_mysql_bin(writer, column),
            Self::Float(value) => value.to_mysql_bin(writer, column),
            Self::Double(value) => value.to_mysql_bin(writer, column),
            Self::Date(value) => value.to_mysql_bin(writer, column),
            Self::DateTime(value) => value.to_mysql_bin(writer, column),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_bin(writer, column),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_time_uses_native_scalar_text_encoding() {
        let value = MysqlResultValue::Time {
            negative: false,
            days: 1,
            hours: 2,
            minutes: 3,
            seconds: 4,
            micros: 5,
        };
        let mut encoded = Vec::new();

        value.to_mysql_text(&mut encoded).expect("serialize time");

        assert_eq!(encoded, b"\x0f26:03:04.000005");
    }
}
