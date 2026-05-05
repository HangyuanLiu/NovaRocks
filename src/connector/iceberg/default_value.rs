// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Default value helpers shared by DDL, schema transport, parquet read path,
//! and INSERT write path.

use iceberg::spec::{Literal as IcebergLiteral, PrimitiveLiteral};

use crate::sql::parser::ast::{DefaultLiteral, SqlType};

/// Convert an AST `DefaultLiteral` to an `iceberg::spec::Literal` validated
/// against the column's SqlType.  Returns `Ok(None)` for `DefaultLiteral::Null`
/// (which is not persisted) and `Err` when the literal does not fit the
/// column's type or the type itself is unsupported.
pub(crate) fn default_literal_to_iceberg(
    literal: &DefaultLiteral,
    column_type: &SqlType,
) -> Result<Option<IcebergLiteral>, String> {
    if matches!(literal, DefaultLiteral::Null) {
        return Ok(None);
    }
    let prim = match (literal, column_type) {
        (DefaultLiteral::Bool(b), SqlType::Boolean) => PrimitiveLiteral::Boolean(*b),
        (DefaultLiteral::Int(v), SqlType::TinyInt) => {
            i8::try_from(*v).map_err(|_| out_of_range("TINYINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::SmallInt) => {
            i16::try_from(*v).map_err(|_| out_of_range("SMALLINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::Int) => {
            i32::try_from(*v).map_err(|_| out_of_range("INT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::BigInt) => PrimitiveLiteral::Long(*v),
        (DefaultLiteral::Float(v), SqlType::Float) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(*v as f32))
        }
        (DefaultLiteral::Float(v), SqlType::Double) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(*v))
        }
        (
            DefaultLiteral::Decimal { unscaled, scale },
            SqlType::Decimal {
                scale: col_scale, ..
            },
        ) => {
            if *scale != *col_scale {
                return Err(format!(
                    "DEFAULT value scale {scale} does not match column scale {col_scale}"
                ));
            }
            PrimitiveLiteral::Int128(*unscaled)
        }
        (DefaultLiteral::String(s), SqlType::String) => PrimitiveLiteral::String(s.clone()),
        (DefaultLiteral::Date(d), SqlType::Date) => PrimitiveLiteral::Int(*d),
        (DefaultLiteral::DateTime(t), SqlType::DateTime) => PrimitiveLiteral::Long(*t),
        (DefaultLiteral::Binary(b), SqlType::Binary) => PrimitiveLiteral::Binary(b.clone()),
        (lit, ty) => {
            return Err(format!(
                "DEFAULT value type does not match column type: literal={lit:?} column={ty:?}"
            ));
        }
    };
    Ok(Some(IcebergLiteral::Primitive(prim)))
}

fn out_of_range(type_name: &str, value: i64) -> String {
    format!("DEFAULT value {value} is out of range for {type_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_default_round_trips() {
        let lit = default_literal_to_iceberg(&DefaultLiteral::Bool(true), &SqlType::Boolean)
            .expect("bool default")
            .expect("not null");
        assert!(matches!(
            lit,
            IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(true))
        ));
    }

    #[test]
    fn int_overflow_rejected_for_tinyint() {
        let err = default_literal_to_iceberg(&DefaultLiteral::Int(200), &SqlType::TinyInt)
            .expect_err("overflow");
        assert!(err.contains("TINYINT"));
    }

    #[test]
    fn decimal_scale_mismatch_rejected() {
        let err = default_literal_to_iceberg(
            &DefaultLiteral::Decimal {
                unscaled: 1234,
                scale: 3,
            },
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect_err("scale mismatch");
        assert!(err.contains("scale"));
    }

    #[test]
    fn null_returns_none() {
        let lit =
            default_literal_to_iceberg(&DefaultLiteral::Null, &SqlType::Int).expect("null default");
        assert!(lit.is_none());
    }

    #[test]
    fn type_mismatch_rejected() {
        let err = default_literal_to_iceberg(&DefaultLiteral::String("x".into()), &SqlType::Int)
            .expect_err("type mismatch");
        assert!(err.contains("type does not match"));
    }
}
