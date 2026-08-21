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

use novarocks_types::schema::SqlType;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTableStmt {
    pub name: ObjectName,
    pub kind: CreateTableKind,
    /// Set to `true` when the SQL was `CREATE TABLE IF NOT EXISTS ...`.
    /// For CTAS, the engine skips table creation and data write when the
    /// target table already exists.
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CreateTableKind {
    Iceberg {
        columns: Vec<TableColumnDef>,
        key_desc: Option<TableKeyDesc>,
        bucket_count: Option<u32>,
        /// Columns named in `DISTRIBUTED BY HASH(...)`. Empty when no such
        /// clause was written (StarRocks then derives the distribution from
        /// the leading key columns). Used by StarRocks table DDL to reject
        /// BITMAP / HLL columns up front.
        distribution_columns: Vec<String>,
        partition_fields: Vec<IcebergPartitionFieldExpr>,
        properties: Vec<(String, String)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergPartitionFieldExpr {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterIcebergPartitionSpecStmt {
    AddPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
    DropPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
    pub aggregation: Option<ColumnAggregation>,
    pub default: Option<DefaultLiteral>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableKeyDesc {
    pub kind: TableKeyKind,
    pub columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKeyKind {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnAggregation {
    Sum,
    Min,
    Max,
    Replace,
    /// `REPLACE_IF_NOT_NULL` — replace the existing value with the incoming
    /// value, but only when the incoming value is non-NULL. NULL inserts are
    /// silently ignored.
    ReplaceIfNotNull,
    BitmapUnion,
    HllUnion,
}

/// Literal that may appear in `DEFAULT <literal>` clauses for Iceberg v3
/// columns.  `Null` is the sentinel for `DEFAULT NULL` and is NOT persisted
/// into the Iceberg metadata; it only suppresses duplicate-DEFAULT diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub enum DefaultLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal { unscaled: i128, scale: i8 },
    String(String),
    Date(i32),     // days since 1970-01-01
    DateTime(i64), // microseconds since 1970-01-01T00:00:00Z
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName {
    pub parts: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColumnRef {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    Column(ColumnRef),
    Literal(Literal),
    Arithmetic {
        left: Box<Expr>,
        op: ArithmeticOp,
        right: Box<Expr>,
    },
    ScalarFunction(ScalarFunctionExpr),
    Array(Vec<Expr>),
    Cast {
        expr: Box<Expr>,
        data_type: SqlType,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScalarFunctionExpr {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<Literal>),
    Map(Vec<(Literal, Literal)>),
    Struct(Vec<Literal>),
}
