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

//! Generic terminal connector writer boundary.
//!
//! This module deliberately contains only Arrow and SQL planner facts.  The
//! application resolves a [`SqlWritePlanInput`] through its request-local
//! binding token and attaches a concrete connector writer after placement.

use std::sync::Arc;

#[cfg(test)]
use arrow::datatypes::Field;
use arrow::datatypes::{Schema, SchemaRef};
use novarocks_types::schema::ColumnDef;

use crate::analysis::TypedExpr;
use crate::planner::distributed::write::contract::{ConnectorWriteInputBinding, SqlWritePlanInput};

/// Provider-neutral input for a distributed connector writer. It contains
/// only the Arrow contract between the terminal fragment and the BE-local
/// writer; concrete providers keep target metadata in their opaque handle.
#[derive(Clone, Debug)]
pub(crate) struct ConnectorWritePlanInput {
    pub(crate) target_schema: SchemaRef,
    pub(crate) input: ConnectorWriteInputBinding,
    /// A root-only projection supplied by SQL when the physical stream needs
    /// to materialize hidden or state columns immediately before the sink.
    /// The expression contract is generic planner data, never provider data.
    pub(crate) root_output_exprs: Option<Vec<TypedExpr>>,
}

impl ConnectorWritePlanInput {
    /// The writer input schema, one field per input column.
    ///
    /// Each field is materialized from its column's declared value type, so a
    /// root logical identity (LARGEINT, HLL, BITMAP, UUID, VARIANT, ...) stays
    /// on the field the statistics pins are checked against. A plain carrier
    /// column materializes as the same unannotated field it always did.
    pub(crate) fn target_schema_from_sql_write_plan_input(
        sink: &SqlWritePlanInput,
    ) -> Result<SchemaRef, String> {
        let fields = sink
            .contract
            .input_columns
            .iter()
            .map(writer_input_field)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    #[cfg(test)]
    pub(crate) fn from_target_columns(
        target_columns: &[ColumnDef],
        input: ConnectorWriteInputBinding,
        root_output_exprs: Option<Vec<TypedExpr>>,
    ) -> Self {
        let fields = target_columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>();
        Self {
            target_schema: Arc::new(Schema::new(fields)),
            input,
            root_output_exprs,
        }
    }

    /// Project the sealed SQL write contract into the generic Arrow sink
    /// boundary. This consumes no provider metadata: the binding token stays
    /// in the SQL contract for application-side writer registration.
    pub(crate) fn from_sql_write_plan_input(sink: SqlWritePlanInput) -> Result<Self, String> {
        let target_schema = Self::target_schema_from_sql_write_plan_input(&sink)?;
        Ok(Self {
            target_schema,
            input: sink.input,
            root_output_exprs: sink.root_output_exprs,
        })
    }
}

/// Materialize one writer input column as the Arrow field its declared value
/// type authors.
fn writer_input_field(column: &ColumnDef) -> Result<arrow::datatypes::Field, String> {
    column
        .declared_value_type()
        .map_err(|error| format!("write input column `{}`: {error}", column.name))?
        .try_to_field(column.name.clone())
        .map_err(|error| format!("write input column `{}`: {error}", column.name))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_connector_iceberg_functions::{
        ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration,
    };
    use novarocks_functions::EngineFunctionCatalogBuilder;
    use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
    use novarocks_spi::connector::{
        StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
    };
    use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
    use novarocks_types::schema::{ColumnDef, SqlType};

    use super::ConnectorWritePlanInput;
    use crate::compiler::SqlFunctionCatalog;
    use crate::planner::distributed::write::auxiliary::{
        WriterStatisticsTargetInput, plan_writer_statistics,
    };
    use crate::planner::distributed::write::contract::ConnectorWriteInputBinding;
    use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;

    fn column(
        name: &str,
        data_type: DataType,
        nullable: bool,
        logical_type: Option<SqlType>,
    ) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type,
        }
    }

    fn sink_with_input_columns(
        columns: Vec<ColumnDef>,
    ) -> crate::planner::distributed::write::contract::SqlWritePlanInput {
        let mut sink = simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal);
        sink.contract.input_columns = columns;
        sink
    }

    fn theta_catalog() -> Arc<dyn SqlFunctionCatalog> {
        let registration = iceberg_theta_registration().expect("theta registration");
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(registration.definition().clone())
            .expect("register theta");
        Arc::new(builder.seal_bound().expect("seal catalog"))
    }

    fn largeint_pin(ordinal: usize, name: &str) -> StatisticsRequiredAggregation {
        StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(
                ordinal,
                name,
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                )
                .expect("LARGEINT value type"),
            )
            .expect("pinned input"),
            ICEBERG_THETA_AGGREGATE_NAME,
            StatisticsArtifactIdentity::try_new(vec![6], "test-largeint-pin-v1")
                .expect("artifact identity"),
        )
        .expect("requirement")
    }

    fn plan_statistics(
        schema: &Schema,
        requirements: &[StatisticsRequiredAggregation],
    ) -> Result<(), String> {
        plan_writer_statistics(
            &[WriterStatisticsTargetInput {
                target: WriteTargetOrdinal::try_new(0).expect("target"),
                input_schema: schema,
                requirements,
            }],
            theta_catalog().as_ref(),
            DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
    }

    /// A plain carrier column materializes exactly the unannotated field it
    /// always did, and each declared root identity survives as the field's
    /// logical annotation.
    #[test]
    fn writer_input_schema_keeps_root_logical_identities_and_plain_carriers() {
        let sink = sink_with_input_columns(vec![
            column("id", DataType::Int64, false, None),
            column(
                "big",
                DataType::FixedSizeBinary(16),
                true,
                Some(SqlType::LargeInt),
            ),
            column("h", DataType::Binary, true, Some(SqlType::Hll)),
            column("b", DataType::Binary, true, Some(SqlType::Bitmap)),
            column(
                "u",
                DataType::FixedSizeBinary(16),
                true,
                Some(SqlType::Uuid),
            ),
            column("v", DataType::LargeBinary, true, Some(SqlType::Variant)),
        ]);
        let schema = ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink)
            .expect("writer input schema");
        assert_eq!(
            schema.field(0).as_ref(),
            &Field::new("id", DataType::Int64, false)
        );
        for (ordinal, expected) in [
            (1, ValueLogicalType::LargeInt),
            (2, ValueLogicalType::Hll),
            (3, ValueLogicalType::Bitmap),
            (4, ValueLogicalType::Uuid),
            (5, ValueLogicalType::Variant),
        ] {
            let field = schema.field(ordinal);
            let value_type = FunctionValueType::try_from_field(field).expect("field value type");
            assert_eq!(value_type.logical_type, expected, "{}", field.name());
            assert_eq!(
                field.data_type(),
                &sink.contract.input_columns[ordinal].data_type
            );
        }
    }

    /// A LARGEINT column pinned by a collect-on-write statistics requirement
    /// plans against the writer input: the pin names the column's own value
    /// type, LargeInt included. A writer input that dropped the declaration
    /// is exactly what the pin check refuses.
    #[test]
    fn largeint_statistics_pin_plans_against_the_writer_input() {
        let sink = sink_with_input_columns(vec![
            column("id", DataType::Int64, false, None),
            column(
                "big",
                DataType::FixedSizeBinary(16),
                false,
                Some(SqlType::LargeInt),
            ),
        ]);
        let schema = ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink)
            .expect("writer input schema");
        let requirements = vec![largeint_pin(1, "big")];
        plan_statistics(&schema, &requirements).expect("LARGEINT pin plans");

        let undeclared = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("big", DataType::FixedSizeBinary(16), false),
        ]);
        let error = plan_statistics(&undeclared, &requirements)
            .expect_err("an undeclared carrier contradicts the pin");
        assert!(
            error.contains("does not match the pinned column"),
            "{error}"
        );
    }

    /// A declaration the carrier cannot hold is refused instead of being
    /// materialized as some other field.
    #[test]
    fn writer_input_schema_refuses_a_declaration_its_carrier_cannot_hold() {
        let sink = sink_with_input_columns(vec![column(
            "big",
            DataType::Int64,
            false,
            Some(SqlType::LargeInt),
        )]);
        let error = ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink)
            .expect_err("LARGEINT over Int64 is refused");
        assert!(error.contains("write input column `big`"), "{error}");
    }
}
