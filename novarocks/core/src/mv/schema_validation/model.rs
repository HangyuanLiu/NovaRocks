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

use crate::mv::analysis::rebind::RebindColumn;

#[derive(Clone, Debug)]
pub(crate) struct CurrentIcebergTableView<'a> {
    pub(crate) table_uuid: String,
    pub(crate) format_version: iceberg::spec::FormatVersion,
    pub(crate) row_lineage_enabled: bool,
    pub(crate) schema: &'a iceberg::spec::Schema,
    pub(crate) default_partition_spec: &'a iceberg::spec::PartitionSpec,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind { rebound_columns: Vec<RebindColumn> },
    Incompatible(SchemaEvolutionError),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SchemaEvolutionError {
    BaseTableIdentityChanged {
        expected: String,
        actual: String,
    },
    BaseRowLineageContractBroken {
        reason: String,
    },
    BaseFieldDropped {
        field_id: i32,
        name_at_create: String,
    },
    BaseFieldTypeChanged {
        field_id: i32,
        name_at_create: String,
        from: String,
        to: String,
    },
    BaseFieldNullabilityChanged {
        field_id: i32,
        name_at_create: String,
        from_required: bool,
        to_required: bool,
    },
    TargetTableIdentityChanged {
        expected: String,
        actual: String,
    },
    TargetRowLineageContractBroken {
        reason: String,
    },
    TargetVisibleFieldDropped {
        output_name: String,
        target_field_id: i32,
    },
    TargetVisibleFieldRenamed {
        target_field_id: i32,
        expected: String,
        actual: String,
    },
    TargetVisibleFieldTypeChanged {
        target_field_id: i32,
        from: String,
        to: String,
    },
    HiddenApplyKeyContractBroken {
        reason: String,
    },
    TargetPartitionSpecChanged {
        reason: String,
    },
    AggregateStateContractBroken {
        reason: String,
    },
}

impl std::fmt::Display for SchemaEvolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BaseTableIdentityChanged { expected, actual } => write!(
                f,
                "iceberg MV refresh blocked: base table identity changed (uuid expected={expected}, actual={actual}); run REFRESH FULL or recreate the MV"
            ),
            Self::BaseRowLineageContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: base table row-lineage contract broken ({reason}); run REFRESH FULL or recreate the MV"
            ),
            Self::BaseFieldDropped {
                field_id,
                name_at_create,
            } => write!(
                f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) was dropped from base table; run REFRESH FULL or recreate the MV"
            ),
            Self::BaseFieldTypeChanged {
                field_id,
                name_at_create,
                from,
                to,
            } => write!(
                f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) changed type from {from} to {to}; run REFRESH FULL or recreate the MV"
            ),
            Self::BaseFieldNullabilityChanged {
                field_id,
                name_at_create,
                from_required,
                to_required,
            } => write!(
                f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) changed nullability from required={from_required} to required={to_required}; run REFRESH FULL or recreate the MV"
            ),
            Self::TargetTableIdentityChanged { expected, actual } => write!(
                f,
                "iceberg MV refresh blocked: target table identity changed (uuid expected={expected}, actual={actual}); recreate the MV"
            ),
            Self::TargetRowLineageContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: target table row-lineage contract broken ({reason}); recreate the MV"
            ),
            Self::TargetVisibleFieldDropped {
                output_name,
                target_field_id,
            } => write!(
                f,
                "iceberg MV refresh blocked: target visible column \"{output_name}\" (field id {target_field_id}) was dropped; recreate the MV"
            ),
            Self::TargetVisibleFieldRenamed {
                target_field_id,
                expected,
                actual,
            } => write!(
                f,
                "iceberg MV refresh blocked: target visible column (field id {target_field_id}) renamed externally: expected \"{expected}\", actual \"{actual}\"; recreate the MV"
            ),
            Self::TargetVisibleFieldTypeChanged {
                target_field_id,
                from,
                to,
            } => write!(
                f,
                "iceberg MV refresh blocked: target visible column (field id {target_field_id}) changed type from {from} to {to}; recreate the MV"
            ),
            Self::HiddenApplyKeyContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: target hidden apply-key column contract broken ({reason}); recreate the MV"
            ),
            Self::TargetPartitionSpecChanged { reason } => write!(
                f,
                "iceberg MV refresh blocked: target partition spec changed externally ({reason}); recreate the MV"
            ),
            Self::AggregateStateContractBroken { reason } => write!(
                f,
                "iceberg MV refresh blocked: target aggregate state contract broken ({reason}); recreate the MV"
            ),
        }
    }
}

impl std::error::Error for SchemaEvolutionError {}
