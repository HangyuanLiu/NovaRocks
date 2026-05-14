//! IVM-A11 MV schema / field-id contract.
//!
//! Persisted inside `StoredMvDefinition.schema_contract`. Captures base
//! referenced fields + output lineage + target schema mapping at CREATE
//! MV time. Validated on every REFRESH.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvSchemaContract {
    pub contract_version: u16,
    pub base: BaseContract,
    pub output: OutputContract,
    pub target: TargetContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseContract {
    pub table_fqn: String,
    pub table_uuid: String,
    pub schema_id_at_create: i32,
    pub schema_at_create: BaseSchemaSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSchemaSnapshot {
    pub fields: Vec<BaseFieldRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseFieldRecord {
    pub field_id: i32,
    pub name_at_create: String,
    pub type_signature: String,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputContract {
    pub columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputColumnLineage {
    pub expression: ExpressionLineage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpressionLineage {
    pub kind: ExpressionKind,
    pub referenced_base_field_ids: Vec<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExpressionKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterLineage {
    pub referenced_base_field_ids: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetContract {
    pub table_fqn: String,
    pub table_uuid: String,
    pub schema_id_at_create: i32,
    pub visible_columns: Vec<TargetVisibleColumn>,
    pub hidden_apply_key: HiddenApplyKeyContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetVisibleColumn {
    pub output_name: String,
    pub target_field_id: i32,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HiddenApplyKeyContract {
    pub column_name: String,
    pub target_field_id: i32,
    pub source: ApplyKeySource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKeySource {
    BaseRowId,
}

/// Errors returned by `MvSchemaContract::ensure_self_consistent`.
/// These indicate the contract was constructed incorrectly at CREATE
/// time — they should never surface to end users in practice.
#[derive(Debug, PartialEq, Eq)]
pub enum ContractSelfCheckError {
    OutputTargetLenMismatch {
        output_len: usize,
        target_len: usize,
    },
    HiddenApplyKeyColumnNameWrong {
        expected: String,
        actual: String,
    },
    OutputReferencesUnknownBaseFieldId {
        output_index: usize,
        field_id: i32,
    },
    FilterReferencesUnknownBaseFieldId {
        field_id: i32,
    },
    EmptyBaseTableUuid,
    NegativeBaseSchemaId(i32),
    DuplicateBaseFieldIdWithDifferentType {
        field_id: i32,
        first: String,
        second: String,
    },
}

impl std::fmt::Display for ContractSelfCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputTargetLenMismatch {
                output_len,
                target_len,
            } => {
                write!(
                    f,
                    "MV contract output columns ({output_len}) and target visible columns ({target_len}) must have the same length"
                )
            }
            Self::HiddenApplyKeyColumnNameWrong { expected, actual } => {
                write!(
                    f,
                    "MV contract hidden apply-key column name expected {expected}, got {actual}"
                )
            }
            Self::OutputReferencesUnknownBaseFieldId {
                output_index,
                field_id,
            } => {
                write!(
                    f,
                    "MV contract output column #{output_index} references base field id {field_id} that is not in base.schema_at_create"
                )
            }
            Self::FilterReferencesUnknownBaseFieldId { field_id } => {
                write!(
                    f,
                    "MV contract WHERE filter references base field id {field_id} that is not in base.schema_at_create"
                )
            }
            Self::EmptyBaseTableUuid => write!(f, "MV contract base.table_uuid is empty"),
            Self::NegativeBaseSchemaId(id) => {
                write!(f, "MV contract base.schema_id_at_create is negative: {id}")
            }
            Self::DuplicateBaseFieldIdWithDifferentType {
                field_id,
                first,
                second,
            } => {
                write!(
                    f,
                    "MV contract base.schema_at_create contains field id {field_id} twice with different type signatures: {first} vs {second}"
                )
            }
        }
    }
}

impl std::error::Error for ContractSelfCheckError {}

pub const HIDDEN_APPLY_KEY_COLUMN_NAME: &str = "__nova_base_row_id";

impl MvSchemaContract {
    /// Cheap structural self-check run at CREATE time. Does NOT consult
    /// the live Iceberg tables — that part lives in
    /// `validate_schema_contract` and runs at REFRESH time.
    pub fn ensure_self_consistent(&self) -> Result<(), ContractSelfCheckError> {
        if self.output.columns.len() != self.target.visible_columns.len() {
            return Err(ContractSelfCheckError::OutputTargetLenMismatch {
                output_len: self.output.columns.len(),
                target_len: self.target.visible_columns.len(),
            });
        }
        if self.target.hidden_apply_key.column_name != HIDDEN_APPLY_KEY_COLUMN_NAME {
            return Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong {
                expected: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                actual: self.target.hidden_apply_key.column_name.clone(),
            });
        }
        if self.base.table_uuid.is_empty() {
            return Err(ContractSelfCheckError::EmptyBaseTableUuid);
        }
        if self.base.schema_id_at_create < 0 {
            return Err(ContractSelfCheckError::NegativeBaseSchemaId(
                self.base.schema_id_at_create,
            ));
        }
        let known_field_ids: std::collections::BTreeSet<i32> = self
            .base
            .schema_at_create
            .fields
            .iter()
            .map(|f| f.field_id)
            .collect();
        for (i, col) in self.output.columns.iter().enumerate() {
            for fid in &col.expression.referenced_base_field_ids {
                if !known_field_ids.contains(fid) {
                    return Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId {
                        output_index: i,
                        field_id: *fid,
                    });
                }
            }
        }
        if let Some(filter) = &self.output.filter {
            for fid in &filter.referenced_base_field_ids {
                if !known_field_ids.contains(fid) {
                    return Err(ContractSelfCheckError::FilterReferencesUnknownBaseFieldId {
                        field_id: *fid,
                    });
                }
            }
        }
        let mut seen: std::collections::BTreeMap<i32, &str> = std::collections::BTreeMap::new();
        for f in &self.base.schema_at_create.fields {
            if let Some(prev) = seen.get(&f.field_id) {
                if *prev != f.type_signature.as_str() {
                    return Err(
                        ContractSelfCheckError::DuplicateBaseFieldIdWithDifferentType {
                            field_id: f.field_id,
                            first: prev.to_string(),
                            second: f.type_signature.clone(),
                        },
                    );
                }
            } else {
                seen.insert(f.field_id, &f.type_signature);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.ns.orders".to_string(),
                table_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                    },
                }],
                filter: None,
            },
            target: TargetContract {
                table_fqn: "ice.mv.orders_mv".to_string(),
                table_uuid: "22222222-2222-2222-2222-222222222222".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
            },
        }
    }

    #[test]
    fn contract_round_trips_through_serde_json() {
        let c = sample_contract();
        let json = serde_json::to_string(&c).expect("serialize");
        let decoded: MvSchemaContract = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, c);
    }

    #[test]
    fn self_check_accepts_well_formed_contract() {
        assert!(sample_contract().ensure_self_consistent().is_ok());
    }

    #[test]
    fn self_check_rejects_mismatched_output_and_target_lengths() {
        let mut c = sample_contract();
        c.target.visible_columns.push(TargetVisibleColumn {
            output_name: "extra".to_string(),
            target_field_id: 99,
            type_signature: "long".to_string(),
            nullable: true,
        });
        match c.ensure_self_consistent() {
            Err(ContractSelfCheckError::OutputTargetLenMismatch {
                output_len: 1,
                target_len: 2,
            }) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn self_check_rejects_wrong_hidden_column_name() {
        let mut c = sample_contract();
        c.target.hidden_apply_key.column_name = "wrong".to_string();
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong { .. })
        ));
    }

    #[test]
    fn self_check_rejects_unknown_referenced_field_id() {
        let mut c = sample_contract();
        c.output.columns[0].expression.referenced_base_field_ids = vec![999];
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId { field_id: 999, .. })
        ));
    }

    #[test]
    fn self_check_rejects_empty_base_uuid() {
        let mut c = sample_contract();
        c.base.table_uuid = String::new();
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::EmptyBaseTableUuid)
        ));
    }

    #[test]
    fn self_check_rejects_filter_referencing_unknown_field_id() {
        let mut c = sample_contract();
        c.output.filter = Some(FilterLineage {
            referenced_base_field_ids: vec![999],
        });
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::FilterReferencesUnknownBaseFieldId { field_id: 999 })
        ));
    }
}
