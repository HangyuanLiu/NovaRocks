#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::DictionarySnapshot;
use crate::sql::optimizer::operator::ScanOp;

#[derive(Clone, Debug, Default)]
pub(crate) struct RepresentationProperty {
    by_logical_column: BTreeMap<ColumnId, ColumnRepresentationSet>,
}

impl RepresentationProperty {
    pub(crate) fn from_scan(scan: &ScanOp) -> Self {
        let mut property = Self::default();
        for hint in &scan.dict_columns {
            let Some(physical_slot) = scan
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&hint.dict_column))
            else {
                continue;
            };

            property.insert(ColumnRepresentationSet {
                logical_column: LogicalColumn {
                    column_id: hint.source_column_id,
                    name: hint.source_column.clone(),
                    logical_type: hint.dictionary.data_type.clone(),
                    nullable: physical_slot.nullable,
                },
                current_slot: PhysicalSlot {
                    column_id: physical_slot.column_id,
                    name: physical_slot.name.clone(),
                    data_type: physical_slot.data_type.clone(),
                    nullable: physical_slot.nullable,
                },
                representations: vec![PhysicalRepresentation::DictInt32(DictInt32Representation {
                    domain: DictionaryDomain::from_snapshot(&hint.dictionary),
                })],
            });
        }
        property
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.by_logical_column.is_empty()
    }

    pub(crate) fn has_dictionary_representation(&self) -> bool {
        self.by_logical_column.values().any(|set| {
            set.representations.iter().any(|representation| {
                matches!(representation, PhysicalRepresentation::DictInt32(_))
            })
        })
    }

    pub(crate) fn get(&self, column_id: ColumnId) -> Option<&ColumnRepresentationSet> {
        self.by_logical_column.get(&column_id)
    }

    pub(crate) fn insert(&mut self, representation_set: ColumnRepresentationSet) {
        self.by_logical_column.insert(
            representation_set.logical_column.column_id,
            representation_set,
        );
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnRepresentationSet {
    pub logical_column: LogicalColumn,
    pub current_slot: PhysicalSlot,
    pub representations: Vec<PhysicalRepresentation>,
}

impl ColumnRepresentationSet {
    /// The dictionary representation carried by this column, if any.
    ///
    /// A set carries at most one `DictInt32` representation today, so returning
    /// the first match is unambiguous.
    pub(crate) fn dictionary_representation(&self) -> Option<&DictInt32Representation> {
        self.representations
            .iter()
            .find_map(|representation| match representation {
                PhysicalRepresentation::DictInt32(dict) => Some(dict),
                PhysicalRepresentation::Plain { .. } => None,
            })
    }

    /// Re-key this set to a new output logical column identity, preserving the
    /// underlying physical representation (the dictionary domain is unchanged).
    pub(crate) fn remapped_to_output(
        &self,
        output_column_id: ColumnId,
        output_name: &str,
        nullable: bool,
    ) -> Self {
        ColumnRepresentationSet {
            logical_column: LogicalColumn {
                column_id: output_column_id,
                name: output_name.to_string(),
                logical_type: self.logical_column.logical_type.clone(),
                nullable,
            },
            current_slot: PhysicalSlot {
                column_id: output_column_id,
                name: output_name.to_string(),
                data_type: self.current_slot.data_type.clone(),
                nullable,
            },
            representations: self.representations.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalColumn {
    pub column_id: ColumnId,
    pub name: String,
    pub logical_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalSlot {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum PhysicalRepresentation {
    Plain { logical_type: DataType },
    DictInt32(DictInt32Representation),
}

#[derive(Clone, Debug)]
pub(crate) struct DictInt32Representation {
    pub domain: DictionaryDomain,
}

#[derive(Clone, Debug)]
pub(crate) struct DictionaryDomain {
    pub dictionary_id: i64,
    pub owner_key: String,
    pub column_id: Option<i64>,
    pub column_name: String,
    pub logical_type: DataType,
    pub version: i64,
    pub watermark_json: String,
    pub null_id: i32,
    pub order_preserving: bool,
    pub snapshot: Arc<DictionarySnapshot>,
}

impl DictionaryDomain {
    pub(crate) fn from_snapshot(snapshot: &Arc<DictionarySnapshot>) -> Self {
        Self {
            dictionary_id: snapshot.dictionary_id,
            owner_key: snapshot.owner.stable_key(),
            column_id: snapshot.column_id,
            column_name: snapshot.column_name.clone(),
            logical_type: snapshot.data_type.clone(),
            version: snapshot.version,
            watermark_json: snapshot.watermark.stable_json(),
            null_id: snapshot.null_id,
            order_preserving: snapshot.order_preserving,
            snapshot: Arc::clone(snapshot),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_dictionary_snapshot(order_preserving: bool) -> Arc<DictionarySnapshot> {
    use crate::sql::common::{
        DictionaryOwner, DictionaryState, DictionaryValue, DictionaryWatermark,
    };
    Arc::new(DictionarySnapshot {
        dictionary_id: 7,
        owner: DictionaryOwner::StarRocksTable {
            database: "db".to_string(),
            table: "tbl".to_string(),
            db_id: 11,
            table_id: 13,
        },
        column_id: Some(17),
        column_name: "city".to_string(),
        data_type: DataType::Utf8,
        version: 19,
        watermark: DictionaryWatermark::Iceberg {
            snapshot_id: Some(23),
            schema_id: 29,
        },
        values: vec![DictionaryValue {
            id: 1,
            bytes: b"beijing".to_vec(),
        }],
        null_id: -1,
        state: DictionaryState::Active,
        order_preserving,
    })
}

/// A dictionary-backed representation set fixture. The column names/types
/// (`city` / `Utf8` / `city_dict` / `Int32`) are arbitrary test values; only
/// the column-id wiring and `order_preserving` flag are meaningful to callers.
#[cfg(test)]
pub(crate) fn test_dict_representation_set(
    logical_column_id: ColumnId,
    slot_column_id: ColumnId,
    order_preserving: bool,
) -> ColumnRepresentationSet {
    let snapshot = test_dictionary_snapshot(order_preserving);
    ColumnRepresentationSet {
        logical_column: LogicalColumn {
            column_id: logical_column_id,
            name: "city".to_string(),
            logical_type: DataType::Utf8,
            nullable: true,
        },
        current_slot: PhysicalSlot {
            column_id: slot_column_id,
            name: "city_dict".to_string(),
            data_type: DataType::Int32,
            nullable: true,
        },
        representations: vec![PhysicalRepresentation::DictInt32(DictInt32Representation {
            domain: DictionaryDomain::from_snapshot(&snapshot),
        })],
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::common::{DictionarySnapshot, OutputColumn, ScanDictionaryColumn};
    use crate::sql::optimizer::operator::ScanOp;

    #[test]
    fn empty_property_declares_no_representations() {
        let property = RepresentationProperty::default();
        assert!(property.is_empty());
        assert!(!property.has_dictionary_representation());
    }

    #[test]
    fn dictionary_domain_preserves_snapshot_identity() {
        let snapshot = test_snapshot();
        let domain = DictionaryDomain::from_snapshot(&snapshot);

        assert_eq!(domain.dictionary_id, snapshot.dictionary_id);
        assert_eq!(domain.owner_key, snapshot.owner.stable_key());
        assert_eq!(domain.version, snapshot.version);
        assert_eq!(domain.null_id, snapshot.null_id);
        assert_eq!(domain.logical_type, snapshot.data_type);
    }

    #[test]
    fn inserted_dictionary_representation_is_available_by_logical_column() {
        let snapshot = test_snapshot();
        let domain = DictionaryDomain::from_snapshot(&snapshot);
        let logical_column_id = ColumnId::new_for_test(5);
        let mut property = RepresentationProperty::default();

        property.insert(ColumnRepresentationSet {
            logical_column: LogicalColumn {
                column_id: logical_column_id,
                name: "city".to_string(),
                logical_type: DataType::Utf8,
                nullable: true,
            },
            current_slot: PhysicalSlot {
                column_id: ColumnId::new_for_test(6),
                name: "city_dict".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            representations: vec![PhysicalRepresentation::DictInt32(DictInt32Representation {
                domain,
            })],
        });

        let set = property
            .get(logical_column_id)
            .expect("representation exists");
        assert_eq!(set.logical_column.column_id, logical_column_id);
        assert_eq!(set.current_slot.name, "city_dict");
        assert!(property.has_dictionary_representation());
    }

    #[test]
    fn scan_without_dictionary_hints_has_empty_representation_property() {
        let scan = test_scan(
            vec![output_column(
                ColumnId::new_for_test(1),
                "city",
                DataType::Utf8,
                true,
            )],
            vec![],
        );

        let property = RepresentationProperty::from_scan(&scan);

        assert!(property.is_empty());
        assert!(!property.has_dictionary_representation());
    }

    #[test]
    fn scan_dictionary_hint_builds_dict_int32_representation_property() {
        let snapshot = test_snapshot();
        let source_column_id = ColumnId::new_for_test(5);
        let physical_column_id = ColumnId::new_for_test(6);
        let scan = test_scan(
            vec![output_column(
                physical_column_id,
                "__nr_dict_tbl_city",
                DataType::Int32,
                true,
            )],
            vec![ScanDictionaryColumn {
                source_column_id,
                source_column: "city".to_string(),
                dict_column: "__NR_DICT_TBL_CITY".to_string(),
                dictionary: Arc::clone(&snapshot),
            }],
        );

        let property = RepresentationProperty::from_scan(&scan);

        let set = property
            .get(source_column_id)
            .expect("dictionary representation exists");
        assert_eq!(set.logical_column.column_id, source_column_id);
        assert_eq!(set.logical_column.name, "city");
        assert_eq!(set.logical_column.logical_type, DataType::Utf8);
        assert!(set.logical_column.nullable);
        assert_eq!(set.current_slot.column_id, physical_column_id);
        assert_eq!(set.current_slot.name, "__nr_dict_tbl_city");
        assert_eq!(set.current_slot.data_type, DataType::Int32);
        assert!(set.current_slot.nullable);
        assert_eq!(set.representations.len(), 1);
        match &set.representations[0] {
            PhysicalRepresentation::DictInt32(dict) => {
                assert_eq!(dict.domain.dictionary_id, snapshot.dictionary_id);
                assert_eq!(dict.domain.owner_key, snapshot.owner.stable_key());
                assert_eq!(dict.domain.column_id, snapshot.column_id);
                assert_eq!(dict.domain.column_name, snapshot.column_name);
                assert_eq!(dict.domain.logical_type, snapshot.data_type);
                assert_eq!(dict.domain.version, snapshot.version);
                assert_eq!(dict.domain.watermark_json, snapshot.watermark.stable_json());
                assert_eq!(dict.domain.null_id, snapshot.null_id);
                assert_eq!(dict.domain.order_preserving, snapshot.order_preserving);
                assert!(Arc::ptr_eq(&dict.domain.snapshot, &snapshot));
            }
            other => panic!("expected DictInt32 representation, got {other:?}"),
        }
        assert!(property.has_dictionary_representation());
    }

    #[test]
    fn scan_dictionary_hint_missing_dict_output_column_is_ignored() {
        let snapshot = test_snapshot();
        let scan = test_scan(
            vec![output_column(
                ColumnId::new_for_test(5),
                "city",
                DataType::Utf8,
                true,
            )],
            vec![ScanDictionaryColumn {
                source_column_id: ColumnId::new_for_test(5),
                source_column: "city".to_string(),
                dict_column: "__nr_dict_tbl_city".to_string(),
                dictionary: snapshot,
            }],
        );

        let property = RepresentationProperty::from_scan(&scan);

        assert!(property.is_empty());
        assert!(!property.has_dictionary_representation());
    }

    fn output_column(
        column_id: ColumnId,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id,
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn test_scan(columns: Vec<OutputColumn>, dict_columns: Vec<ScanDictionaryColumn>) -> ScanOp {
        ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: "tbl".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 11,
                    table_id: 13,
                },
            },
            alias: None,
            stats_ref: None,
            columns,
            predicates: vec![],
            required_columns: None,
            dict_columns,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn test_snapshot() -> Arc<DictionarySnapshot> {
        // Single source of truth: an order-preserving snapshot fixture.
        super::test_dictionary_snapshot(true)
    }
}
