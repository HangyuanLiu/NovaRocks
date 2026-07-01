#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::DictionarySnapshot;

#[derive(Clone, Debug, Default)]
pub(crate) struct RepresentationProperty {
    by_logical_column: BTreeMap<ColumnId, ColumnRepresentationSet>,
}

impl RepresentationProperty {
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
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::common::{
        DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    };

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

    fn test_snapshot() -> Arc<DictionarySnapshot> {
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
            order_preserving: true,
        })
    }
}
