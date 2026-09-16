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

//! Exact execution evidence used to construct MV publication inputs.
//!
//! Optimizer rewrites may clone one logical source scan and may inject target
//! state or locator scans. Publication input construction therefore validates
//! equivalence classes of source scans and emits one entry per persisted D
//! occurrence instead of zipping D with physical plan nodes.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_mv_application::persistence::codec::{
    DefinitionDocument, PublicationInput, RelationOccurrence,
};
use novarocks_mv_application::persistence::identity::{NativeDataVersion, ObjectIdentity};
use novarocks_query_application::preparation::FrozenExecutionDescription;
use novarocks_sql::planning::query_execution::SqlScanPreparationCategory;

use novarocks_mv_application::persistence::exact_revision::persist_exact_query_revision;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LogicalOccurrenceKey {
    catalog: String,
    namespace: String,
    relation: String,
    qualifier: String,
}

impl LogicalOccurrenceKey {
    fn from_definition(occurrence: &RelationOccurrence) -> Self {
        Self {
            catalog: occurrence.catalog_at_binding.clone(),
            namespace: occurrence.namespace_at_binding.clone(),
            relation: occurrence.relation_at_binding.clone(),
            qualifier: occurrence.qualifier_at_binding.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicationScanEvidence {
    category: SqlScanPreparationCategory,
    key: LogicalOccurrenceKey,
    object_id: ObjectIdentity,
    native_data_version: NativeDataVersion,
}

/// Build P inputs from the exact bindings retained by the executed request.
///
/// Data scans are grouped only after their complete persisted semantic facts
/// compare equal. Injected target scans are excluded by their SQL-owned typed
/// category. Any other scan category fails closed because it has no approved
/// interpretation as a materialized-view source occurrence.
pub(crate) fn publication_inputs_from_execution(
    definition: &DefinitionDocument,
    execution: &FrozenExecutionDescription,
) -> Result<Vec<PublicationInput>, String> {
    let evidence = execution
        .scans()
        .iter()
        .filter_map(|scan| match scan.preparation_category() {
            SqlScanPreparationCategory::MvTargetState
            | SqlScanPreparationCategory::MvTargetLocator => None,
            category => Some((scan, category)),
        })
        .map(|(scan, category)| {
            if !matches!(
                category,
                SqlScanPreparationCategory::AdmittedData
                    | SqlScanPreparationCategory::AdmittedFrozenSnapshot
                    | SqlScanPreparationCategory::Delta
            ) {
                return Err(format!(
                    "MV publication input has unsupported scan category {category:?}"
                ));
            }
            let binding = scan.lineage().occurrence().binding();
            let object = binding.object_identity().ok_or_else(|| {
                "MV publication input scan has no exact object identity".to_string()
            })?;
            let data = binding
                .data_version()
                .ok_or_else(|| "MV publication input scan has no exact data version".to_string())?;
            let (object_id, native_data_version) =
                persist_exact_query_revision(object, data).map_err(|error| error.to_string())?;
            let occurrence = scan.logical_occurrence();
            Ok(PublicationScanEvidence {
                category,
                key: LogicalOccurrenceKey {
                    catalog: occurrence.catalog().to_string(),
                    namespace: occurrence.namespace().to_string(),
                    relation: occurrence.relation().to_string(),
                    qualifier: occurrence.qualifier().to_string(),
                },
                object_id,
                native_data_version,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    publication_inputs_from_evidence(definition, &evidence)
}

fn publication_inputs_from_evidence(
    definition: &DefinitionDocument,
    evidence: &[PublicationScanEvidence],
) -> Result<Vec<PublicationInput>, String> {
    let mut source_by_key = BTreeMap::new();
    for scan in evidence {
        match scan.category {
            SqlScanPreparationCategory::MvTargetState
            | SqlScanPreparationCategory::MvTargetLocator => continue,
            SqlScanPreparationCategory::AdmittedData
            | SqlScanPreparationCategory::AdmittedFrozenSnapshot
            | SqlScanPreparationCategory::Delta => {}
            category => {
                return Err(format!(
                    "MV publication input has unsupported scan category {category:?}"
                ));
            }
        }
        match source_by_key.get(&scan.key) {
            None => {
                source_by_key.insert(
                    scan.key.clone(),
                    (scan.object_id.clone(), scan.native_data_version.clone()),
                );
            }
            Some((object_id, native_data_version))
                if object_id == &scan.object_id
                    && native_data_version == &scan.native_data_version => {}
            Some(_) => {
                return Err(
                    "physical clones of one MV source occurrence have conflicting exact revisions"
                        .to_string(),
                );
            }
        }
    }

    let mut matched_keys = BTreeSet::new();
    let inputs = definition
        .relation_occurrences
        .iter()
        .map(|occurrence| {
            let key = LogicalOccurrenceKey::from_definition(occurrence);
            let (object_id, native_data_version) = source_by_key.get(&key).ok_or_else(|| {
                format!(
                    "MV definition occurrence {} has no exact executed source scan",
                    occurrence.occurrence_id
                )
            })?;
            if object_id != &occurrence.object_id {
                return Err(format!(
                    "MV definition occurrence {} resolved to another source object",
                    occurrence.occurrence_id
                ));
            }
            matched_keys.insert(key);
            Ok(PublicationInput {
                relation_occurrence_id: occurrence.occurrence_id,
                object_id: object_id.clone(),
                native_data_version: native_data_version.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    if matched_keys.len() != source_by_key.len() {
        return Err(
            "executed MV data plan contains a source absent from its definition".to_string(),
        );
    }
    Ok(inputs)
}

#[cfg(test)]
mod tests {
    use novarocks_mv_application::persistence::codec::{
        DefinitionDocument, QueryDialect, QuerySource, RelationOccurrence, ResolutionContext,
    };
    use novarocks_mv_application::persistence::identity::{
        ComputationIdentity, NativeDataVersion, ObjectIdentity, SchemaVersion,
    };

    use super::*;

    fn object(value: u8) -> ObjectIdentity {
        ObjectIdentity::try_new(vec![value]).unwrap()
    }

    fn version(value: u8) -> NativeDataVersion {
        NativeDataVersion::try_new(vec![value]).unwrap()
    }

    fn key(relation: &str, qualifier: &str) -> LogicalOccurrenceKey {
        LogicalOccurrenceKey {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            relation: relation.to_string(),
            qualifier: qualifier.to_string(),
        }
    }

    fn occurrence(id: u32, relation: &str, qualifier: &str, object_id: u8) -> RelationOccurrence {
        RelationOccurrence {
            occurrence_id: id,
            catalog_at_binding: "ice".to_string(),
            namespace_at_binding: "sales".to_string(),
            relation_at_binding: relation.to_string(),
            qualifier_at_binding: qualifier.to_string(),
            object_id: object(object_id),
            schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
            fields: Vec::new(),
        }
    }

    fn definition(occurrences: Vec<RelationOccurrence>) -> DefinitionDocument {
        DefinitionDocument {
            query: QuerySource {
                effective_sql: "SELECT 1".to_string(),
                dialect: QueryDialect::StarRocks,
                resolution: ResolutionContext {
                    default_catalog: "ice".to_string(),
                    default_namespace: "sales".to_string(),
                },
            },
            relation_occurrences: occurrences,
            outputs: Vec::new(),
            computation_identity: ComputationIdentity::from_canonical_bytes(b"definition"),
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn scan(
        category: SqlScanPreparationCategory,
        key: LogicalOccurrenceKey,
        object_id: u8,
        data_version: u8,
    ) -> PublicationScanEvidence {
        PublicationScanEvidence {
            category,
            key,
            object_id: object(object_id),
            native_data_version: version(data_version),
        }
    }

    #[test]
    fn cloned_join_scans_collapse_to_one_fact_per_definition_occurrence() {
        let definition = definition(vec![
            occurrence(8, "orders", "o", 1),
            occurrence(9, "customers", "c", 2),
        ]);
        let evidence = vec![
            scan(SqlScanPreparationCategory::Delta, key("orders", "o"), 1, 11),
            scan(
                SqlScanPreparationCategory::AdmittedFrozenSnapshot,
                key("customers", "c"),
                2,
                12,
            ),
            scan(
                SqlScanPreparationCategory::AdmittedFrozenSnapshot,
                key("orders", "o"),
                1,
                11,
            ),
            scan(
                SqlScanPreparationCategory::Delta,
                key("customers", "c"),
                2,
                12,
            ),
            scan(
                SqlScanPreparationCategory::MvTargetState,
                key("orders_mv", "orders_mv"),
                9,
                21,
            ),
            scan(
                SqlScanPreparationCategory::MvTargetLocator,
                key("orders_mv", "orders_mv"),
                9,
                21,
            ),
        ];

        let inputs = publication_inputs_from_evidence(&definition, &evidence).unwrap();

        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].relation_occurrence_id, 8);
        assert_eq!(inputs[0].native_data_version, version(11));
        assert_eq!(inputs[1].relation_occurrence_id, 9);
        assert_eq!(inputs[1].native_data_version, version(12));
    }

    #[test]
    fn repeated_unaliased_definition_occurrences_are_not_deduplicated() {
        let definition = definition(vec![
            occurrence(3, "orders", "orders", 1),
            occurrence(7, "orders", "orders", 1),
        ]);
        let evidence = vec![scan(
            SqlScanPreparationCategory::AdmittedData,
            key("orders", "orders"),
            1,
            20,
        )];

        let inputs = publication_inputs_from_evidence(&definition, &evidence).unwrap();

        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].relation_occurrence_id, 3);
        assert_eq!(inputs[1].relation_occurrence_id, 7);
    }

    #[test]
    fn rejects_conflicting_clones_object_drift_and_unapproved_categories() {
        let definition = definition(vec![occurrence(0, "orders", "o", 1)]);
        let conflicting = vec![
            scan(SqlScanPreparationCategory::Delta, key("orders", "o"), 1, 10),
            scan(SqlScanPreparationCategory::Delta, key("orders", "o"), 1, 11),
        ];
        assert!(publication_inputs_from_evidence(&definition, &conflicting).is_err());

        let changed_object = vec![scan(
            SqlScanPreparationCategory::AdmittedData,
            key("orders", "o"),
            2,
            10,
        )];
        assert!(publication_inputs_from_evidence(&definition, &changed_object).is_err());

        let metadata = vec![scan(
            SqlScanPreparationCategory::AdmittedMetadata,
            key("orders", "o"),
            1,
            10,
        )];
        assert!(publication_inputs_from_evidence(&definition, &metadata).is_err());
    }

    #[test]
    fn rejects_missing_and_unmatched_executed_sources() {
        let definition = definition(vec![occurrence(0, "orders", "o", 1)]);
        assert!(publication_inputs_from_evidence(&definition, &[]).is_err());

        let evidence = vec![
            scan(
                SqlScanPreparationCategory::AdmittedData,
                key("orders", "o"),
                1,
                10,
            ),
            scan(
                SqlScanPreparationCategory::AdmittedData,
                key("customers", "c"),
                2,
                20,
            ),
        ];
        assert!(publication_inputs_from_evidence(&definition, &evidence).is_err());
    }
}
