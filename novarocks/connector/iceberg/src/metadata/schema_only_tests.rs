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

// Functional tests for the private existing-serializer projection.
use super::*;
use crate::iceberg::spec::{
    FormatVersion, ListType, Literal, Map, MapType, NestedField, Operation, PartitionSpec,
    PrimitiveType, Schema, Snapshot, SortOrder, Summary, TableMetadataBuilder, Type,
};
use novarocks_spi::connector::{ConnectorErrorKind, ConnectorInstanceId};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Witness(Arc<AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Scope {
    limit: u64,
    stop: AtomicBool,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope for Scope {
    fn check_active(&self) -> Result<(), ConnectorError> {
        if self.stop.load(Ordering::SeqCst) {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "original stopped projection",
            ))
        } else {
            Ok(())
        }
    }
    fn check_total(&self, total: u64) -> Result<(), ConnectorError> {
        if total > self.limit {
            Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "original projection capacity refused",
            ))
        } else {
            Ok(())
        }
    }
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        assert!(
            !self.stop.load(Ordering::SeqCst),
            "stopped builder entered the owned handoff"
        );
        self.guard.clone()
    }
}
fn scope(limit: u64) -> (Scope, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Scope {
            limit,
            stop: AtomicBool::new(false),
            guard: ConnectorPayloadRetentionGuard::new(Witness(drops.clone())),
        },
        drops,
    )
}
fn metadata(version: FormatVersion) -> TableMetadata {
    let map = Type::Map(MapType {
        key_field: Arc::new(NestedField::map_key_element(2, PrimitiveType::Int.into())),
        value_field: Arc::new(
            NestedField::map_value_element(3, PrimitiveType::String.into(), false)
                .with_initial_default(Literal::string("synthetic map child")),
        ),
    });
    let default = Literal::Map(Map::from([
        (
            Literal::int(9),
            Some(Literal::string("nine\nquoted \"name\"")),
        ),
        (Literal::int(1), None),
    ]));
    let list = Type::List(ListType {
        element_field: Arc::new(
            NestedField::list_element(6, PrimitiveType::Long.into(), false)
                .with_initial_default(Literal::long(7))
                .with_write_default(Literal::long(8)),
        ),
    });
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(
                NestedField::optional(1, "attrs", map)
                    .with_initial_default(default.clone())
                    .with_write_default(default),
            ),
            Arc::new(
                NestedField::optional(4, "nan", PrimitiveType::Double.into())
                    .with_initial_default(Literal::double(f64::NAN)),
            ),
            Arc::new(NestedField::optional(5, "items", list)),
        ])
        .build()
        .expect("valid small stock schema");
    let base = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://warehouse/db/defaults".to_string(),
        version,
        Default::default(),
    )
    .expect("stock metadata builder")
    .build()
    .expect("stock metadata")
    .metadata;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(41)
        .with_sequence_number(if version == FormatVersion::V1 { 0 } else { 1 })
        .with_schema_id(base.current_schema_id())
        .with_timestamp_ms(base.last_updated_ms())
        .with_manifest_list("memory://warehouse/db/defaults/snap-41.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        })
        .with_row_range(base.next_row_id(), 1)
        .build();
    base.into_builder(None)
        .add_snapshot(snapshot)
        .expect("real snapshot admission")
        .build()
        .expect("snapshot metadata")
        .metadata
}
#[test]
fn cow_shared_schema_is_wire_transparent_and_reader_defaults_match_for_stock_versions() {
    use crate::commit::write_stack::domain::{IcebergDataBranchRecipe, IcebergDataSchema};
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let metadata = metadata(version);
        let schema = metadata.current_schema();
        let old = crate::schema_facts::iceberg_schema_def(schema);
        let (scope, _) = scope(u64::MAX); // Finite codec component, no FE capacity claim.
        let plan = SchemaOnlyPlan::inspect(schema, &scope).unwrap();
        let backing = plan
            .authorize(std::mem::size_of::<Scope>() as u64, &scope)
            .unwrap()
            .build()
            .unwrap();
        let shared = IcebergDataSchema::CowShared(backing.clone());
        let bytes = serde_json::to_vec(&shared).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&old).unwrap());
        let decoded: IcebergDataSchema = serde_json::from_slice(&bytes).unwrap();
        assert!(matches!(&decoded, IcebergDataSchema::Owned(_)));
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
        let old_recipe =
            IcebergDataBranchRecipe::try_new(Some(old), vec![], vec![], vec![], true).unwrap();
        let recipe =
            IcebergDataBranchRecipe::try_new_cow(backing, vec![], vec![], vec![], true).unwrap();
        let arrow = Arc::new(crate::iceberg::arrow::schema_to_arrow_schema(schema).unwrap());
        let old_read = crate::schema_mapping::annotate_read_schema_from_scan_model(
            &arrow,
            old_recipe.input_schema().unwrap(),
        )
        .unwrap();
        let new_read = crate::schema_mapping::annotate_read_schema_from_scan_model(
            &arrow,
            recipe.input_schema().unwrap(),
        )
        .unwrap();
        assert_eq!(new_read, old_read);
        assert_eq!(
            recipe.input_schema().unwrap().fields[0].children[1]
                .initial_default_json
                .as_deref(),
            Some("\"synthetic map child\"")
        );
        assert_eq!(
            recipe.input_schema().unwrap().fields[2].children[0]
                .write_default_json
                .as_deref(),
            Some("8")
        );
    }
}
#[test]
fn actual_data_recipe_clone_shares_schema_strings_and_keeps_the_same_guard() {
    use crate::commit::write_stack::domain::IcebergDataBranchRecipe;
    let metadata = metadata(FormatVersion::V3);
    let (scope, drops) = scope(u64::MAX);
    let backing = SchemaOnlyPlan::inspect(metadata.current_schema(), &scope)
        .unwrap()
        .authorize(std::mem::size_of::<Scope>() as u64, &scope)
        .unwrap()
        .build()
        .unwrap();
    let first = IcebergDataBranchRecipe::try_new_cow(
        backing,
        vec!["nan".into()],
        vec!["nan_partition".into()],
        vec!["identity".into()],
        true,
    )
    .unwrap();
    let last = first.clone(); // ACTUAL production clone specialization.
    let first_schema = first.input_schema().unwrap();
    let last_schema = last.input_schema().unwrap();
    assert!(std::ptr::eq(first_schema, last_schema));
    assert_eq!(
        first_schema.fields[0].name.as_ptr(),
        last_schema.fields[0].name.as_ptr()
    );
    assert_eq!(
        first_schema.fields[0]
            .initial_default_json
            .as_ref()
            .unwrap()
            .as_ptr(),
        last_schema.fields[0]
            .initial_default_json
            .as_ref()
            .unwrap()
            .as_ptr()
    );
    drop(first);
    drop(scope);
    drop(metadata);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        last.partition_column_names(),
        &["nan_partition".to_string()]
    );
    assert!(
        last.input_schema().unwrap().fields[0]
            .initial_default_json
            .is_some()
    );
    drop(last);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn schema_refusal_keeps_original_opaque_cause_and_no_schema_is_published() {
    #[derive(Debug)]
    struct Opaque(Box<u8>);
    impl std::fmt::Display for Opaque {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("arbitrary original formatter must not run")
        }
    }
    struct Reject {
        error: std::sync::Mutex<Option<Opaque>>,
        guard: ConnectorPayloadRetentionGuard,
    }
    impl OriginalScope<Opaque> for Reject {
        fn check_active(&self) -> Result<(), Opaque> {
            Ok(())
        }
        fn check_total(&self, _: u64) -> Result<(), Opaque> {
            Err(self.error.lock().unwrap().take().unwrap())
        }
        fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
            self.guard.clone()
        }
    }
    let metadata = metadata(FormatVersion::V3);
    let raw = Box::new(19u8);
    let pointer = &*raw as *const u8;
    let (scope, drops) = scope(u64::MAX);
    let reject = Reject {
        error: std::sync::Mutex::new(Some(Opaque(raw))),
        guard: scope.guard.clone(),
    };
    let plan = SchemaOnlyPlan::inspect(metadata.current_schema(), &reject).unwrap();
    let error = match plan.authorize(std::mem::size_of::<Reject>() as u64, &reject) {
        Err(Error::Control(error)) => error,
        _ => panic!("original refusal must precede owned schema construction"),
    };
    assert_eq!(&*error.0 as *const u8, pointer);
    drop(error);
    drop(reject);
    drop(scope);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn authorization_does_not_replace_the_original_stop_check_before_build() {
    let metadata = metadata(FormatVersion::V3);
    let (scope, drops) = scope(u64::MAX);
    let checked = SchemaOnlyPlan::inspect(metadata.current_schema(), &scope)
        .unwrap()
        .authorize(std::mem::size_of::<Scope>() as u64, &scope)
        .unwrap();
    scope.stop.store(true, Ordering::SeqCst);
    let error = match checked.build() {
        Err(Error::Control(error)) => error,
        _ => panic!("late original stop must refuse schema publication"),
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
    drop(error);
    drop(scope);
    drop(metadata);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn field_entry_stop_destroys_the_unclaimed_root_headers_and_keeps_raw_cause() {
    #[derive(Debug)]
    struct Opaque(Box<u8>);
    struct StopAtField {
        armed: AtomicBool,
        error: std::sync::Mutex<Option<Opaque>>,
        guard: ConnectorPayloadRetentionGuard,
    }
    impl OriginalScope<Opaque> for StopAtField {
        fn check_active(&self) -> Result<(), Opaque> {
            if self.armed.load(Ordering::SeqCst) {
                Err(self.error.lock().unwrap().take().unwrap())
            } else {
                Ok(())
            }
        }
        fn check_total(&self, _: u64) -> Result<(), Opaque> {
            Ok(())
        }
        fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
            let guard = self.guard.clone();
            // This is the actual build handoff, after inspect/authorize. The
            // first original field checkpoint then refuses the unclaimed graph.
            self.armed.store(true, Ordering::SeqCst);
            guard
        }
    }
    let metadata = metadata(FormatVersion::V3);
    let raw = Box::new(29u8);
    let pointer = &*raw as *const u8;
    let (owner, drops) = scope(u64::MAX);
    let checker = StopAtField {
        armed: AtomicBool::new(false),
        error: std::sync::Mutex::new(Some(Opaque(raw))),
        guard: owner.guard.clone(),
    };
    let checked = SchemaOnlyPlan::inspect(metadata.current_schema(), &checker)
        .unwrap()
        .authorize(std::mem::size_of::<StopAtField>() as u64, &checker)
        .unwrap();
    let error = match checked.build() {
        Err(Error::Control(error)) => error,
        _ => panic!("field checkpoint must keep original stop"),
    };
    assert_eq!(&*error.0 as *const u8, pointer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(error);
    drop(checker);
    drop(owner);
    drop(metadata);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
