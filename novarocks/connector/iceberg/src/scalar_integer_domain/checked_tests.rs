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

use super::*;
use crate::iceberg::spec::{
    FormatVersion, Literal, NestedField, PartitionSpec, SortOrder, TableMetadata,
    TableMetadataBuilder,
};
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Canary {
    drops: Arc<AtomicUsize>,
}
impl std::fmt::Display for Canary {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("original checker payload formatted")
    }
}
impl Drop for Canary {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
enum Cause {
    Provider(ConnectorError),
    Check(Box<Canary>),
}
impl From<ConnectorError> for Cause {
    fn from(error: ConnectorError) -> Self {
        Self::Provider(error)
    }
}
impl std::fmt::Debug for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Provider(_) => "Provider",
            Self::Check(_) => "Check",
        })
    }
}
fn schema(width: i32, generation: i32) -> Schema {
    let mut fields = (1..=width)
        .map(|id| {
            Arc::new(NestedField::optional(
                id,
                format!("field_{id}"),
                PrimitiveType::Int.into(),
            ))
        })
        .collect::<Vec<_>>();
    // Each lawful history version has a real distinct field, avoiding stock
    // deduplication of semantically identical schemas.
    fields.push(Arc::new(NestedField::optional(
        10000 + generation,
        format!("generation_{generation}"),
        PrimitiveType::Boolean.into(),
    )));
    Schema::builder().with_fields(fields).build().unwrap()
}
fn loaded_history(width: i32, count: i32, properties: HashMap<String, String>) -> TableMetadata {
    let mut metadata = TableMetadataBuilder::new(
        schema(width, 0),
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://checked-domain-history".to_string(),
        FormatVersion::V2,
        properties,
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    for generation in 1..count {
        metadata = metadata
            .into_builder(None)
            .add_schema(schema(width, generation))
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
    }
    serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap()
}
fn collect_cause<T>(
    result: Result<T, Cause>,
    identity: *const Canary,
    drops: &AtomicUsize,
) -> (bool, bool) {
    match result {
        Err(Cause::Check(raw)) => {
            let same = (&*raw) as *const Canary == identity;
            let held = drops.load(Ordering::SeqCst) == 0;
            drop(raw);
            (same, held)
        }
        other => {
            drop(other);
            (false, false)
        }
    }
}
#[test]
fn real_stock_history_noop_matches_original_and_cancel_stops_the_actual_history_scan() {
    let metadata = loaded_history(
        96,
        32,
        HashMap::from([(PROPERTY.to_string(), r#"{"95":"tinyint"}"#.to_string())]),
    );
    assert_eq!(metadata.schemas_iter().count(), 32);
    let expected = metadata_declarations(&metadata).unwrap();
    assert_eq!(
        metadata_declarations_checked::<ConnectorError>(&metadata, &|_| Ok(())).unwrap(),
        expected
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let raw = Box::new(Canary {
        drops: drops.clone(),
    });
    let identity = (&*raw) as *const Canary;
    let original = RefCell::new(Some(raw));
    let fields = Cell::new(0);
    let failed = Cell::new(false);
    let after = Cell::new(false);
    let result = metadata_declarations_checked::<Cause>(&metadata, &|point| {
        if failed.get() {
            after.set(true)
        }
        if point == CheckPoint::HistoryField {
            fields.set(fields.get() + 1);
            if fields.get() == 500 {
                failed.set(true);
                return Err(Cause::Check(original.borrow_mut().take().unwrap()));
            }
        }
        Ok(())
    });
    let facts = collect_cause(result, identity, &drops);
    drop(original.into_inner());
    assert_eq!(facts, (true, true));
    assert_eq!(fields.get(), 500);
    assert!(!after.get());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn actual_legacy_field_filter_stops_before_finishing_its_wide_schema_and_keeps_original_cause() {
    let metadata = loaded_history(
        512,
        1,
        HashMap::from([(format!("{LEGACY_PREFIX}FIELD_512"), "SmallInt".to_string())]),
    );
    let expected = metadata_declarations(&metadata).unwrap();
    assert_eq!(expected.get(&512), Some(&ScalarIntegerDomain::Int16));
    let drops = Arc::new(AtomicUsize::new(0));
    let raw = Box::new(Canary {
        drops: drops.clone(),
    });
    let identity = (&*raw) as *const Canary;
    let original = RefCell::new(Some(raw));
    let visits = Cell::new(0);
    let result = metadata_declarations_checked::<Cause>(&metadata, &|point| {
        if point == CheckPoint::LegacyField {
            visits.set(visits.get() + 1);
            if visits.get() == 128 {
                return Err(Cause::Check(original.borrow_mut().take().unwrap()));
            }
        }
        Ok(())
    });
    let facts = collect_cause(result, identity, &drops);
    drop(original.into_inner());
    assert_eq!(facts, (true, true));
    assert_eq!(visits.get(), 128);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn actual_schema_validation_walk_stops_with_original_cause_and_no_new_publication() {
    let schema = schema(512, 0);
    let domains = BTreeMap::from([(512, ScalarIntegerDomain::Int16)]);
    assert!(validate_schema(&schema, &domains).is_ok());
    let drops = Arc::new(AtomicUsize::new(0));
    let raw = Box::new(Canary {
        drops: drops.clone(),
    });
    let identity = (&*raw) as *const Canary;
    let original = RefCell::new(Some(raw));
    let visits = Cell::new(0);
    let result = validate_schema_checked::<Cause>(&schema, &domains, &|point| {
        if point == CheckPoint::ValidationField {
            visits.set(visits.get() + 1);
            if visits.get() == 128 {
                return Err(Cause::Check(original.borrow_mut().take().unwrap()));
            }
        }
        Ok(())
    });
    let facts = collect_cause(result, identity, &drops);
    drop(original.into_inner());
    assert_eq!(facts, (true, true));
    assert_eq!(visits.get(), 128);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn actual_serde_map_visitor_returns_unformatted_checker_cause_after_partial_map_drop() {
    let raw = (1..=512)
        .map(|id| format!("\"{id}\":\"tinyint\""))
        .collect::<Vec<_>>()
        .join(",");
    let raw = format!("{{{raw}}}");
    assert_eq!(decode(&raw).unwrap().len(), 512);
    let drops = Arc::new(AtomicUsize::new(0));
    let original_box = Box::new(Canary {
        drops: drops.clone(),
    });
    let identity = (&*original_box) as *const Canary;
    let original = RefCell::new(Some(original_box));
    let visits = Cell::new(0);
    let result = decode_checked::<Cause>(&raw, &|point| {
        if point == CheckPoint::DecodeEntry {
            visits.set(visits.get() + 1);
            if visits.get() == 128 {
                return Err(Cause::Check(original.borrow_mut().take().unwrap()));
            }
        }
        Ok(())
    });
    let facts = collect_cause(result, identity, &drops);
    drop(original.into_inner());
    assert_eq!(facts, (true, true));
    assert_eq!(visits.get(), 128);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn original_error_order_and_text_remain_with_noop_and_before_later_validation_checks() {
    let too_large = "x".repeat(MAX_BYTES + 1);
    let properties = HashMap::from([(PROPERTY.to_string(), too_large)]);
    let calls = Cell::new(0);
    let actual = declarations_checked::<ConnectorError>(&schema(1, 0), &properties, &|_| {
        calls.set(calls.get() + 1);
        Ok(())
    })
    .err()
    .unwrap();
    assert_eq!(
        actual,
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg scalar integer declarations exceed the hard limit"
        )
    );
    assert_eq!(calls.get(), 0);
    for raw in [
        r#"{"1":"tinyint","1":"smallint"}"#,
        r#"{"0":"tinyint"}"#,
        r#"{"1":"other"}"#,
        r#"{} trailing"#,
    ] {
        assert_eq!(
            decode(raw).err().unwrap(),
            decode_checked::<ConnectorError>(raw, &|_| Ok(()))
                .err()
                .unwrap()
        );
    }
    let invalid = Schema::builder()
        .with_fields(vec![Arc::new(
            NestedField::optional(1, "tiny", PrimitiveType::Int.into())
                .with_initial_default(Literal::int(128)),
        )])
        .build()
        .unwrap();
    let domains = BTreeMap::from([(1, ScalarIntegerDomain::Int8)]);
    let expected = corrupt("Iceberg INT value exceeds declared TINYINT domain");
    assert_eq!(validate_schema(&invalid, &domains).err().unwrap(), expected);
    let after = Cell::new(false);
    let actual = validate_schema_checked::<ConnectorError>(&invalid, &domains, &|point| {
        if point == CheckPoint::ValidationTopLevel {
            after.set(true)
        }
        Ok(())
    })
    .err()
    .unwrap();
    assert_eq!(actual, expected);
    assert!(!after.get());
}
