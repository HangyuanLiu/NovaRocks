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

use iceberg::{Error, ErrorKind, Result};

// This is an RPC item-count bound, not a response-byte or allocation bound.
pub(crate) const HMS_TABLE_OBJECT_BATCH_SIZE: usize = 100;

// Validate the complete HMS entity batch before publishing any classification.
// Missing entities are allowed: a table can disappear between the two reads.
pub(crate) fn project_iceberg_table_batch(
    database: &str,
    requested: &[faststr::FastStr],
    objects: &[hive_metastore::Table],
) -> Result<[bool; HMS_TABLE_OBJECT_BATCH_SIZE]> {
    let invalid = |message| Error::new(ErrorKind::DataInvalid, message);
    if requested.len() > HMS_TABLE_OBJECT_BATCH_SIZE || objects.len() > requested.len() {
        return Err(invalid(
            "HMS table object response exceeds its requested batch",
        ));
    }
    for (index, name) in requested.iter().enumerate() {
        if name.is_empty() || requested[..index].iter().any(|prior| prior == name) {
            return Err(invalid(
                "HMS table object batch has an empty or duplicate requested name",
            ));
        }
    }

    let mut seen = [false; HMS_TABLE_OBJECT_BATCH_SIZE];
    let mut iceberg = [false; HMS_TABLE_OBJECT_BATCH_SIZE];
    for object in objects {
        let returned_database = object
            .db_name
            .as_ref()
            .ok_or_else(|| invalid("HMS table object response is missing its database name"))?;
        if !returned_database.as_str().eq_ignore_ascii_case(database) {
            return Err(invalid(
                "HMS table object response has a different database name",
            ));
        }
        let name = object
            .table_name
            .as_ref()
            .ok_or_else(|| invalid("HMS table object response is missing its table name"))?;
        let index = requested
            .iter()
            .position(|requested| requested == name)
            .ok_or_else(|| {
                invalid("HMS table object response contains an unrequested table name")
            })?;
        if seen[index] {
            return Err(invalid(
                "HMS table object response contains a duplicate table name",
            ));
        }
        seen[index] = true;
        iceberg[index] = object
            .parameters
            .as_ref()
            .and_then(|parameters| parameters.get("table_type"))
            .is_some_and(|value| value.as_str().eq_ignore_ascii_case("iceberg"));
    }
    Ok(iceberg)
}

#[cfg(test)]
mod table_projection_tests {
    use super::*;
    use faststr::FastStr;

    fn names(values: &[&str]) -> Vec<FastStr> {
        values
            .iter()
            .map(|value| (*value).to_owned().into())
            .collect()
    }

    fn object(database: &str, name: &str, kind: Option<&str>) -> hive_metastore::Table {
        let mut object = hive_metastore::Table {
            db_name: Some(database.to_owned().into()),
            table_name: Some(name.to_owned().into()),
            ..Default::default()
        };
        if let Some(kind) = kind {
            object.parameters = Some(Default::default());
            object
                .parameters
                .as_mut()
                .unwrap()
                .insert("table_type".into(), kind.to_owned().into());
        }
        object
    }

    #[test]
    fn historical_case_is_preserved_and_views_foreign_missing_params_are_excluded() {
        let requested = names(&[
            "upper", "lower", "mixed", "view", "foreign", "missing", "no_key",
        ]);
        let mut no_key = object("db", "no_key", None);
        no_key.parameters = Some(Default::default());
        let objects = [
            object("db", "upper", Some("ICEBERG")),
            object("db", "lower", Some("iceberg")),
            object("db", "mixed", Some("IcEbErG")),
            object("db", "view", Some("ICEBERG-VIEW")),
            object("db", "foreign", Some("FOREIGN")),
            object("db", "missing", None),
            no_key,
        ];
        let projected = project_iceberg_table_batch("db", &requested, &objects).unwrap();
        assert_eq!(
            &projected[..requested.len()],
            &[true, true, true, false, false, false, false]
        );
        assert!(projected[requested.len()..].iter().all(|keep| !keep));
    }

    #[test]
    fn concurrent_absence_is_not_filled_or_retried() {
        let requested = names(&["gone", "still_here"]);
        let objects = [object("db", "still_here", Some("ICEBERG"))];
        let projected = project_iceberg_table_batch("db", &requested, &objects).unwrap();
        assert_eq!(&projected[..2], &[false, true]);
        assert!(
            project_iceberg_table_batch("db", &requested, &[])
                .unwrap()
                .iter()
                .all(|keep| !keep)
        );
    }

    #[test]
    fn returned_order_maps_to_requested_names_and_database_case_is_hms_case_insensitive() {
        let requested = names(&["first", "second"]);
        let objects = [
            object("DB", "second", Some("ICEBERG-VIEW")),
            object("DB", "first", Some("ICEBERG")),
        ];
        assert_eq!(
            &project_iceberg_table_batch("db", &requested, &objects).unwrap()[..2],
            &[true, false]
        );
    }

    #[test]
    fn missing_identity_fields_and_wrong_database_are_refused() {
        let requested = names(&["table"]);
        let mut missing_database = object("db", "table", Some("ICEBERG"));
        missing_database.db_name = None;
        let mut missing_name = object("db", "table", Some("ICEBERG"));
        missing_name.table_name = None;
        for invalid in [
            missing_database,
            missing_name,
            object("other", "table", Some("ICEBERG")),
        ] {
            assert!(project_iceberg_table_batch("db", &requested, &[invalid]).is_err());
        }
    }

    #[test]
    fn unrequested_name_cannot_replace_an_absent_requested_object() {
        let requested = names(&["expected"]);
        assert!(
            project_iceberg_table_batch(
                "db",
                &requested,
                &[object("db", "extra", Some("ICEBERG"))]
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_returned_name_is_refused_even_with_different_classification() {
        let requested = names(&["same", "gone"]);
        let objects = [
            object("db", "same", Some("ICEBERG")),
            object("db", "same", Some("ICEBERG-VIEW")),
        ];
        assert!(project_iceberg_table_batch("db", &requested, &objects).is_err());
    }

    #[test]
    fn response_count_and_request_batch_bound_are_checked() {
        let requested = names(&["one"]);
        let objects = [
            object("db", "one", Some("ICEBERG")),
            object("db", "extra", Some("ICEBERG")),
        ];
        assert!(project_iceberg_table_batch("db", &requested, &objects).is_err());
        let too_many: Vec<FastStr> = (0..=HMS_TABLE_OBJECT_BATCH_SIZE)
            .map(|index| format!("table_{index}").into())
            .collect();
        assert!(project_iceberg_table_batch("db", &too_many, &[]).is_err());
        let at_bound: Vec<FastStr> = (0..HMS_TABLE_OBJECT_BATCH_SIZE)
            .map(|index| format!("table_{index}").into())
            .collect();
        assert!(project_iceberg_table_batch("db", &at_bound, &[]).is_ok());
    }

    #[test]
    fn empty_or_duplicate_requested_names_are_refused() {
        assert!(project_iceberg_table_batch("db", &names(&[""]), &[]).is_err());
        assert!(project_iceberg_table_batch("db", &names(&["same", "same"]), &[]).is_err());
    }

    #[test]
    fn late_invalid_entity_prevents_a_successful_projection() {
        let requested = names(&["valid", "second"]);
        let objects = [
            object("db", "valid", Some("ICEBERG")),
            object("other", "second", Some("ICEBERG")),
        ];
        assert!(project_iceberg_table_batch("db", &requested, &objects).is_err());
    }
}
