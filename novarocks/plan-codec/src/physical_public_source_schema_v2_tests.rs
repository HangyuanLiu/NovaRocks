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
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_schema_v2::prepare_schemas_decode_with_creator_in;
use crate::physical_type_v2::{
    PackageTypeProjectionLimits, decode_package_type_table_with_creator_in,
};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::atomic::{AtomicUsize, Ordering};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 4096,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 8 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 16 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 8 * 1024 * 1024,
            max_coexisting_source_and_request_bytes: 16 * 1024 * 1024,
            max_work: 64 * 1024 * 1024,
        },
    }
}

#[test]
fn opaque_nr_and_iceberg_attributes_never_author_id_default_type_or_nullable() {
    let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
        "source",
        DataType::Int64,
        true,
    ))]));
    let shape =
        ConnectorSourceSchemaShape::try_new(schema.clone(), ValueTypeStorageLifetime::pure())
            .unwrap();
    let field = ConnectorSourceFieldFacts::try_new(
        vec![0].into_boxed_slice(),
        Some(41),
        Some(ConnectorSourceInitialDefault::json(
            Arc::from("7"),
            ValueTypeStorageLifetime::pure(),
        )),
        HashMap::from([
            ("nr.logical_type".into(), "json".into()),
            ("iceberg.field.id".into(), "999".into()),
            ("iceberg.initial-default".into(), "null".into()),
        ]),
        &[],
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let facts = ConnectorSourceSchemaFacts::try_new(
        shape,
        HashMap::from([("nr.logical_type".into(), "variant".into())]),
        vec![field].into_boxed_slice(),
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let wire = encode_public_source_schemas_pure_in(
        &[SourceSchemaFactsSource {
            id: u32::MAX,
            source_shape_schema_id: 0,
            facts: &facts,
        }],
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let declared = encoded_public_source_schemas_backing_bytes(&wire).unwrap();
    use novarocks_proto_models::{arrow_type::ArrowPrimitiveType, physical_type_v2 as t};
    let package = wire::FragmentPackage {
        types: Some(t::TypeTable {
            carriers: vec![t::CarrierTypeDefinition {
                id: 0,
                kind: Some(t::carrier_type_definition::Kind::Primitive(
                    ArrowPrimitiveType::Int64 as i32,
                )),
            }],
            fields: vec![t::FieldDefinition {
                id: 0,
                name: "source".into(),
                nullable: true,
                carrier_type_id: Some(0),
                logical_type_id: None,
                value_binding_id: None,
            }],
            value_types: vec![],
            logical_types: vec![],
        }),
        source_fields: Some(t::SourceFieldTable {
            shape: Some(t::TypeTable::default()),
            field_facts: vec![],
        }),
        ..Default::default()
    };
    let table = package.types.as_ref().unwrap();
    // This fixture is the original raw author: count its real public backing,
    // then carry the actual Type receiver's construction contribution once.
    let raw_source = std::mem::size_of_val(&package)
        + table.carriers.capacity() * size_of::<t::CarrierTypeDefinition>()
        + table.fields.capacity() * size_of::<t::FieldDefinition>()
        + table.fields[0].name.capacity();
    let mut exact = |_: Layout| Ok::<(), ()>(());
    let mut opaque = |_: usize| Ok::<(), ()>(());
    // This is an explicit pure preparation author, without runtime funding.
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let types = decode_package_type_table_with_creator_in(
        &package,
        raw_source,
        PackageTypeProjectionLimits {
            max_definitions: 32,
            max_expanded_nodes: 256,
            max_string_bytes: 4096,
            max_allocation_requests: 10000,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
            max_work: 4 * 1024 * 1024,
        },
        &mut creation,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let schema_wire = vec![wire::SchemaDefinition {
        id: 0,
        field_ids: vec![0],
    }];
    let schema_source = size_of::<Vec<wire::SchemaDefinition>>()
        + schema_wire.capacity() * size_of::<wire::SchemaDefinition>()
        + schema_wire[0].field_ids.capacity() * size_of::<u32>();
    let schema_invoice = raw_source
        + schema_source
        + types.creation_request_bytes_upper_bound().unwrap()
        + std::mem::size_of_val(&types);
    let schema_requests = AtomicUsize::new(0);
    let mut exact = |layout: Layout| {
        schema_requests.fetch_add(layout.size(), Ordering::SeqCst);
        Ok::<(), ()>(())
    };
    let mut opaque = |bytes: usize| {
        schema_requests.fetch_add(bytes, Ordering::SeqCst);
        Ok::<(), ()>(())
    };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let schemas = prepare_schemas_decode_with_creator_in(
        &schema_wire,
        &types,
        schema_invoice,
        limits(),
        &mut creation,
        &mut work,
    )
    .unwrap()
    .emit_observed_in(&mut |_| Ok(()), &mut work)
    .unwrap();
    let source_invoice = declared
        + schema_invoice
        + schema_requests.load(Ordering::SeqCst)
        + std::mem::size_of_val(&schemas);
    let decoded =
        decode_public_source_schemas_pure_in(&wire, &schemas, source_invoice, limits(), &mut work)
            .unwrap();
    let observed = decoded
        .lookup_observed(u32::MAX, &mut work)
        .unwrap()
        .unwrap();
    assert_eq!(
        decoded
            .source_shape_schema_id_observed(u32::MAX, &mut work)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        observed.shape().schema().fields()[0].data_type(),
        &DataType::Int64
    );
    assert!(observed.shape().schema().fields()[0].is_nullable());
    let field = observed.field_fact(&[0]).unwrap();
    assert_eq!(field.field_id(), Some(41));
    assert_eq!(field.initial_default().unwrap().as_json(), "7");
    assert_eq!(field.attributes()["iceberg.field.id"], "999");
    assert_eq!(field.attributes()["nr.logical_type"], "json");
    assert_eq!(observed.schema_attributes()["nr.logical_type"], "variant");
    work.finish().unwrap();
}

#[test]
fn encoded_source_backing_counts_real_spare_capacity_and_utf8_allocations() {
    let mut defs = Vec::with_capacity(7);
    let mut path = Vec::with_capacity(5);
    path.push(0);
    let mut key = String::with_capacity(37);
    key.push_str("key");
    let mut value = String::with_capacity(53);
    value.push_str("value");
    let mut attrs = Vec::with_capacity(3);
    attrs.push(ArrowFieldMetadataEntry { key, value });
    let mut fields = Vec::with_capacity(4);
    fields.push(wire::PublicSourceFieldFactsDefinition {
        field_path: path,
        field_id: Some(0),
        initial_default: None,
        attributes: attrs,
    });
    defs.push(wire::PublicSourceSchemaFactsDefinition {
        id: Some(0),
        source_shape_schema_id: Some(0),
        schema_attributes: vec![],
        field_facts: fields,
    });
    let expected = size_of::<Vec<wire::PublicSourceSchemaFactsDefinition>>()
        + layout::<wire::PublicSourceSchemaFactsDefinition>(defs.capacity())
            .unwrap()
            .size()
        + layout::<wire::PublicSourceFieldFactsDefinition>(defs[0].field_facts.capacity())
            .unwrap()
            .size()
        + layout::<u32>(defs[0].field_facts[0].field_path.capacity())
            .unwrap()
            .size()
        + layout::<ArrowFieldMetadataEntry>(defs[0].field_facts[0].attributes.capacity())
            .unwrap()
            .size()
        + defs[0].field_facts[0].attributes[0].key.capacity()
        + defs[0].field_facts[0].attributes[0].value.capacity();
    assert_eq!(
        encoded_public_source_schemas_backing_bytes(&defs).unwrap(),
        expected
    );
    assert!(expected > std::mem::size_of_val(defs.as_slice()));
}
