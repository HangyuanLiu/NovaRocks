// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use std::mem::size_of_val;

fn encoded_observed<T>(
    token: &EncodedConnectorPayloads<'_, '_>,
    visit: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, Error>,
) -> Result<T, Error> {
    let mut work = CompileCheckpoints::try_new(token.original_control(), CompilePhase::Encode)?;
    let result = visit(&mut work);
    finish(result, work)
}
fn decoded_observed<T>(
    token: &DecodedConnectorPayloads<'_, '_>,
    visit: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, Error>,
) -> Result<T, Error> {
    let mut work = CompileCheckpoints::try_new(token.original_control(), CompilePhase::Decode)?;
    let result = visit(&mut work);
    finish(result, work)
}
fn composition_prefixes(invoke: impl Fn(&Control) -> Result<(), Error>, succeeds: bool) {
    let original = Control::default();
    assert_eq!(invoke(&original).is_ok(), succeeds);
    let expected = trace(&original);
    assert!(!expected.is_empty());
    for at in 0..expected.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((at, cause)),
                ..Control::default()
            };
            assert!(matches!(invoke(&control), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(trace(&control), expected[..=at]);
        }
    }
}

#[test]
fn namespace_source_reference_requires_original_owner_and_unique_sparse_association() {
    let original = payload(ConnectorCodecCategory::ReadTable, &[0, 7, 255]);
    let equal_clone = original.clone();
    assert_eq!(original, equal_clone);
    assert!(!std::ptr::eq(&original, &equal_clone));
    let control = Control::default();
    let one = [(u32::MAX, &original)];
    let token = encode_connector_payloads(&one, SOURCE, limits(), &control).unwrap();
    assert!(std::ptr::eq(
        token.original_control(),
        &control as &dyn PureCompileControl
    ));
    assert_eq!(token.source_count(), 1);
    assert_eq!(
        encoded_observed(&token, |work| token.source_id_observed(&original, work)).unwrap(),
        u32::MAX
    );
    assert!(matches!(
        encoded_observed(&token, |work| token.source_id_observed(&equal_clone, work)),
        Err(Error::InvalidShape(
            "connector payload source owner is not in this namespace"
        ))
    ));
    let alias = [(u32::MAX, &original), (0, &original)];
    let token = encode_connector_payloads(&alias, SOURCE, limits(), &control).unwrap();
    assert_eq!(token.source_count(), 2);
    assert!(matches!(
        encoded_observed(&token, |work| token.source_id_observed(&original, work)),
        Err(Error::InvalidShape(
            "connector payload source association is ambiguous"
        ))
    ));
    // Equal values from distinct original owners have separate lawful IDs.
    let distinct = [(u32::MAX, &original), (0, &equal_clone)];
    let token = encode_connector_payloads(&distinct, SOURCE, limits(), &control).unwrap();
    assert_eq!(
        encoded_observed(&token, |work| token.source_id_observed(&original, work)).unwrap(),
        u32::MAX
    );
    assert_eq!(
        encoded_observed(&token, |work| token.source_id_observed(&equal_clone, work)).unwrap(),
        0
    );
    let empty = encode_connector_payloads(&[], 0, limits(), &control).unwrap();
    assert_eq!(empty.source_count(), 0);
    assert!(matches!(
        encoded_observed(&empty, |work| empty.source_id_observed(&original, work)),
        Err(Error::InvalidShape(_))
    ));
}

#[test]
fn encoded_namespace_composition_counts_original_invoice_once_and_all_owned_outputs() {
    let original = payload(ConnectorCodecCategory::WriteHandle, &[1, 2, 3]);
    let inputs = [(0, &original), (u32::MAX, &original)];
    let control = Control::default();
    let a = encode_connector_payloads(&inputs, SOURCE, limits(), &control).unwrap();
    let b = encode_connector_payloads(&inputs, SOURCE + 8192, limits(), &control).unwrap();
    let a_floor = encoded_observed(&a, |work| a.retained_floor_observed(work)).unwrap();
    let b_floor = encoded_observed(&b, |work| b.retained_floor_observed(work)).unwrap();
    assert_eq!(b_floor - a_floor, 8192);
    let wire = a.as_wire();
    assert_eq!(wire.len(), 2);
    let first = wire[0].payload.as_ref().unwrap();
    let second = wire[1].payload.as_ref().unwrap();
    assert!(!std::ptr::eq(
        first.payload.as_ptr(),
        second.payload.as_ptr()
    ));
    // Independently visible current backing. Original alias backing is already
    // invoiced once; each wire output really owns separate strings and bytes.
    let visible_owned = a.wire.capacity() * size_of::<wire::ConnectorPayloadDefinition>()
        + wire
            .iter()
            .map(|definition| {
                let value = definition.payload.as_ref().unwrap();
                let header = value.header.as_ref().unwrap();
                let catalog = header.catalog.as_ref().unwrap();
                value.payload.capacity()
                    + header.provider_id.capacity()
                    + catalog.catalog_name.capacity()
                    + catalog.version.capacity()
            })
            .sum::<usize>();
    assert!(
        a_floor >= SOURCE + size_of_val(&a) + visible_owned + inputs.len() * size_of::<usize>()
    );
    let accepted = encoded_observed(&a, |work| {
        let floor = a.retained_floor_observed(work)?;
        source_floor(a_floor, floor, work)
    });
    assert!(accepted.is_ok());
    assert!(matches!(
        encoded_observed(&a, |work| {
            let floor = a.retained_floor_observed(work)?;
            source_floor(a_floor - 1, floor, work)
        }),
        Err(Error::InvalidShape(_))
    ));
    composition_prefixes(
        |control| {
            let token = encode_connector_payloads(&inputs, SOURCE, limits(), control)?;
            encoded_observed(&token, |work| {
                let floor = token.retained_floor_observed(work)?;
                source_floor(a_floor, floor, work)
            })
        },
        true,
    );
    composition_prefixes(
        |control| {
            let token = encode_connector_payloads(&inputs, SOURCE, limits(), control)?;
            encoded_observed(&token, |work| {
                let floor = token.retained_floor_observed(work)?;
                source_floor(a_floor - 1, floor, work)
            })
        },
        false,
    );
}

#[test]
fn decoded_namespace_composition_keeps_wire_invoice_and_new_typed_owner_backings() {
    let mut definitions = Vec::with_capacity(32);
    definitions.push(expected(u32::MAX, 1, &[9, 8]));
    definitions.push(expected(0, 6, &[7]));
    let raw = definitions[0].payload.as_mut().unwrap();
    raw.payload.reserve(8192);
    raw.header.as_mut().unwrap().provider_id.reserve(4096);
    let visible_original = definitions.capacity() * size_of::<wire::ConnectorPayloadDefinition>()
        + definitions
            .iter()
            .map(|definition| {
                let value = definition.payload.as_ref().unwrap();
                let header = value.header.as_ref().unwrap();
                let catalog = header.catalog.as_ref().unwrap();
                value.payload.capacity()
                    + header.provider_id.capacity()
                    + catalog.catalog_name.capacity()
                    + catalog.version.capacity()
            })
            .sum::<usize>();
    assert!(visible_original < SOURCE);
    let control = Control::default();
    let a = decode_connector_payloads(&definitions, SOURCE, limits(), &control).unwrap();
    let b = decode_connector_payloads(&definitions, SOURCE + 8192, limits(), &control).unwrap();
    assert!(std::ptr::eq(
        a.original_control(),
        &control as &dyn PureCompileControl
    ));
    assert!(std::ptr::eq(a.as_wire(), definitions.as_slice()));
    assert_eq!(a.source_count(), 2);
    let a_floor = decoded_observed(&a, |work| a.retained_floor_observed(work)).unwrap();
    let b_floor = decoded_observed(&b, |work| b.retained_floor_observed(work)).unwrap();
    assert_eq!(b_floor - a_floor, 8192);
    let current_minimum = a.payloads.capacity() * size_of::<ConnectorEncodedPayload>()
        + a.payloads
            .iter()
            .map(|value| {
                value.payload().len()
                    + value.header().provider_id().as_str().len()
                    + value.header().catalog().catalog_name().as_str().len()
            })
            .sum::<usize>()
        + definitions.len() * size_of::<usize>();
    assert!(a_floor >= SOURCE + size_of_val(&a) + current_minimum);
    assert!(
        decoded_observed(&a, |work| {
            let floor = a.retained_floor_observed(work)?;
            source_floor(a_floor, floor, work)
        })
        .is_ok()
    );
    composition_prefixes(
        |control| {
            let token = decode_connector_payloads(&definitions, SOURCE, limits(), control)?;
            decoded_observed(&token, |work| {
                let floor = token.retained_floor_observed(work)?;
                source_floor(a_floor - 1, floor, work)
            })
        },
        false,
    );
    composition_prefixes(
        |control| {
            let token = decode_connector_payloads(&definitions, SOURCE, limits(), control)?;
            decoded_observed(&token, |work| token.retained_floor_observed(work)).map(|_| ())
        },
        true,
    );
}

#[test]
fn source_reference_ordinary_missing_and_alias_tails_preserve_each_original_control_cause() {
    let original = payload(ConnectorCodecCategory::ReadColumn, &[1]);
    let foreign = original.clone();
    for aliases in [false, true] {
        let inputs = if aliases {
            vec![(0, &original), (u32::MAX, &original)]
        } else {
            vec![(0, &original)]
        };
        let run = |control: &Control| {
            let token = encode_connector_payloads(&inputs, SOURCE, limits(), control)?;
            encoded_observed(&token, |work| {
                token.source_id_observed(if aliases { &original } else { &foreign }, work)
            })
            .map(|_| ())
        };
        let baseline = Control::default();
        assert!(run(&baseline).is_err());
        assert_eq!(
            trace(&baseline).last().unwrap(),
            &(CompilePhase::Encode, inputs.len() as u32)
        );
        composition_prefixes(run, false);
    }
    let inputs = [(u32::MAX, &original)];
    composition_prefixes(
        |control| {
            let token = encode_connector_payloads(&inputs, SOURCE, limits(), control)?;
            encoded_observed(&token, |work| token.source_id_observed(&original, work)).map(|_| ())
        },
        true,
    );
}
