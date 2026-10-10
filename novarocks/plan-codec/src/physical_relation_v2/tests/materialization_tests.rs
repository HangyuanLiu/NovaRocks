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
const OWNED_SOURCE: usize = SOURCE * 2;
fn with_decoded(
    c: &Control,
    wide: bool,
    mut f: impl FnMut(&DecodedRelations<'_, '_, '_>, &[p::Relation]) -> Result<(), Error>,
) -> Result<(), Error> {
    with_sources(c, |sources, reads, types| {
        let bs = decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), c)
            .unwrap();
        let ps = decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), c)
            .unwrap();
        let rs =
            decode_provider_reads(reads.as_wire(), &bs, &ps, 256 * 1024, read_limits()).unwrap();
        let ts = decode_type_table(types.as_wire(), type_limits(), c).unwrap();
        let defs = if wide {
            (0..320)
                .map(|id| {
                    let mut d = expected(0);
                    d.id = id;
                    d
                })
                .collect::<Vec<_>>()
        } else {
            vec![expected(0), expected(1)]
        };
        let d = decode_relations(&defs, &rs, &ts, SOURCE, limits())?;
        f(&d, sources)
    })
}
#[test]
fn selected_relation_uses_original_receiving_grammar_sparse_identity_and_type_loans() {
    let c = Control::default();
    with_decoded(&c, false, |d, sources| {
        for (id, expected) in [(u32::MAX, &sources[0]), (0, &sources[1])] {
            c.arm(None);
            let token = prepare_relation_materialization(d, id, OWNED_SOURCE, limits())?;
            assert_eq!(token.source_id(), id);
            assert_eq!(token.facts().definition_count, 1);
            let output = token.emit()?;
            assert_eq!(&output, expected);
            assert_eq!(materialize_relation(d, id, OWNED_SOURCE, limits())?, output);
            assert_ne!(
                output.schema().as_ptr(),
                d.relation(id)?.unwrap().schema().as_ptr()
            );
            // Only root-owned dictionary Boxes are copied. Nested FieldRef
            // storage retains exactly the original type owner's Arc identity.
            if let (DataType::Struct(a), DataType::Struct(b)) = (
                &d.types().value_type(42).unwrap().data_type,
                &output.schema()[2].ty.data_type,
            ) {
                assert!(Arc::ptr_eq(&a[0], &b[0]));
            } else {
                panic!("Struct lost");
            }
            assert_eq!(output.schema()[0].ty.logical_type, ValueLogicalType::Json);
        }
        Ok(())
    })
    .unwrap();
}
#[test]
fn selected_relation_exact_nine_caps_request_layouts_and_retained_namespace_floor() {
    let c = Control::default();
    with_decoded(&c, false, |d, _| {
        c.arm(None);
        let token = prepare_relation_materialization(d, 0, OWNED_SOURCE, limits())?;
        let facts = *token.facts();
        // Independent locked-layout invoice for the original selected metadata
        // route: temporary index/outerVec; schema/guarantee Vec+Box; dictionary
        // two Boxes; three column and two read Bytes promotion Shared blocks;
        // ordering Vec+Box; exact kind String; coverage Vec+Box.
        // The original bytes source deliberately admits private-field-order
        // padding, rather than claiming the apparent 24-byte struct is exact.
        let alignment = std::mem::align_of::<*mut u8>()
            .max(std::mem::align_of::<usize>())
            .max(std::mem::align_of::<std::sync::atomic::AtomicUsize>());
        let shared = Layout::from_size_align(
            size_of::<*mut u8>()
                + size_of::<usize>()
                + size_of::<std::sync::atomic::AtomicUsize>()
                + 3 * (alignment - 1),
            alignment,
        )
        .unwrap()
        .pad_to_align()
        .size();
        let oracle = Layout::array::<usize>(1).unwrap().size()
            + Layout::array::<p::Relation>(1).unwrap().size()
            + 2 * Layout::array::<p::RelationField>(3).unwrap().size()
            + 2 * Layout::array::<p::PredicateGuarantee>(3).unwrap().size()
            + 2 * size_of::<DataType>()
            + 5 * shared
            + 2 * Layout::array::<p::OrderingKey>(2).unwrap().size()
            + "files / ✓".len()
            + 2 * 3;
        assert_eq!(facts.allocation_requests_upper_bound, 18);
        assert_eq!(facts.allocation_request_bytes_upper_bound, oracle);
        assert_eq!(
            facts.coexisting_source_and_request_bytes_upper_bound,
            OWNED_SOURCE + oracle
        );
        let exact = exact(&facts);
        assert_eq!(
            prepare_relation_materialization(d, 0, OWNED_SOURCE, exact)?.emit()?,
            d.relation(0)?.unwrap().clone()
        );
        for axis in 0..9 {
            assert!(
                matches!(
                    prepare_relation_materialization(d, 0, OWNED_SOURCE, under(exact, axis)),
                    Err(Error::InvalidShape(_))
                ),
                "axis {axis}"
            );
        }
        let floor = d.retained_invoice_floor()?;
        assert!(matches!(
            prepare_relation_materialization(d, 0, floor - 1, limits()),
            Err(Error::InvalidShape(_))
        ));
        assert!(matches!(
            prepare_relation_materialization(d, 99, OWNED_SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        Ok(())
    })
    .unwrap();
}
#[test]
fn selected_relation_every_actual_control_prefix_and_ordinary_tail_keep_first_cause() {
    let c = Control::default();
    with_decoded(&c, false, |d, _| {
        for id in [0, 99] {
            c.arm(None);
            let result = materialize_relation(d, id, OWNED_SOURCE, limits());
            if id == 0 { result?; } else { assert!(matches!(result, Err(Error::InvalidShape(_)))); }
            let baseline = trace(&c);
            assert!(baseline.iter().all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256));
            assert!(baseline.last().unwrap().1 > 0);
            for at in 0..baseline.len() {
                for cause in CAUSES {
                    c.arm(Some((at, cause)));
                    assert!(matches!(materialize_relation(d, id, OWNED_SOURCE, limits()), Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(trace(&c), baseline[..=at]);
                }
            }
            c.arm(None);
        }
        let token = prepare_relation_materialization(d, 0, OWNED_SOURCE, limits())?;
        c.arm(None); token.emit()?;
        let baseline = trace(&c);
        for at in 0..baseline.len() {
            for cause in CAUSES {
                c.arm(None);
                let token = prepare_relation_materialization(d, 0, OWNED_SOURCE, limits())?;
                c.arm(Some((at, cause)));
                assert!(matches!(token.emit(), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(trace(&c), baseline[..=at]);
            }
        }
        c.arm(None);
        Ok(())
    }).unwrap();
}
#[test]
fn selected_relation_real_retained_namespace_scan_is_admitted_before_lookup_and_emission() {
    let c = Control::default();
    with_decoded(&c, true, |d, sources| {
        c.arm(None);
        assert_eq!(materialize_relation(d, 319, OWNED_SOURCE, limits())?, sources[0]);
        let baseline = trace(&c);
        let quantum = baseline.iter().position(|(_, units)| *units == 256).unwrap();
        assert_eq!(baseline[quantum].0, CompilePhase::Decode);
        for at in [0, quantum, baseline.len()-1] {
            for cause in CAUSES {
                c.arm(Some((at, cause)));
                assert!(matches!(materialize_relation(d, 319, OWNED_SOURCE, limits()), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(trace(&c), baseline[..=at]);
            }
        }
        c.arm(None);
        let mut low = limits(); low.max_work = 1024 + 8 * d.source_count() - 1;
        assert!(matches!(prepare_relation_materialization(d, 319, OWNED_SOURCE, low), Err(Error::InvalidShape(_))));
        assert!(!trace(&c).iter().any(|(_, units)| *units == 256));
        c.arm(None);
        Ok(())
    }).unwrap();
}
