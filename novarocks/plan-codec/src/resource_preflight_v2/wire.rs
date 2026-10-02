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

//! Borrowed protobuf wire primitives with caller-owned control observation.
//! Prost remains the grammar authority. Resource policy, generated schema
//! interpretation and entry/tail publication checks belong to the parent.

use super::ResourceModelError;
use novarocks_type_contract::CompileCheckpoints;
use prost::encoding::{WireType, decode_key, decode_varint};

type E = ResourceModelError;

pub(super) struct Cursor<'a> {
    remaining: &'a [u8],
}
impl<'a> Cursor<'a> {
    pub(super) fn new(raw: &'a [u8]) -> Self {
        Self { remaining: raw }
    }
    pub(super) fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
    pub(super) fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    pub(super) fn key(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<(u32, WireType)>, E> {
        work.step()?;
        if self.is_empty() {
            return Ok(None);
        }
        // The locked Prost scalar primitive reads at most ten bytes, accepts
        // non-shortest varints and checks tag/wire grammar exactly as decode.
        decode_key(&mut self.remaining)
            .map(Some)
            .map_err(|_| E::Malformed)
    }
    pub(super) fn varint(&mut self, work: &mut CompileCheckpoints<'_>) -> Result<u64, E> {
        work.step()?;
        decode_varint(&mut self.remaining).map_err(|_| E::Malformed)
    }
    pub(super) fn fixed(
        &mut self,
        width: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a [u8], E> {
        work.step()?;
        let bytes = self.remaining.get(..width).ok_or(E::Malformed)?;
        self.remaining = &self.remaining[width..];
        Ok(bytes)
    }
    pub(super) fn length_delimited(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a [u8], E> {
        let length = usize::try_from(self.varint(work)?).map_err(|_| E::Malformed)?;
        self.fixed(length, work)
    }
    /// Skip one field without heap traversal storage or recursive Rust calls.
    /// Prost scalar primitives may allocate a DecodeError on malformed input;
    /// scanner error scratch remains the host's separate responsibility.
    /// The caller must supply a group bound no greater than the locked Prost
    /// 0.13.5 default recursion allowance. That dependency is built without
    /// `no-recursion-limit`; a feature/version change requires model review.
    #[cfg(test)]
    pub(super) fn skip(
        &mut self,
        wire: WireType,
        tag: u32,
        max_group_depth: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        self.skip_observed(wire, tag, max_group_depth, work, || Ok(()))
    }

    /// Observe every key inside an unknown group, including its closing key.
    /// The outer key is owned by the caller and is not observed a second time.
    pub(super) fn skip_observed(
        &mut self,
        wire: WireType,
        tag: u32,
        max_group_depth: usize,
        work: &mut CompileCheckpoints<'_>,
        mut observe_key: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        const PROST_RECURSION_LIMIT: usize = 100;
        if max_group_depth > PROST_RECURSION_LIMIT {
            return Err(E::Schema(
                "unknown group bound exceeds locked Prost recursion allowance",
            ));
        }
        let mut group_tags = [0_u32; PROST_RECURSION_LIMIT];
        let mut depth = 0;
        let mut current = (tag, wire);
        loop {
            match current.1 {
                WireType::Varint => {
                    self.varint(work)?;
                }
                WireType::SixtyFourBit => {
                    self.fixed(8, work)?;
                }
                WireType::LengthDelimited => {
                    self.length_delimited(work)?;
                }
                WireType::ThirtyTwoBit => {
                    self.fixed(4, work)?;
                }
                WireType::EndGroup => {
                    if depth == 0 || group_tags[depth - 1] != current.0 {
                        return Err(E::Malformed);
                    }
                    depth -= 1;
                }
                WireType::StartGroup => {
                    work.step()?;
                    if depth == PROST_RECURSION_LIMIT {
                        // The locked decoder cannot consume this additional
                        // group. Stop resource projection at the malformed
                        // prefix; Prost remains the wire-legality authority.
                        return Err(E::Malformed);
                    }
                    if depth >= max_group_depth {
                        return Err(E::Limit(
                            "unknown protobuf group depth exceeds caller limit",
                        ));
                    }
                    group_tags[depth] = current.0;
                    depth += 1;
                }
            }
            if depth == 0 {
                return Ok(());
            }
            current = self.key(work)?.ok_or(E::Malformed)?;
            observe_key()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use prost::Message;
    use std::sync::Mutex;

    #[derive(Clone, PartialEq, Message)]
    struct Empty {}
    #[derive(Default)]
    struct Control {
        fail: Option<(usize, CompileControlError)>,
        events: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut events = self.events.lock().unwrap();
            events.push(units);
            if let Some((at, error)) = self.fail
                && at == events.len()
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn observed<T>(
        control: &dyn PureCompileControl,
        body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = body(&mut work);
        if matches!(&result, Err(E::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn skip_all(raw: &[u8], depth: usize, control: &dyn PureCompileControl) -> Result<(), E> {
        observed(control, |work| {
            let mut cursor = Cursor::new(raw);
            while let Some((tag, wire)) = cursor.key(work)? {
                cursor.skip(wire, tag, depth, work)?;
            }
            assert!(cursor.is_empty());
            Ok(())
        })
    }

    #[test]
    fn independent_varints_preserve_noncanonical_forms_and_full_u64_range() {
        for (bytes, value) in [
            (&[0][..], 0),
            (&[0xac, 0x02][..], 300),
            (&[0x80, 0][..], 0),
            (&[0x81, 0x80, 0][..], 1),
            (
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01][..],
                u64::MAX,
            ),
        ] {
            observed(&Control::default(), |work| {
                let mut cursor = Cursor::new(bytes);
                assert_eq!(cursor.varint(work)?, value);
                assert!(cursor.is_empty());
                Ok(())
            })
            .unwrap();
        }
    }
    #[test]
    fn tenth_byte_overflow_continuation_and_truncated_varints_are_malformed() {
        for bytes in [
            &[0x80][..],
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2][..],
            &[
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ][..],
        ] {
            assert!(matches!(
                observed(&Control::default(), |work| Cursor::new(bytes).varint(work)),
                Err(E::Malformed)
            ));
            assert!(prost::encoding::decode_varint(&mut &bytes[..]).is_err());
        }
    }
    #[test]
    fn exact_prost_key_grammar_rejects_zero_tag_unknown_wire_and_oversized_key() {
        for bytes in [
            &[0][..],
            &[0x0e][..],
            &[0x0f][..],
            &[0x80, 0x80, 0x80, 0x80, 0x10][..],
            &[0x80][..],
        ] {
            assert!(matches!(
                observed(&Control::default(), |work| Cursor::new(bytes).key(work)),
                Err(E::Malformed)
            ));
            assert!(prost::encoding::decode_key(&mut &bytes[..]).is_err());
        }
        observed(&Control::default(), |work| {
            assert_eq!(
                Cursor::new(&[0xfd, 0xff, 0xff, 0xff, 0x0f]).key(work)?,
                Some((0x1fff_ffff, WireType::ThirtyTwoBit))
            );
            assert_eq!(Cursor::new(&[]).key(work)?, None);
            Ok(())
        })
        .unwrap();
    }
    #[test]
    fn fixed_and_length_delimited_values_borrow_original_storage_with_checked_truncation() {
        let bytes = [3, 0xaa, 0xbb, 0xcc, 0xdd];
        observed(&Control::default(), |work| {
            let mut cursor = Cursor::new(&bytes);
            let payload = cursor.length_delimited(work)?;
            assert_eq!(payload, &bytes[1..4]);
            assert_eq!(payload.as_ptr(), bytes[1..].as_ptr());
            assert_eq!(cursor.remaining(), &bytes[4..]);
            assert_eq!(cursor.fixed(1, work)?, &bytes[4..]);
            assert!(cursor.is_empty());
            Ok(())
        })
        .unwrap();
        for bytes in [
            &[2, 1][..],
            &[0x80][..],
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1][..],
        ] {
            assert!(matches!(
                observed(&Control::default(), |work| Cursor::new(bytes)
                    .length_delimited(work)),
                Err(E::Malformed)
            ));
        }
        assert!(matches!(
            observed(&Control::default(), |work| Cursor::new(&[1])
                .fixed(usize::MAX, work)),
            Err(E::Malformed)
        ));
    }
    #[test]
    fn every_unknown_wire_kind_and_noncanonical_varint_matches_prost_acceptance() {
        let raw = [
            0x40, 0x80, 0, 0x49, 1, 2, 3, 4, 5, 6, 7, 8, 0x52, 2, 0xaa, 0xbb, 0x5b, 0x60, 1, 0x5c,
            0x6d, 1, 2, 3, 4,
        ];
        assert!(Empty::decode(&raw[..]).is_ok());
        skip_all(&raw, 4, &Control::default()).unwrap();
        let original = [0x98, 0x06, 0x80, 0x00, 0xa2, 0x06, 0x01, 0xff];
        assert!(Empty::decode(&original[..]).is_ok());
        skip_all(&original, 0, &Control::default()).unwrap();
    }
    #[test]
    fn unknown_groups_match_end_tags_and_use_explicit_depth_without_native_default() {
        let valid = [0x0b, 0x13, 0x18, 0x96, 1, 0x14, 0x0c];
        assert!(Empty::decode(&valid[..]).is_ok());
        skip_all(&valid, 2, &Control::default()).unwrap();
        assert!(matches!(
            skip_all(&valid, 1, &Control::default()),
            Err(E::Limit(_))
        ));
        assert!(matches!(
            skip_all(&valid, 0, &Control::default()),
            Err(E::Limit(_))
        ));
        for invalid in [
            &[0x0b, 0x14][..],
            &[0x0b][..],
            &[0x0c][..],
            &[0x0b, 0x13, 0x0c, 0x14][..],
        ] {
            assert!(Empty::decode(invalid).is_err());
            assert!(matches!(
                skip_all(invalid, 4, &Control::default()),
                Err(E::Malformed)
            ));
        }
    }
    #[test]
    fn fixed_group_stack_bounds_resource_projection_without_claiming_wire_legality() {
        let mut empty_groups = vec![0x0b; 100];
        empty_groups.extend(std::iter::repeat_n(0x0c, 100));
        assert!(Empty::decode(&empty_groups[..]).is_ok());
        skip_all(&empty_groups, 100, &Control::default()).unwrap();

        let mut deepest_scalar = vec![0x0b; 100];
        deepest_scalar.extend_from_slice(&[0x10, 0]);
        deepest_scalar.extend(std::iter::repeat_n(0x0c, 100));
        assert!(Empty::decode(&deepest_scalar[..]).is_err());
        // Scalar children at this depth exhaust Prost's recursion context.
        // The resource cursor may still conservatively charge them: a gate
        // success does not imply that the subsequent decoder will accept.
        skip_all(&deepest_scalar, 100, &Control::default()).unwrap();
        let mut excessive_groups = vec![0x0b; 101];
        excessive_groups.extend(std::iter::repeat_n(0x0c, 101));
        assert!(Empty::decode(&excessive_groups[..]).is_err());
        assert!(matches!(
            skip_all(&excessive_groups, 100, &Control::default()),
            Err(E::Malformed)
        ));
        for bound in [101, usize::MAX] {
            assert!(matches!(
                skip_all(&[0x0b, 0x0c], bound, &Control::default()),
                Err(E::Schema(_))
            ));
        }
    }
    #[test]
    fn packed_varint_and_unknown_group_fields_observe_256_and_all_typed_failures() {
        let packed = [0x80, 0].repeat(320);
        let group = {
            let mut raw = vec![0x0b];
            for _ in 0..320 {
                raw.extend_from_slice(&[0x10, 0x80, 0]);
            }
            raw.push(0x0c);
            raw
        };
        for mode in 0..2 {
            let run = |control: &dyn PureCompileControl| -> Result<(), E> {
                if mode == 0 {
                    observed(control, |work| {
                        let mut cursor = Cursor::new(&packed);
                        while !cursor.is_empty() {
                            assert_eq!(cursor.varint(work)?, 0);
                        }
                        Ok(())
                    })
                } else {
                    skip_all(&group, 1, control)
                }
            };
            let good = Control::default();
            run(&good).unwrap();
            let trace = good.events.into_inner().unwrap();
            assert_eq!(trace[0], 0);
            assert!(trace.contains(&256));
            assert!(trace.iter().any(|units| *units > 0 && *units < 256));
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for at in 1..=trace.len() {
                    let stop = Control {
                        fail: Some((at, error)),
                        events: Mutex::default(),
                    };
                    assert!(matches!(run(&stop),Err(E::Control(actual)) if actual==error));
                    assert_eq!(stop.events.lock().unwrap().len(), at);
                }
            }
        }
    }
    #[test]
    fn unknown_group_key_observer_retains_prefix_counts_and_refuses_before_later_fields() {
        let mut visited = 0;
        let result = observed(&Control::default(), |work| {
            Cursor::new(&[0x10, 1, 0x13, 0x18, 2]).skip_observed(
                WireType::StartGroup,
                1,
                2,
                work,
                || {
                    visited += 1;
                    Ok(())
                },
            )
        });
        assert!(matches!(result, Err(E::Malformed)));
        assert_eq!(visited, 3);
        let mut visited = 0;
        let result = observed(&Control::default(), |work| {
            Cursor::new(&[0x10, 1, 0x0c]).skip_observed(WireType::StartGroup, 1, 1, work, || {
                visited += 1;
                Err(E::Limit("test observed key budget"))
            })
        });
        assert!(matches!(result, Err(E::Limit(_))));
        assert_eq!(visited, 1);
        let mut visited = 0;
        observed(&Control::default(), |work| {
            Cursor::new(&[0x10, 1, 0x0c]).skip_observed(WireType::StartGroup, 1, 1, work, || {
                visited += 1;
                Ok(())
            })
        })
        .unwrap();
        assert_eq!(visited, 2);
    }
    #[test]
    fn prost_merge_loop_reads_shared_suffix_before_reporting_declared_boundary_overrun() {
        #[derive(Clone, PartialEq, Message)]
        struct Child {
            #[prost(string, tag = "1")]
            text: String,
        }
        #[derive(Clone, PartialEq, Message)]
        struct Parent {
            #[prost(message, optional, tag = "1")]
            child: Option<Child>,
        }
        // Child declares two bytes (String key+length). Its String still
        // copies eight following root bytes before merge_loop refuses.
        let raw = [
            0x0a, 2, 0x0a, 8, b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h',
        ];
        let mut actual = Parent::default();
        assert!(actual.merge(&raw[..]).is_err());
        assert_eq!(actual.child.unwrap().text, "abcdefgh");
        observed(&Control::default(), |work| {
            let mut cursor = Cursor::new(&raw);
            assert_eq!(cursor.key(work)?, Some((1, WireType::LengthDelimited)));
            let length = cursor.varint(work)? as usize;
            let end = cursor.remaining().len() - length;
            assert_eq!(cursor.key(work)?, Some((1, WireType::LengthDelimited)));
            assert_eq!(cursor.length_delimited(work)?, b"abcdefgh");
            assert!(cursor.remaining().len() < end);
            Ok(())
        })
        .unwrap();

        #[derive(Clone, PartialEq, Message)]
        struct Packed {
            #[prost(uint64, repeated, tag = "1")]
            values: Vec<u64>,
        }
        // A packed one-byte unterminated varint consumes the next root byte
        // and pushes its value before the final length check detects overrun.
        let raw = [0x0a, 1, 0x80, 0];
        let mut actual = Packed::default();
        assert!(actual.merge(&raw[..]).is_err());
        assert_eq!(actual.values, [0]);
        observed(&Control::default(), |work| {
            let mut cursor = Cursor::new(&raw);
            cursor.key(work)?;
            let length = cursor.varint(work)? as usize;
            let end = cursor.remaining().len() - length;
            assert_eq!(cursor.varint(work)?, 0);
            assert!(cursor.remaining().len() < end);
            Ok(())
        })
        .unwrap();
    }
    #[test]
    fn parent_entry_and_malformed_tail_flush_preserve_original_control_category() {
        let raw = [0x08, 0];
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = Control {
                fail: Some((1, error)),
                events: Mutex::default(),
            };
            assert!(matches!(skip_all(&raw,0,&stop),Err(E::Control(actual)) if actual==error));
            let stop = Control {
                fail: Some((2, error)),
                events: Mutex::default(),
            };
            assert!(
                matches!(skip_all(&[0x0a,2,1],0,&stop),Err(E::Control(actual)) if actual==error)
            );
        }
    }
}
