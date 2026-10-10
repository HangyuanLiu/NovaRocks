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
//! Original AES mode/key/IV/AAD, source and carrier witnesses, before extraction.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{
    FunctionKind,
    encryption::{eval_aes_decrypt, eval_aes_encrypt},
};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, NullArray,
        StringArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::sync::Arc;
pub(crate) fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
pub(crate) fn binary(v: Vec<Option<&[u8]>>) -> ArrayRef {
    Arc::new(BinaryArray::from(v))
}
pub(crate) fn setup(arrays: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots: Vec<_> = (0..arrays.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect();
    let fields: Vec<_> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays.clone()).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let args = arrays
        .iter()
        .zip(slots)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(s), a.data_type().clone()))
        .collect();
    (arena, args, chunk)
}
pub(crate) fn raw(
    encrypt: bool,
    arrays: Vec<ArrayRef>,
    target: DataType,
) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(arrays);
    let name = if encrypt {
        "aes_encrypt"
    } else {
        "aes_decrypt"
    };
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            args: args.clone(),
        },
        target,
    );
    if encrypt {
        eval_aes_encrypt(&arena, expr, &args, &chunk)
    } else {
        eval_aes_decrypt(&arena, expr, &args, &chunk)
    }
}
pub(crate) fn bytes(a: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    a.as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(<[u8]>::to_vec))
        .collect()
}
fn text(a: &ArrayRef) -> Vec<Option<String>> {
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
pub(crate) const MODES: &[&str] = &[
    "AES_128_ECB",
    "AES_192_ECB",
    "AES_256_ECB",
    "AES_128_CBC",
    "AES_192_CBC",
    "AES_256_CBC",
    "AES_128_CFB",
    "AES_192_CFB",
    "AES_256_CFB",
    "AES_128_CFB1",
    "AES_192_CFB1",
    "AES_256_CFB1",
    "AES_128_CFB8",
    "AES_192_CFB8",
    "AES_256_CFB8",
    "AES_128_CFB128",
    "AES_192_CFB128",
    "AES_256_CFB128",
    "AES_128_OFB",
    "AES_192_OFB",
    "AES_256_OFB",
    "AES_128_CTR",
    "AES_192_CTR",
    "AES_256_CTR",
    "AES_128_GCM",
    "AES_192_GCM",
    "AES_256_GCM",
];
#[test]
fn legacy_aes_original_known_ecb_block_key_fold_and_default_mode() {
    let source = hex::decode("00112233445566778899aabbccddeeff").unwrap();
    let key = hex::decode("000102030405060708090a0b0c0d0e0f").unwrap();
    let out = raw(
        true,
        vec![binary(vec![Some(&source)]), binary(vec![Some(&key)])],
        DataType::Binary,
    )
    .unwrap();
    assert_eq!(
        &bytes(&out)[0].as_ref().unwrap()[..16],
        hex::decode("69c4e0d86a7b0430d8cdb78070b4c55a").unwrap()
    );
    for mode in ["unknown", "aes_128_ecb", "", " AES_128_ECB "] {
        assert_eq!(
            bytes(
                &raw(
                    true,
                    vec![
                        binary(vec![Some(&source)]),
                        binary(vec![Some(&key)]),
                        strings(vec![None]),
                        strings(vec![Some(mode)])
                    ],
                    DataType::Binary
                )
                .unwrap()
            ),
            bytes(&out)
        );
    }
    let extended: Vec<_> = key.iter().copied().chain([0; 16]).collect();
    assert_eq!(
        bytes(
            &raw(
                true,
                vec![binary(vec![Some(&source)]), binary(vec![Some(&extended)])],
                DataType::Binary
            )
            .unwrap()
        ),
        bytes(&out)
    );
}
#[test]
fn legacy_aes_original_all_twenty_seven_modes_roundtrip_and_gcm_embedded_iv() {
    let plaintext = b"a\0b\xff";
    let key = b"a deliberately long key for folding";
    let iv = b"123";
    for &mode in MODES {
        let mut args = vec![
            binary(vec![Some(plaintext.as_slice())]),
            binary(vec![Some(key.as_slice())]),
            binary(vec![Some(iv.as_slice())]),
            strings(vec![Some(mode)]),
        ];
        if mode.ends_with("GCM") {
            args.push(strings(vec![Some("aad")]));
        }
        let ciphertext = raw(true, args, DataType::Binary).unwrap();
        assert!(!ciphertext.is_null(0), "mode {mode}");
        let mut args = vec![
            ciphertext.clone(),
            binary(vec![Some(key.as_slice())]),
            binary(vec![Some(iv.as_slice())]),
            strings(vec![Some(mode)]),
        ];
        if mode.ends_with("GCM") {
            args.push(strings(vec![Some("aad")]));
            let c = bytes(&ciphertext);
            assert_eq!(&c[0].as_ref().unwrap()[..3], iv);
            assert!(c[0].as_ref().unwrap()[3..12].iter().all(|b| *b == 0));
            args[2] = strings(vec![Some("ignored on GCM decrypt")]);
        }
        assert_eq!(
            bytes(&raw(false, args, DataType::Binary).unwrap()),
            vec![Some(plaintext.to_vec())],
            "mode {mode}"
        );
    }
}
#[test]
fn legacy_aes_original_null_empty_key_mode_iv_aad_error_precedence() {
    let src = strings(vec![None, Some(""), Some("a")]);
    let key = strings(vec![Some("k"), Some(""), None]);
    let out = raw(true, vec![src.clone(), key.clone()], DataType::Binary).unwrap();
    assert!(out.is_null(0));
    assert_eq!(bytes(&out)[1].as_ref().unwrap().len(), 16);
    assert!(out.is_null(2));
    assert_eq!(
        bytes(&raw(false, vec![src, key], DataType::Binary).unwrap()),
        vec![None, None, None]
    );
    for encrypt in [true, false] {
        let err = raw(
            encrypt,
            vec![
                strings(vec![Some("a")]),
                strings(vec![Some("k")]),
                strings(vec![None]),
                strings(vec![Some("AES_128_ECB")]),
                strings(vec![Some("")]),
            ],
            DataType::Utf8,
        )
        .unwrap_err();
        assert_eq!(
            err,
            if encrypt {
                "aes_encrypt: requires GCM mode to use AAD parameter"
            } else {
                "aes_decrypt: requires GCM mode to use AAD parameter"
            }
        );
        let out = raw(
            encrypt,
            vec![
                strings(vec![Some("a"), Some("a")]),
                strings(vec![Some("k"), Some("k")]),
                strings(vec![None, None]),
                strings(vec![None, Some("AES_128_CBC")]),
                strings(vec![Some("aad"), Some("aad")]),
            ],
            DataType::Utf8,
        )
        .unwrap();
        assert_eq!(text(&out), vec![None, None]);
    }
}
#[test]
fn legacy_aes_original_arity_and_eager_normalization_before_row_null() {
    for encrypt in [true, false] {
        let label = if encrypt {
            "aes_encrypt"
        } else {
            "aes_decrypt"
        };
        for count in [1, 3, 6] {
            assert_eq!(
                raw(
                    encrypt,
                    (0..count).map(|_| strings(vec![None])).collect(),
                    DataType::Utf8
                )
                .unwrap_err(),
                format!("{label} expects 2, 4, or 5 arguments")
            );
        }
        assert_eq!(
            raw(
                encrypt,
                vec![
                    strings(vec![None]),
                    Arc::new(Int64Array::from(vec![Some(1)]))
                ],
                DataType::Utf8
            )
            .unwrap_err(),
            format!("{label}: arg1 must be VARCHAR or VARBINARY")
        );
        let (mut arena, args, chunk) = setup(vec![strings(vec![None])]);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Encryption(label),
                args: args.clone(),
            },
            DataType::Utf8,
        );
        let args = [args[0], ExprId(usize::MAX)];
        let err = if encrypt {
            eval_aes_encrypt(&arena, expr, &args, &chunk)
        } else {
            eval_aes_decrypt(&arena, expr, &args, &chunk)
        }
        .unwrap_err();
        assert_eq!(err, "invalid ExprId");
    }
}
#[test]
fn legacy_aes_original_wide_byte_carriers_null_type_slice_and_empty() {
    let source = b"a\0b";
    let key = b"key";
    let expected = bytes(
        &raw(
            true,
            vec![
                binary(vec![Some(source.as_slice())]),
                binary(vec![Some(key.as_slice())]),
            ],
            DataType::Binary,
        )
        .unwrap(),
    );
    for input in [
        strings(vec![Some("a\0b")]),
        binary(vec![Some(source.as_slice())]),
        Arc::new(LargeStringArray::from(vec![Some("a\0b")])) as ArrayRef,
        Arc::new(LargeBinaryArray::from(vec![Some(source.as_slice())])) as ArrayRef,
    ] {
        assert_eq!(
            bytes(
                &raw(
                    true,
                    vec![input, strings(vec![Some("key")])],
                    DataType::Binary
                )
                .unwrap()
            ),
            expected
        );
    }
    for encrypt in [true, false] {
        assert_eq!(
            text(
                &raw(
                    encrypt,
                    vec![
                        Arc::new(NullArray::new(2)),
                        strings(vec![Some("k"), Some("k")])
                    ],
                    DataType::Utf8
                )
                .unwrap()
            ),
            vec![None, None]
        );
        assert_eq!(
            raw(
                encrypt,
                vec![strings(vec![]), strings(vec![])],
                DataType::Utf8
            )
            .unwrap()
            .len(),
            0
        );
    }
    let input = strings(vec![Some("guard"), Some("a\0b"), Some("guard")]).slice(1, 1);
    assert_eq!(
        bytes(
            &raw(
                true,
                vec![input, strings(vec![Some("key")])],
                DataType::Binary
            )
            .unwrap()
        ),
        expected
    );
}
#[test]
fn legacy_aes_original_utf8_vs_binary_output_projection() {
    let source = b"\xff\x80\0";
    let key = b"key";
    let enc = raw(
        true,
        vec![
            binary(vec![Some(source.as_slice())]),
            strings(vec![Some("key")]),
        ],
        DataType::Binary,
    )
    .unwrap();
    let cipher = bytes(&enc)[0].clone().unwrap();
    let latin1: String = cipher.iter().map(|b| char::from(*b)).collect();
    assert_eq!(
        text(
            &raw(
                true,
                vec![
                    binary(vec![Some(source.as_slice())]),
                    strings(vec![Some("key")])
                ],
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![Some(latin1)]
    );
    assert_eq!(
        bytes(
            &raw(
                false,
                vec![enc.clone(), binary(vec![Some(key.as_slice())])],
                DataType::Binary
            )
            .unwrap()
        ),
        vec![Some(source.to_vec())]
    );
    assert_eq!(
        text(
            &raw(
                false,
                vec![enc, binary(vec![Some(key.as_slice())])],
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![Some("��\0".into())]
    );
}
#[test]
fn legacy_aes_original_source_receipt_ctr_no_after_the_fact_value_guess() {
    let zeros = [0u8; 16];
    let input = strings(vec![Some("ÿ\u{80}")]);
    let ordinary = raw(
        false,
        vec![
            input,
            binary(vec![Some(&zeros)]),
            binary(vec![Some(&zeros)]),
            strings(vec![Some("AES_128_CTR")]),
        ],
        DataType::Binary,
    )
    .unwrap();
    assert_eq!(bytes(&ordinary), vec![Some(vec![0xa5, 0x56, 0x89, 0x54])]);
    let (mut arena, mut args, chunk) = setup(vec![
        strings(vec![Some("ff80")]),
        binary(vec![Some(&zeros)]),
        binary(vec![Some(&zeros)]),
        strings(vec![Some("AES_128_CTR")]),
    ]);
    let child = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption("to_binary"),
            args: vec![args[0]],
        },
        DataType::Utf8,
    );
    assert_eq!(
        text(&arena.eval(child, &chunk).unwrap()),
        vec![Some("ÿ\u{80}".into())]
    );
    args[0] = child;
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption("aes_decrypt"),
            args: args.clone(),
        },
        DataType::Binary,
    );
    assert_eq!(
        bytes(&eval_aes_decrypt(&arena, expr, &args, &chunk).unwrap()),
        vec![Some(vec![0x99, 0x69])]
    );
}
