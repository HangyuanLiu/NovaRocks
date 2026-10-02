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

//! Bounded pure writer recipes. Sealing proves provider-private semantics
//! without opening a writer, resolving credentials or retaining a runtime handle.

use crate::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorEncodedPayload, ConnectorError,
    ConnectorErrorKind, ConnectorWriteBinding, ConnectorWriteInputShape,
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, PureProviderCompileError,
    WRITE_FIELD_ALLOCATION_CHARGE, validate_write_field_schema,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{error::Error, fmt, sync::Arc};

/// Count preflight derived from the existing aggregate schema allocation bound,
/// not from the read recipe's unrelated column limit.
pub const MAX_CONNECTOR_WRITE_INPUT_FIELDS: usize =
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES / WRITE_FIELD_ALLOCATION_CHARGE;

pub const MAX_CONNECTOR_WRITER_HANDLE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteRecipeDraft {
    binding: ConnectorWriteBinding,
    payload: ConnectorEncodedPayload,
    input: Arc<ConnectorWriteInputShape>,
    charged_bytes: usize,
}

impl ConnectorWriteRecipeDraft {
    pub fn try_new(
        binding: ConnectorWriteBinding,
        payload: ConnectorEncodedPayload,
        input: ConnectorWriteInputShape,
    ) -> Result<Self, ConnectorError> {
        if binding.descriptor().instance_id != *binding.catalog_handle().catalog_name() {
            return Err(invalid("writer recipe catalog differs from its instance"));
        }
        payload
            .header()
            .validate_expected::<ConnectorCodecContractError>(
                &binding.descriptor().provider_id,
                binding.catalog_handle(),
                ConnectorCodecCategory::WriteHandle,
                payload.header().codec_revision(),
            )
            .map_err(|error| invalid(error.to_string()))?;
        if payload.payload().len() > MAX_CONNECTOR_WRITER_HANDLE_BYTES {
            return Err(resource("writer recipe handle exceeds the byte limit"));
        }
        if input.field_count() > MAX_CONNECTOR_WRITE_INPUT_FIELDS {
            return Err(resource(
                "writer recipe input exceeds the field count limit",
            ));
        }
        input.validate()?;
        let mut schema_bytes = 0;
        for field in input.fields_iter() {
            if field.field().name().is_empty() {
                return Err(invalid("writer recipe input field name is empty"));
            }
            validate_write_field_schema(field.field(), 1, &mut schema_bytes)?;
        }
        // Field allocation accounting is shared with the existing write
        // relations. Their aggregate schema bound remains authoritative.
        debug_assert!(schema_bytes <= MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES);
        let charged_bytes = schema_bytes
            .checked_add(payload.payload().len())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            // The shared shape moved out of Self. Preserve its structural
            // retained invoice, including the Arc counters; this is not an
            // allocator-size or unique-ownership memory grant.
            .and_then(|bytes| {
                bytes.checked_add(
                    std::mem::size_of::<ConnectorWriteInputShape>()
                        + 2 * std::mem::size_of::<usize>(),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    input.field_count() * std::mem::size_of::<crate::ConnectorWriteFieldToken>(),
                )
            })
            .ok_or_else(|| resource("writer recipe retained charge overflowed"))?;
        let input = input.owned_bounded()?;
        let payload = ConnectorEncodedPayload::new(
            payload.header().clone(),
            bytes::Bytes::copy_from_slice(payload.payload()),
        );
        Ok(Self {
            binding,
            payload,
            input: Arc::new(input),
            charged_bytes,
        })
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }
    pub const fn payload(&self) -> &ConnectorEncodedPayload {
        &self.payload
    }
    pub fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }
}

/// The caller selects the exact installed pure definition. This interface
/// cannot recover or return an execution handle or acquire I/O capabilities.
/// Private decode, canonicalization and copying must observe bounded work;
/// interruption returns Control unchanged, never a Provider diagnostic.
pub trait ConnectorWriteRecipeCompiler: Send + Sync {
    type Error: Error;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>>;
}

#[derive(Debug)]
pub enum ConnectorWriteRecipeCompileError<E: Error> {
    Contract(ConnectorError),
    Provider(E),
    Control(CompileControlError),
}

impl<E: Error> fmt::Display for ConnectorWriteRecipeCompileError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => fmt::Display::fmt(error, f),
            Self::Provider(error) => fmt::Display::fmt(error, f),
            Self::Control(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl<E: Error + 'static> Error for ConnectorWriteRecipeCompileError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Provider(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteRecipe(ConnectorWriteRecipeDraft);

impl ConnectorWriteRecipe {
    pub fn try_compile_with_provider<C: ConnectorWriteRecipeCompiler + ?Sized>(
        draft: &ConnectorWriteRecipeDraft,
        compiler: &C,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConnectorWriteRecipeCompileError<C::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)
            .map_err(ConnectorWriteRecipeCompileError::Control)?;
        let result = (|| {
            work.flush()
                .map_err(ConnectorWriteRecipeCompileError::Control)?;
            let canonical = compiler
                .compile_private(draft, work.control())
                .map_err(|failure| match failure {
                    PureProviderCompileError::Provider(error) => {
                        ConnectorWriteRecipeCompileError::Provider(error)
                    }
                    PureProviderCompileError::Control(error) => {
                        ConnectorWriteRecipeCompileError::Control(error)
                    }
                })?;
            let same_envelope = canonical.binding == draft.binding
                && canonical.payload.header() == draft.payload.header();
            work.step()
                .map_err(ConnectorWriteRecipeCompileError::Control)?;
            if !same_envelope {
                return Err(ConnectorWriteRecipeCompileError::Contract(invalid(
                    "writer recipe public facts changed during provider canonicalization",
                )));
            }
            let same_backing = Arc::ptr_eq(&canonical.input, &draft.input);
            work.step()
                .map_err(ConnectorWriteRecipeCompileError::Control)?;
            // Both drafts own checked immutable input snapshots. Sharing this
            // exact backing proves identity without copying or rescanning all
            // field trees. A new provider-authored snapshot still faces the
            // complete role/token/field comparison.
            let same_input = same_backing
                || canonical
                    .input
                    .same_layout_observed::<CompileControlError>(&draft.input, || work.step())
                    .map_err(ConnectorWriteRecipeCompileError::Control)?;
            if !same_input {
                return Err(ConnectorWriteRecipeCompileError::Contract(invalid(
                    "writer recipe public facts changed during provider canonicalization",
                )));
            }
            Ok(Self(canonical))
        })();
        if matches!(&result, Err(ConnectorWriteRecipeCompileError::Control(_))) {
            return result;
        }
        work.finish()
            .map_err(ConnectorWriteRecipeCompileError::Control)?;
        result
    }
    pub const fn draft(&self) -> &ConnectorWriteRecipeDraft {
        &self.0
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn resource(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    };
    use arrow_schema::{DataType, Field};

    fn draft(input: ConnectorWriteInputShape) -> ConnectorWriteRecipeDraft {
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let binding = ConnectorWriteBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            bytes::Bytes::from_static(b"private writer"),
        );
        ConnectorWriteRecipeDraft::try_new(binding, payload, input).unwrap()
    }
    fn field(index: u8) -> ConnectorWriteFieldBinding {
        ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([index; 32]),
            Field::new(format!("v{index}"), DataType::Int64, false),
        )
    }
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    struct Compiler(u8);
    impl ConnectorWriteRecipeCompiler for Compiler {
        type Error = ConnectorError;
        fn compile_private(
            &self,
            draft: &ConnectorWriteRecipeDraft,
            _: &dyn PureCompileControl,
        ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>> {
            let mut input = draft.input().clone();
            let mut binding = draft.binding.clone();
            let mut payload = draft.payload.clone();
            if let ConnectorWriteInputShape::Data { fields } = &mut input {
                match self.0 {
                    1 => fields.reverse(),
                    2 => fields[0] = field(9),
                    3 => {
                        fields[0] = ConnectorWriteFieldBinding::new(
                            fields[0].token(),
                            fields[0]
                                .field()
                                .clone()
                                .with_metadata([("logical_type".into(), "json".into())].into()),
                        )
                    }
                    4 => {
                        binding = ConnectorWriteBinding::new(
                            binding.descriptor().clone(),
                            CatalogHandle::new(
                                binding.catalog_handle().catalog_name().clone(),
                                CatalogVersion::from_bytes([8; 32]),
                            ),
                        )
                    }
                    5 => {
                        input = ConnectorWriteInputShape::EqualityDelete {
                            equality_fields: fields.clone(),
                        }
                    }
                    _ => {}
                }
            }
            if self.0 == 4 {
                payload = ConnectorEncodedPayload::new(
                    ConnectorEnvelopeHeader::new(
                        binding.descriptor().provider_id.clone(),
                        binding.catalog_handle().clone(),
                        ConnectorCodecCategory::WriteHandle,
                        payload.header().codec_revision(),
                    ),
                    payload.payload().clone(),
                );
            } else {
                payload = ConnectorEncodedPayload::new(
                    payload.header().clone(),
                    bytes::Bytes::from_static(b"canonical writer"),
                );
            }
            ConnectorWriteRecipeDraft::try_new(binding, payload, input)
                .map_err(PureProviderCompileError::Provider)
        }
    }
    #[test]
    fn pure_writer_canonicalization_preserves_all_public_identities() {
        let draft = draft(ConnectorWriteInputShape::Data {
            fields: vec![field(1), field(2)],
        });
        let recipe =
            ConnectorWriteRecipe::try_compile_with_provider(&draft, &Compiler(0), &Control)
                .unwrap();
        assert_eq!(recipe.draft().input(), draft.input());
        assert_eq!(
            recipe.draft().payload().payload().as_ref(),
            b"canonical writer"
        );
        for mutation in 1..=5 {
            assert!(
                matches!(
                    ConnectorWriteRecipe::try_compile_with_provider(
                        &draft,
                        &Compiler(mutation),
                        &Control
                    ),
                    Err(ConnectorWriteRecipeCompileError::Contract(_))
                ),
                "mutation {mutation}"
            );
        }
    }
    #[test]
    fn writer_shape_roles_survive_pure_compilation() {
        for input in [
            ConnectorWriteInputShape::Data {
                fields: vec![field(1)],
            },
            ConnectorWriteInputShape::RowLineage {
                data_fields: vec![field(1)],
                row_identity_fields: vec![field(2)],
            },
            ConnectorWriteInputShape::PositionDelete {
                identity_fields: vec![field(1)],
                partition_source_fields: vec![field(2)],
            },
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: vec![field(1)],
                partition_source_fields: vec![field(2)],
            },
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: vec![field(1)],
            },
        ] {
            let draft = draft(input.clone());
            assert_eq!(
                ConnectorWriteRecipe::try_compile_with_provider(&draft, &Compiler(0), &Control)
                    .unwrap()
                    .draft()
                    .input(),
                &input
            );
        }
    }
    #[test]
    fn writer_recipe_refuses_duplicate_identities_and_unbounded_metadata() {
        let valid = draft(ConnectorWriteInputShape::Data {
            fields: vec![field(1)],
        });
        for input in [
            ConnectorWriteInputShape::Data {
                fields: vec![field(1), field(1)],
            },
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    field(1).token(),
                    Field::new("v", DataType::Int64, false).with_metadata(
                        [(
                            "k".into(),
                            "x".repeat(crate::MAX_WRITE_RELATION_METADATA_VALUE_BYTES + 1),
                        )]
                        .into(),
                    ),
                )],
            },
        ] {
            assert!(
                ConnectorWriteRecipeDraft::try_new(
                    valid.binding.clone(),
                    valid.payload.clone(),
                    input
                )
                .is_err()
            );
        }
    }
    #[test]
    fn dictionary_field_ids_and_ordering_are_exact_public_facts() {
        #[allow(deprecated)]
        let field = |id| {
            Field::new_dict(
                "v",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                id,
                false,
            )
        };
        let first = ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            field(1),
        );
        let second = ConnectorWriteFieldBinding::new(first.token(), field(2));
        assert_ne!(first, second);
    }

    #[test]
    fn recipe_owns_bounded_field_and_metadata_backing() {
        let mut metadata = std::collections::HashMap::with_capacity(10_000);
        metadata.insert("key".into(), "value".into());
        let mut fields = Vec::with_capacity(10_000);
        fields.push(ConnectorWriteFieldBinding::new(
            field(1).token(),
            Field::new("v", DataType::Int64, false).with_metadata(metadata),
        ));
        let draft = draft(ConnectorWriteInputShape::Data { fields });
        let ConnectorWriteInputShape::Data { fields } = draft.input() else {
            unreachable!()
        };
        assert_eq!(fields.capacity(), fields.len());
        assert!(fields[0].field().metadata().capacity() < 10);
        assert_eq!(fields[0].field().metadata()["key"], "value");
    }

    #[test]
    fn writer_draft_clones_share_the_exact_owned_input_without_reauthoring_fields() {
        let original = draft(ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field(1)],
            row_identity_fields: vec![field(2)],
        });
        let copied = original.clone();
        assert!(std::ptr::eq(original.input(), copied.input()));
        assert_eq!(original.charged_bytes(), copied.charged_bytes());
        struct ValidatedCopy;
        impl ConnectorWriteRecipeCompiler for ValidatedCopy {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                draft: &ConnectorWriteRecipeDraft,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                Ok(draft.clone())
            }
        }
        // This fixture tests the neutral seal's immutable ownership only;
        // it does not certify any provider-private bytes as meaningful.
        let sealed =
            ConnectorWriteRecipe::try_compile_with_provider(&original, &ValidatedCopy, &Control)
                .unwrap();
        assert!(std::ptr::eq(original.input(), sealed.draft().input()));
        drop(original);
        assert_eq!(sealed.draft().input(), copied.input());
    }

    #[test]
    fn writer_seal_observes_ordinary_error_tail_and_never_rechecks_primary_control() {
        use std::sync::Mutex;
        struct Trace {
            units: Mutex<Vec<u32>>,
            cause: CompileControlError,
            refuse_tail: bool,
        }
        impl PureCompileControl for Trace {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::ProviderValidation);
                let mut trace = self.units.lock().unwrap();
                trace.push(units);
                if self.refuse_tail && trace.len() == 3 {
                    Err(self.cause)
                } else {
                    Ok(())
                }
            }
        }
        struct Refusal(Option<CompileControlError>);
        impl ConnectorWriteRecipeCompiler for Refusal {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                _: &ConnectorWriteRecipeDraft,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                match self.0 {
                    Some(cause) => Err(PureProviderCompileError::Control(cause)),
                    None => Err(PureProviderCompileError::Provider(invalid(
                        "private refusal",
                    ))),
                }
            }
        }
        let original = draft(ConnectorWriteInputShape::Data {
            fields: vec![field(1)],
        });
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace {
                units: Mutex::default(),
                cause,
                refuse_tail: true,
            };
            let error = ConnectorWriteRecipe::try_compile_with_provider(
                &original,
                &Refusal(None),
                &control,
            )
            .unwrap_err();
            assert!(
                matches!(error, ConnectorWriteRecipeCompileError::Control(actual) if actual == cause)
            );
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
            assert_eq!(control.units.lock().unwrap().as_slice(), &[0, 0, 0]);

            let control = Trace {
                units: Mutex::default(),
                cause,
                refuse_tail: true,
            };
            let error = ConnectorWriteRecipe::try_compile_with_provider(
                &original,
                &Refusal(Some(cause)),
                &control,
            )
            .unwrap_err();
            assert!(
                matches!(error, ConnectorWriteRecipeCompileError::Control(actual) if actual == cause)
            );
            assert_eq!(control.units.lock().unwrap().as_slice(), &[0, 0]);
        }
        let control = Trace {
            units: Mutex::default(),
            cause: CompileControlError::Cancelled,
            refuse_tail: false,
        };
        let error =
            ConnectorWriteRecipe::try_compile_with_provider(&original, &Refusal(None), &control)
                .unwrap_err();
        assert!(matches!(
            error,
            ConnectorWriteRecipeCompileError::Provider(_)
        ));
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<ConnectorError>()
                .unwrap()
                .message(),
            "private refusal"
        );
        assert_eq!(control.units.lock().unwrap().as_slice(), &[0, 0, 0]);
    }
}
