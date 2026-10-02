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

//! One provider-owned projection of frozen Iceberg source and selection facts.
//!
//! This owner never opens metadata, enumerates splits or acquires runtime access.

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadInputVersion,
    ConnectorReadProperties, ConnectorReadStaticFacts, SystemTableDistribution,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use prost::Message;
use sha2::{Digest, Sha256};

use super::column_handle::{invalid, unsupported};
use super::{IcebergColumnHandle, IcebergRuntimeRelation, IcebergTableExecuteProcedureHandle};

#[derive(Debug)]
pub(crate) enum ReadStaticFactsError {
    Source(ConnectorError),
    Control(CompileControlError),
}
impl std::fmt::Display for ReadStaticFactsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => std::fmt::Display::fmt(error, f),
            Self::Control(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ReadStaticFactsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}
impl From<ConnectorError> for ReadStaticFactsError {
    fn from(error: ConnectorError) -> Self {
        Self::Source(error)
    }
}
impl From<CompileControlError> for ReadStaticFactsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

struct Work<'a>(Option<CompileCheckpoints<'a>>);
impl Work<'_> {
    fn step(&mut self) -> Result<(), ReadStaticFactsError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ReadStaticFactsError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    // Frozen domain constructors bound their inputs. Prost, DTO authoring and
    // immutable constructor internals remain opaque here: these boundaries do
    // not claim internal cooperation or host allocation authorization.
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, ReadStaticFactsError> {
        self.flush()?;
        let result = operation();
        self.flush()?;
        Ok(result)
    }
    fn copy(&mut self, input: &[u8]) -> Result<Vec<u8>, ReadStaticFactsError> {
        let mut output = self.opaque(|| Vec::with_capacity(input.len()))?;
        for bytes in input.chunks(256) {
            output.extend_from_slice(bytes);
            self.step()?;
        }
        Ok(output)
    }
}

pub(crate) fn iceberg_final_static_facts(
    relation: &IcebergRuntimeRelation,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ConnectorError> {
    match project(relation, &mut Work(None)) {
        Ok(value) => Ok(value),
        Err(ReadStaticFactsError::Source(error)) => Err(error),
        Err(ReadStaticFactsError::Control(_)) => {
            unreachable!("legacy source projection has no control port")
        }
    }
}

pub(crate) fn iceberg_final_static_facts_for_compile(
    relation: &IcebergRuntimeRelation,
    control: &dyn PureCompileControl,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ReadStaticFactsError> {
    let mut work = Work(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::ProviderValidation,
    )?));
    let result = project(relation, &mut work);
    if matches!(&result, Err(ReadStaticFactsError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

fn project(
    relation: &IcebergRuntimeRelation,
    work: &mut Work<'_>,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ReadStaticFactsError> {
    match relation {
        IcebergRuntimeRelation::Table(handle) => {
            let input = work.opaque(|| {
                let mut input = handle.to_proto();
                input.unenforced_predicate = None;
                input.enforced_predicate = None;
                input.limit = None;
                input.projected_columns.clear();
                input.encode_to_vec()
            })?;
            let selection = work.opaque(|| handle.to_proto().encode_to_vec())?;
            static_facts_from_bytes(
                b"iceberg-final-static-table-v1",
                &input,
                &selection,
                unconstrained_read_properties(work)?,
                ConnectorReadArtifactCoverage::NoArtifactInputs,
                work,
            )
        }
        IcebergRuntimeRelation::SystemTable(reference) => {
            let frozen = work.opaque(|| reference.to_proto().encode_to_vec())?;
            let properties = match reference.system_table_type().distribution() {
                SystemTableDistribution::AllNodes => unconstrained_read_properties(work)?,
                SystemTableDistribution::SingleCoordinator => work.opaque(|| {
                    ConnectorReadProperties::try_new(
                        ConnectorReadDistribution::Singleton,
                        Vec::new(),
                    )
                })??,
            };
            let input_digest =
                static_digest(b"iceberg-final-static-system-table-v1", &frozen, work)?;
            let selection_digest =
                static_digest(b"iceberg-final-static-system-table-v1", &frozen, work)?;
            work.opaque(|| {
                ConnectorReadStaticFacts::try_new(
                    ConnectorReadInputVersion::try_new(input_digest.as_slice())?,
                    selection_digest,
                    properties,
                    ConnectorReadArtifactCoverage::NoArtifactInputs,
                    frozen,
                )
            })?
            .map_err(Into::into)
        }
        IcebergRuntimeRelation::ChangeWindow(handle) => {
            let frozen = work.opaque(|| handle.to_proto().encode_to_vec())?;
            static_facts_from_bytes(
                b"iceberg-final-static-change-window-v1",
                &frozen,
                &frozen,
                unconstrained_read_properties(work)?,
                ConnectorReadArtifactCoverage::NoArtifactInputs,
                work,
            )
        }
        IcebergRuntimeRelation::TableExecute(handle) => {
            let frozen = work.opaque(|| handle.to_proto().encode_to_vec())?;
            let selection_digest =
                static_digest(b"iceberg-final-static-table-execute-v1", &frozen, work)?;
            let artifact_coverage = match handle.procedure_handle() {
                Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(rewrite)) => {
                    let content_digest =
                        decode_hex(rewrite.artifact().artifact_digest_hex(), work)?;
                    let evidence = work.copy(rewrite.artifact().artifact_location().as_bytes())?;
                    work.opaque(|| {
                        ConnectorReadArtifactCoverage::exact(
                            selection_digest,
                            content_digest,
                            evidence,
                        )
                    })??
                }
                Some(IcebergTableExecuteProcedureHandle::Optimize(_)) | None => {
                    ConnectorReadArtifactCoverage::NoArtifactInputs
                }
            };
            let input_digest = static_digest(
                b"iceberg-final-static-table-execute-input-v1",
                &frozen,
                work,
            )?;
            let properties = unconstrained_read_properties(work)?;
            work.opaque(|| {
                ConnectorReadStaticFacts::try_new(
                    ConnectorReadInputVersion::try_new(input_digest.as_slice())?,
                    selection_digest,
                    properties,
                    artifact_coverage,
                    Vec::new(),
                )
            })?
            .map_err(Into::into)
        }
        IcebergRuntimeRelation::TableFunction(_) => Err(unsupported(
            "iceberg table-function relations do not publish final static read facts",
        )
        .into()),
        IcebergRuntimeRelation::MergeTable(_) => Err(unsupported(
            "iceberg merge relations do not publish final static read facts",
        )
        .into()),
    }
}

fn static_facts_from_bytes(
    domain: &[u8],
    input_identity: &[u8],
    selection: &[u8],
    properties: ConnectorReadProperties<IcebergColumnHandle>,
    artifact_coverage: ConnectorReadArtifactCoverage,
    work: &mut Work<'_>,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ReadStaticFactsError> {
    let input_digest = static_digest(domain, input_identity, work)?;
    let selection_digest = static_digest(domain, selection, work)?;
    work.opaque(|| {
        ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new(input_digest.as_slice())?,
            selection_digest,
            properties,
            artifact_coverage,
            Vec::new(),
        )
    })?
    .map_err(Into::into)
}

fn unconstrained_read_properties(
    work: &mut Work<'_>,
) -> Result<ConnectorReadProperties<IcebergColumnHandle>, ReadStaticFactsError> {
    work.opaque(|| {
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, Vec::new())
    })?
    .map_err(Into::into)
}

fn static_digest(
    domain: &[u8],
    bytes: &[u8],
    work: &mut Work<'_>,
) -> Result<[u8; 32], ReadStaticFactsError> {
    let mut digest = Sha256::new();
    digest.update(domain);
    work.step()?;
    digest.update((bytes.len() as u64).to_be_bytes());
    work.step()?;
    for chunk in bytes.chunks(256) {
        digest.update(chunk);
        work.step()?;
    }
    let result = digest.finalize().into();
    work.step()?;
    Ok(result)
}

#[cfg(test)]
pub(crate) fn decode_sha256_hex(value: &str) -> Result<[u8; 32], ConnectorError> {
    match decode_hex(value, &mut Work(None)) {
        Ok(value) => Ok(value),
        Err(ReadStaticFactsError::Source(error)) => Err(error),
        Err(ReadStaticFactsError::Control(_)) => {
            unreachable!("legacy digest parsing has no control port")
        }
    }
}
fn decode_hex(value: &str, work: &mut Work<'_>) -> Result<[u8; 32], ReadStaticFactsError> {
    if value.len() != 64 {
        return Err(invalid("iceberg artifact digest is not a SHA-256 hex value").into());
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let parsed = value
            .get(index * 2..index * 2 + 2)
            .ok_or_else(|| invalid("iceberg artifact digest is not valid hexadecimal"))
            .and_then(|pair| {
                u8::from_str_radix(pair, 16)
                    .map_err(|_| invalid("iceberg artifact digest is not valid hexadecimal"))
            });
        work.step()?;
        *byte = parsed?;
    }
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_boundary::final_static_facts_tests::table_handle;
    use crate::typed_read::{
        IcebergChangeWindowHandle, IcebergChangeWindowHandleParams, IcebergProcedureId,
        IcebergRewriteArtifactContentId, IcebergRewritePositionDeleteFilesHandle,
        IcebergSystemTableReference, IcebergSystemTableReferenceParams, IcebergSystemTableType,
        IcebergTableExecuteHandle, IcebergTableExecuteHandleParams, TableChangesFunctionHandle,
        TableChangesFunctionHandleParams,
    };
    use novarocks_spi::connector::read_stack::{ConnectorTableHandle as _, SchemaTableName};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            assert!(units <= 256);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((index, error)) = self.refusal {
                assert!(
                    calls.len() <= index,
                    "control must not be called after refusal"
                );
                if calls.len() == index {
                    return Err(error);
                }
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn system(kind: IcebergSystemTableType) -> IcebergRuntimeRelation {
        IcebergRuntimeRelation::SystemTable(
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "orders").unwrap(),
                system_table_type: kind,
                metadata_file_location: "s3://warehouse/db/orders/metadata/v1.json".to_string(),
                table_uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string(),
                snapshot_id: Some(41),
            })
            .unwrap(),
        )
    }
    fn rewrite() -> IcebergRuntimeRelation {
        let table = table_handle();
        let procedure = IcebergRewritePositionDeleteFilesHandle::try_new(
            table.clone(),
            IcebergRewriteArtifactContentId::try_new(
                "s3://warehouse/rewrite.json",
                "01".repeat(32),
            )
            .unwrap(),
            "02".repeat(32),
        )
        .unwrap();
        IcebergRuntimeRelation::TableExecute(
            IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
                schema_table_name: table.schema_table_name().clone(),
                procedure_id: IcebergProcedureId::RewritePositionDeleteFiles,
                table_location: table.table_location().to_string(),
                procedure_handle: Some(
                    IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(procedure),
                ),
            })
            .unwrap(),
        )
    }
    fn change() -> IcebergRuntimeRelation {
        let table = table_handle();
        let schema = table.parse_table_schema().unwrap();
        let columns = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| IcebergColumnHandle::base_column(field))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        IcebergRuntimeRelation::ChangeWindow(
            IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
                schema_table_name: table.schema_table_name().clone(),
                table_schema_json: table.table_schema_json().to_string(),
                columns,
                name_mapping_json: None,
                from_snapshot_id_exclusive: 41,
                to_snapshot_id_inclusive: 42,
                from_read_domain: crate::delete_semantics::test_read_domain(&schema, &[], 41),
                to_read_domain: crate::delete_semantics::test_read_domain(&schema, &[], 42),
                partition_spec_jsons: BTreeMap::new(),
            })
            .unwrap(),
        )
    }
    fn unsupported_function() -> IcebergRuntimeRelation {
        let table = table_handle();
        let schema = table.parse_table_schema().unwrap();
        let columns = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| IcebergColumnHandle::base_column(field))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        IcebergRuntimeRelation::TableFunction(
            TableChangesFunctionHandle::try_new(TableChangesFunctionHandleParams {
                schema_table_name: table.schema_table_name().clone(),
                table_schema_json: table.table_schema_json().to_string(),
                columns,
                name_mapping_json: None,
                start_snapshot_id: 41,
                end_snapshot_id: 42,
            })
            .unwrap(),
        )
    }

    #[test]
    fn compile_source_facts_match_the_actual_legacy_author_for_all_four_producers() {
        for relation in [
            IcebergRuntimeRelation::Table(table_handle()),
            system(IcebergSystemTableType::Files),
            system(IcebergSystemTableType::Snapshots),
            change(),
            rewrite(),
        ] {
            let control = Control::default();
            let actual = iceberg_final_static_facts_for_compile(&relation, &control).unwrap();
            assert_eq!(actual, iceberg_final_static_facts(&relation).unwrap());
            assert!(control.calls.lock().unwrap().len() > 1);
        }
    }
    #[test]
    fn metadata_distribution_coverage_and_rewrite_exact_artifact_are_preserved() {
        for (kind, singleton) in [
            (IcebergSystemTableType::Files, false),
            (IcebergSystemTableType::Snapshots, true),
        ] {
            let relation = system(kind);
            let facts =
                iceberg_final_static_facts_for_compile(&relation, &Control::default()).unwrap();
            assert_eq!(
                matches!(
                    facts.properties().distribution(),
                    ConnectorReadDistribution::Singleton
                ),
                singleton
            );
            let IcebergRuntimeRelation::SystemTable(reference) = relation else {
                unreachable!()
            };
            assert_eq!(
                facts.coverage_evidence(),
                reference.to_proto().encode_to_vec()
            );
        }
        let facts =
            iceberg_final_static_facts_for_compile(&rewrite(), &Control::default()).unwrap();
        let ConnectorReadArtifactCoverage::Exact {
            source_selection_digest,
            content_digest,
            evidence,
        } = facts.artifact_coverage()
        else {
            panic!("rewrite must retain exact artifact coverage")
        };
        assert_eq!(*source_selection_digest, facts.selection_digest());
        assert_eq!(*content_digest, [1; 32]);
        assert_eq!(evidence.as_ref(), b"s3://warehouse/rewrite.json");
        assert!(facts.coverage_evidence().is_empty());
    }
    #[test]
    fn source_fact_entry_preserves_all_three_primary_controls() {
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((1, cause)),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&rewrite(), &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), [0]);
        }
    }
    #[test]
    fn source_fact_hashing_refuses_at_256_real_chunks_without_publication_or_recheck() {
        let mut raw = table_handle().to_proto();
        raw.table_schema_json.push_str(&" ".repeat(256 * 260));
        let relation = IcebergRuntimeRelation::Table(
            super::super::IcebergTableHandle::from_proto(&raw).unwrap(),
        );
        let baseline = Control::default();
        iceberg_final_static_facts_for_compile(&relation, &baseline).unwrap();
        let calls = baseline.calls.lock().unwrap();
        let at = calls.iter().position(|units| *units == 256).unwrap() + 1;
        drop(calls);
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&relation, &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(control.calls.lock().unwrap().last(), Some(&256));
            assert_eq!(control.calls.lock().unwrap().len(), at);
        }
    }
    #[test]
    fn unsupported_source_keeps_legacy_diagnostic_but_observes_ordinary_error_tail() {
        let relation = unsupported_function();
        let legacy = iceberg_final_static_facts(&relation).unwrap_err();
        let error =
            iceberg_final_static_facts_for_compile(&relation, &Control::default()).unwrap_err();
        let ReadStaticFactsError::Source(error) = error else {
            panic!("ordinary source error expected")
        };
        assert_eq!(error.kind(), legacy.kind());
        assert_eq!(error.to_string(), legacy.to_string());
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((2, cause)),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&relation, &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), [0, 0]);
        }
    }
    #[test]
    fn successful_source_fact_tail_refusal_never_publishes() {
        let relation = system(IcebergSystemTableType::Snapshots);
        let baseline = Control::default();
        iceberg_final_static_facts_for_compile(&relation, &baseline).unwrap();
        let at = baseline.calls.lock().unwrap().len();
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&relation, &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(control.calls.lock().unwrap().len(), at);
        }
    }
}
