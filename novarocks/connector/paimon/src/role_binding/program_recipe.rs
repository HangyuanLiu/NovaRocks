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

//! Full frozen-input validation, independent of Paimon runtime capabilities.

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::{DataType, Field, TimeUnit};
use bytes::Bytes;
use novarocks_connector_contract::{
    ConnectorReadArtifactCoverage, ConnectorReadBinding, ConnectorReadDistribution,
    ConnectorReadProgramCompiler, ConnectorReadRelationKind, ConnectorReadRelationRecipeDraft,
    ConnectorReadWorkSource, FrozenConnectorRead, MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES,
    PureProviderCompileError,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorFieldPath, ConnectorReadRelationPayload,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueLogicalType,
    arrow_fields_exact_observed,
};
use sha2::{Digest, Sha256};

use super::{
    MAX_PRIVATE_READ_BYTES, MAX_PRIVATE_RETAINED_BYTES, PAIMON_READ_CODEC_REVISION,
    PaimonReadRecipeCompiler, decode_column, decode_read_view, decode_table, encode_column,
    encode_read_view, encode_table,
};
use crate::{PROVIDER_ID, domain::PaimonColumn, schema::PaimonDataType};

type Failure = PureProviderCompileError<ConnectorCodecError>;
const FIELD_ID_KEY: &str = "PARQUET:field_id";
const OBSERVED_BYTES: usize = 1024;

impl ConnectorReadProgramCompiler for PaimonReadRecipeCompiler {
    type Error = ConnectorCodecError;

    fn compile_private(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, Failure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = compile(frozen, &mut work);
        // A primary interruption is final. Ordinary errors still observe their
        // completed tail, and no canonical draft is published before finish.
        if matches!(&result, Err(Failure::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}

fn compile(
    frozen: &FrozenConnectorRead,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConnectorReadRelationRecipeDraft, Failure> {
    let scan = frozen.scan();
    let draft = scan.recipe();
    let public = frozen.public_facts();
    let source = public.source();
    work.step()?;
    if draft.binding().descriptor().provider_id.as_str() != PROVIDER_ID
        || draft.relation().kind() != ConnectorReadRelationKind::Table
        || scan.work_source() != ConnectorReadWorkSource::RuntimeSplits
        || public.metadata_kind().is_some()
    {
        return Err(invalid(
            "relation",
            "Paimon requires an ordinary runtime-split table read",
        ));
    }
    // This owner declines filtering and supplies no partition/order/coverage
    // promise. Engine residual predicates and optional RF hints remain intact.
    if !scan.enforced_predicate().is_all()
        || !matches!(
            source.properties().distribution(),
            ConnectorReadDistribution::Unconstrained
        )
        || !source.properties().ordering().is_empty()
        || !matches!(
            source.artifact_coverage(),
            ConnectorReadArtifactCoverage::NoArtifactInputs
        )
        || !source.coverage_evidence().is_empty()
    {
        return Err(invalid(
            "source",
            "Paimon read contains an unsupported provider guarantee",
        ));
    }
    if !public.schema().metadata().is_empty() {
        return Err(invalid(
            "schema",
            "Paimon source schema has unexpected schema metadata",
        ));
    }
    let table = decode(
        draft.binding(),
        draft.relation().table(),
        ConnectorCodecCategory::ReadTable,
        work,
        decode_table,
    )?;
    let view = decode(
        draft.binding(),
        draft.relation().view(),
        ConnectorCodecCategory::ReadView,
        work,
        decode_read_view,
    )?;
    if !bytes_equal(
        table.location().as_bytes(),
        view.table_location().as_bytes(),
        work,
    )? {
        return Err(invalid(
            "relation",
            "Paimon table and frozen view name different locations",
        ));
    }

    // Exactly the role-binding final_static_facts input-version recipe. The
    // schema fingerprint is already frozen; projected columns cannot recreate
    // the full schema, primary keys or read options that authored that digest.
    let mut hash = Sha256::new();
    digest_bytes(&mut hash, b"novarocks-paimon-input-version-v1", work)?;
    digest_bytes(&mut hash, view.table_location().as_bytes(), work)?;
    match view.snapshot_id() {
        Some(id) => {
            hash.update([1]);
            hash.update(id.to_be_bytes());
        }
        None => hash.update([0]),
    }
    work.step()?;
    hash.update(view.schema_id().to_be_bytes());
    digest_bytes(&mut hash, view.schema_fingerprint(), work)?;
    let version: [u8; 32] = hash.finalize().into();
    work.step()?;
    if !bytes_equal(source.input_version().as_bytes(), &version, work)?
        || source.selection_digest() != *view.read_recipe_digest()
    {
        return Err(invalid(
            "source",
            "Paimon frozen source version or selection differs from its view",
        ));
    }

    // Duplicate projections are legal. A repeated source identity must retain
    // the same source descriptor; output_ordinal is the original schema ordinal,
    // not the ordinal of this reordered projection. Runtime rebases it later.
    let mut by_id = BTreeMap::<i32, PaimonColumn>::new();
    let mut by_ordinal = BTreeMap::<u32, i32>::new();
    let mut by_name = BTreeMap::<String, i32>::new();
    let mut columns = Vec::with_capacity(draft.columns().len());
    for (ordinal, payload) in draft.columns().iter().enumerate() {
        work.step()?;
        let column = decode(
            draft.binding(),
            payload,
            ConnectorCodecCategory::ReadColumn,
            work,
            decode_column,
        )?;
        let field = &public.schema().fields()[ordinal];
        if public.logical_types()[ordinal] != ValueLogicalType::Physical {
            return Err(invalid(
                "columns",
                "Paimon PAI-1 source has a non-physical logical domain",
            ));
        }
        work.flush()?;
        let expected = column_field(&column);
        work.step()?;
        if !arrow_fields_exact_observed(&expected, field, || work.step())? {
            return Err(invalid(
                "columns",
                "Paimon private column differs from its complete public field",
            ));
        }
        if let Some(existing) = by_id.get(&column.field_id()) {
            if existing.data_type() != column.data_type()
                || existing.nullable() != column.nullable()
                || existing.output_ordinal() != column.output_ordinal()
                || !bytes_equal(existing.name().as_bytes(), column.name().as_bytes(), work)?
            {
                return Err(invalid(
                    "columns",
                    "Paimon repeated field ID changes its source descriptor",
                ));
            }
        } else {
            // Names are individually bounded by the admitted public schema.
            // BTree comparisons are opaque; observe before and after insertion.
            work.flush()?;
            if by_ordinal
                .insert(column.output_ordinal(), column.field_id())
                .is_some_and(|id| id != column.field_id())
                || by_name
                    .insert(column.name().to_owned(), column.field_id())
                    .is_some_and(|id| id != column.field_id())
            {
                return Err(invalid(
                    "columns",
                    "Paimon source name or original ordinal names multiple field IDs",
                ));
            }
            by_id.insert(column.field_id(), column.clone());
            work.step()?;
            work.flush()?;
        }
        columns.push(canonical(payload, work, || encode_column(&column))?);
    }
    let table = canonical(draft.relation().table(), work, || encode_table(&table))?;
    let view = canonical(draft.relation().view(), work, || encode_read_view(&view))?;
    work.flush()?;
    let result = ConnectorReadRelationRecipeDraft::try_new(
        draft.binding().clone(),
        ConnectorReadRelationPayload::new(ConnectorReadRelationKind::Table, table, view),
        columns,
    )
    .map_err(|error| invalid("recipe", error.to_string()));
    work.step()?;
    work.flush()?;
    result
}

fn decode<T>(
    binding: &ConnectorReadBinding,
    payload: &ConnectorEncodedPayload,
    category: ConnectorCodecCategory,
    work: &mut CompileCheckpoints<'_>,
    decode_private: impl FnOnce(
        &[u8],
        &mut ConnectorDecodeContext<'_>,
    ) -> Result<T, ConnectorCodecError>,
) -> Result<T, Failure> {
    work.step()?;
    if payload.payload().len() > MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES {
        return Err(Failure::Provider(ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_payload"),
            ConnectorCodecErrorKind::Capacity,
            "Paimon private payload exceeds the frozen recipe bound",
        )));
    }
    let revision = ConnectorCodecRevision::try_new(PAIMON_READ_CODEC_REVISION)
        .expect("Paimon read codec revision is non-zero");
    payload
        .header()
        .validate_expected::<ConnectorCodecError>(
            &binding.descriptor().provider_id,
            binding.catalog_handle(),
            category,
            revision,
        )
        .map_err(codec_failure)?;
    let limits = ConnectorDecodeLimits::try_new(
        MAX_PRIVATE_READ_BYTES,
        MAX_PRIVATE_RETAINED_BYTES,
        MAX_PRIVATE_READ_BYTES,
        1_000_000,
        64,
    )
    .expect("Paimon recipe decode limits are finite");
    work.flush()?;
    let mut ledger = ConnectorDecodeLedger::new(limits);
    let mut context =
        ConnectorDecodeContext::try_new_for_compile(payload.header(), &mut ledger, work.control())
            .map_err(codec_failure)?;
    let result = decode_private(payload.payload(), &mut context).map_err(codec_failure);
    if matches!(&result, Err(Failure::Control(_))) {
        return result;
    }
    context.flush_compile_control().map_err(codec_failure)?;
    result
}

fn canonical(
    payload: &ConnectorEncodedPayload,
    work: &mut CompileCheckpoints<'_>,
    encode: impl FnOnce() -> Result<Bytes, ConnectorCodecError>,
) -> Result<ConnectorEncodedPayload, Failure> {
    // Prost encode is opaque. Its decoded source is finitely bounded, and
    // these checkpoints cover its entry/exit, not cooperation inside Prost
    // or an allocation grant from a host memory authority.
    work.flush()?;
    let result = encode().map_err(codec_failure);
    if matches!(&result, Err(Failure::Control(_))) {
        return result.map(|bytes| ConnectorEncodedPayload::new(payload.header().clone(), bytes));
    }
    work.step()?;
    work.flush()?;
    let bytes = result?;
    if bytes.len() > MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES {
        return Err(Failure::Provider(ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_payload"),
            ConnectorCodecErrorKind::Capacity,
            "Paimon canonical payload exceeds the frozen recipe bound",
        )));
    }
    Ok(ConnectorEncodedPayload::new(
        payload.header().clone(),
        bytes,
    ))
}

fn codec_failure(error: ConnectorCodecError) -> Failure {
    match error.compile_control_error() {
        Some(cause) => Failure::Control(cause),
        None => Failure::Provider(error),
    }
}

fn invalid(path: &'static str, message: impl AsRef<str>) -> Failure {
    Failure::Provider(ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::InconsistentFields,
        message,
    ))
}

fn bytes_equal(
    left: &[u8],
    right: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, Failure> {
    work.step()?;
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left
        .chunks(OBSERVED_BYTES)
        .zip(right.chunks(OBSERVED_BYTES))
    {
        let equal = left == right;
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn digest_bytes(
    hash: &mut Sha256,
    bytes: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    hash.update((bytes.len() as u64).to_be_bytes());
    work.step()?;
    for chunk in bytes.chunks(OBSERVED_BYTES) {
        hash.update(chunk);
        work.step()?;
    }
    Ok(())
}

/// The admitted PAI-1 projection of the SDK build_target_arrow_schema author.
/// Field ID metadata is source identity; scan variables are separate aliases.
fn column_field(column: &PaimonColumn) -> Field {
    let data_type = match column.data_type() {
        PaimonDataType::Boolean => DataType::Boolean,
        PaimonDataType::Int8 => DataType::Int8,
        PaimonDataType::Int16 => DataType::Int16,
        PaimonDataType::Int32 => DataType::Int32,
        PaimonDataType::Int64 => DataType::Int64,
        PaimonDataType::Float32 => DataType::Float32,
        PaimonDataType::Float64 => DataType::Float64,
        PaimonDataType::Decimal128 { precision, scale } => {
            DataType::Decimal128(precision, scale as i8)
        }
        PaimonDataType::Utf8 => DataType::Utf8,
        PaimonDataType::Binary => DataType::Binary,
        PaimonDataType::Date32 => DataType::Date32,
        PaimonDataType::TimestampMillis { .. } => DataType::Timestamp(TimeUnit::Millisecond, None),
        PaimonDataType::TimestampMicros { .. } => DataType::Timestamp(TimeUnit::Microsecond, None),
    };
    Field::new(column.name(), data_type, column.nullable()).with_metadata(HashMap::from([(
        FIELD_ID_KEY.to_owned(),
        column.field_id().to_string(),
    )]))
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::{Arc, Mutex},
    };

    use arrow::datatypes::Schema;
    use novarocks_connector_contract::{
        CatalogHandle, CatalogVersion, ConnectorEnvelopeHeader, ConnectorExpression,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorReadInputVersion, ConnectorReadProgramCompileError, ConnectorReadProgramRecipe,
        ConnectorReadProperties, ConnectorReadPublicFacts, ConnectorReadStaticFacts,
        ConnectorValue, ConnectorValueType, Domain, FrozenConnectorScan, PureProviderCatalogError,
        PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
        PureProviderProgramError, ScanColumnId, StaticScanAssignment, StaticScanDynamicFilter,
        TupleDomain, connector_type_for_arrow,
    };
    use novarocks_spi::connector::read_stack::SchemaTableName;
    use novarocks_type_contract::{CompileControlError, arrow_fields_exact};
    use paimon::spec::{self, DataField, TableSchema};

    use super::*;
    use crate::domain::{PaimonReadView, PaimonTable};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        fail: Option<(CompileControlError, Stop)>,
    }
    #[derive(Clone, Copy)]
    enum Stop {
        Entry,
        Quantum,
        Call(usize),
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((cause, stop)) = self.fail {
                let refuse = match stop {
                    Stop::Entry => calls.len() == 1,
                    Stop::Quantum => units == 256,
                    Stop::Call(index) => calls.len() == index,
                };
                if refuse {
                    return Err(cause);
                }
            }
            Ok(())
        }
    }

    struct Fixture {
        binding: ConnectorReadBinding,
        table: PaimonTable,
        view: PaimonReadView,
        columns: Vec<PaimonColumn>,
        fields: Vec<Field>,
        logical: Vec<ValueLogicalType>,
        source: ConnectorReadStaticFacts<ScanColumnId>,
        enforced: TupleDomain<ScanColumnId>,
        residual: TupleDomain<ScanColumnId>,
        remaining: Option<ConnectorExpression>,
        filters: Vec<StaticScanDynamicFilter>,
        kind: ConnectorReadRelationKind,
    }
    impl Fixture {
        fn projected() -> Self {
            Self::projected_snapshot(Some(41))
        }
        fn projected_snapshot(snapshot: Option<i64>) -> Self {
            let schema = TableSchema::new(
                7,
                &spec::Schema::builder()
                    .column(
                        "id",
                        spec::DataType::BigInt(spec::BigIntType::with_nullable(false)),
                    )
                    .column(
                        "value",
                        spec::DataType::VarChar(
                            spec::VarCharType::with_nullable(true, 100).unwrap(),
                        ),
                    )
                    .build()
                    .unwrap(),
            );
            Self::from_schema_snapshot(&schema, &[1, 0, 1], snapshot)
        }
        fn from_schema(schema: &TableSchema, selection: &[usize]) -> Self {
            Self::from_schema_snapshot(schema, selection, Some(41))
        }
        fn from_schema_snapshot(
            schema: &TableSchema,
            selection: &[usize],
            snapshot: Option<i64>,
        ) -> Self {
            let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
            let binding = ConnectorReadBinding::new(
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse(PROVIDER_ID).unwrap(),
                    instance_id: instance.clone(),
                },
                CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
            );
            let all_columns = crate::metadata::columns_from_schema(schema).unwrap();
            let primary_ids = schema
                .primary_keys()
                .iter()
                .map(|name| {
                    all_columns
                        .iter()
                        .find(|column| column.name() == name)
                        .unwrap()
                        .field_id()
                })
                .collect::<Vec<_>>();
            let properties = schema
                .options()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let options = crate::options::PaimonReadOptions::analyze(
                &properties,
                &all_columns,
                &primary_ids,
                &[],
            )
            .unwrap();
            let table = PaimonTable::try_new(
                SchemaTableName::try_new("db", "t").unwrap(),
                "s3://bucket/db/t",
                options.merge_engine,
                options.bucket_mode,
                primary_ids,
                vec![],
            )
            .unwrap();
            let view = PaimonReadView::try_new(
                table.location(),
                snapshot,
                schema.id(),
                super::super::schema_fingerprint(schema).unwrap(),
                super::super::read_recipe_digest(&table, snapshot, &options, &all_columns),
                options.sequence_field_id,
            )
            .unwrap();
            let sdk_fields = selection
                .iter()
                .map(|&i| schema.fields()[i].clone())
                .collect::<Vec<_>>();
            let arrow = paimon::arrow::build_target_arrow_schema(&sdk_fields).unwrap();
            let fields = arrow
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();
            let columns = selection.iter().map(|&i| all_columns[i].clone()).collect();
            let source = source(&view);
            Self {
                binding,
                table,
                view,
                columns,
                logical: vec![ValueLogicalType::Physical; fields.len()],
                fields,
                source,
                enforced: TupleDomain::all(),
                residual: TupleDomain::all(),
                remaining: None,
                filters: vec![],
                kind: ConnectorReadRelationKind::Table,
            }
        }
        fn payload(
            &self,
            category: ConnectorCodecCategory,
            bytes: Bytes,
        ) -> ConnectorEncodedPayload {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    self.binding.descriptor().provider_id.clone(),
                    self.binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(PAIMON_READ_CODEC_REVISION).unwrap(),
                ),
                bytes,
            )
        }
        fn draft(&self) -> ConnectorReadRelationRecipeDraft {
            ConnectorReadRelationRecipeDraft::try_new(
                self.binding.clone(),
                ConnectorReadRelationPayload::new(
                    self.kind,
                    self.payload(
                        ConnectorCodecCategory::ReadTable,
                        encode_table(&self.table).unwrap(),
                    ),
                    self.payload(
                        ConnectorCodecCategory::ReadView,
                        encode_read_view(&self.view).unwrap(),
                    ),
                ),
                self.columns
                    .iter()
                    .map(|column| {
                        self.payload(
                            ConnectorCodecCategory::ReadColumn,
                            encode_column(column).unwrap(),
                        )
                    })
                    .collect(),
            )
            .unwrap()
        }
        fn frozen_draft(&self, draft: ConnectorReadRelationRecipeDraft) -> FrozenConnectorRead {
            let scan = FrozenConnectorScan::try_new(
                draft,
                self.fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        StaticScanAssignment::new(
                            Arc::from(format!("alias_{i}")),
                            connector_type_for_arrow(field.data_type()).unwrap(),
                        )
                    })
                    .collect(),
                self.enforced.clone(),
                self.residual.clone(),
                self.remaining.clone(),
                self.filters.clone(),
                NonZeroU64::new(1024).unwrap(),
                NonZeroU64::new(1 << 20).unwrap(),
                ConnectorReadWorkSource::RuntimeSplits,
            )
            .unwrap();
            let public = ConnectorReadPublicFacts::try_new(
                self.source.clone(),
                None,
                Schema::new(self.fields.clone()),
                self.logical.clone(),
            )
            .unwrap();
            FrozenConnectorRead::try_new(scan, public).unwrap()
        }
        fn frozen(&self) -> FrozenConnectorRead {
            self.frozen_draft(self.draft())
        }
    }
    fn source(view: &PaimonReadView) -> ConnectorReadStaticFacts<ScanColumnId> {
        // Independent reproduction of the existing source-facts author, not
        // the implementation under test's incremental hashing helper.
        let mut hash = Sha256::new();
        let append = |hash: &mut Sha256, bytes: &[u8]| {
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        };
        append(&mut hash, b"novarocks-paimon-input-version-v1");
        append(&mut hash, view.table_location().as_bytes());
        match view.snapshot_id() {
            Some(id) => {
                hash.update([1]);
                hash.update(id.to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash.update(view.schema_id().to_be_bytes());
        append(&mut hash, view.schema_fingerprint());
        let version: [u8; 32] = hash.finalize().into();
        ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new(version.as_slice()).unwrap(),
            *view.read_recipe_digest(),
            ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, Vec::new())
                .unwrap(),
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            Vec::new(),
        )
        .unwrap()
    }
    fn compile(
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadProgramRecipe, ConnectorReadProgramCompileError<ConnectorCodecError>>
    {
        ConnectorReadProgramRecipe::try_compile_with_provider(
            frozen,
            &PaimonReadRecipeCompiler,
            control,
        )
    }
    fn rejected(fixture: &Fixture) {
        assert!(matches!(
            compile(&fixture.frozen(), &Control::default()),
            Err(ConnectorReadProgramCompileError::Provider(_))
        ));
    }

    fn pure_catalog() -> PureProviderProgramCatalog<ConnectorCodecError> {
        // This is a read-only Paimon fixture manifest, not the Server manifest.
        let provider = ConnectorProviderId::parse(PROVIDER_ID).unwrap();
        PureProviderProgramCatalog::try_new(
            &[PureProviderManifestEntry::new(
                provider.clone(),
                true,
                false,
            )],
            vec![PureProviderProgramDefinition::new(
                provider,
                Some(Arc::new(PaimonReadRecipeCompiler)),
                None,
            )],
            &Control::default(),
        )
        .unwrap()
    }

    #[test]
    fn installed_pure_catalog_dispatches_full_paimon_facts_and_keeps_provider_rejection() {
        let catalog = pure_catalog();
        let mut fixture = Fixture::projected();
        fixture.residual = TupleDomain::none();
        fixture.remaining = Some(ConnectorExpression::Constant {
            value: Some(ConnectorValue::Boolean(false)),
            value_type: ConnectorValueType::Boolean,
        });
        fixture
            .filters
            .push(StaticScanDynamicFilter::new(37, Arc::from("alias_0")));
        let frozen = fixture.frozen();
        let recipe = catalog.compile_read(&frozen, &Control::default()).unwrap();
        assert_eq!(recipe.frozen(), &frozen);
        assert_eq!(catalog.provider_count(), 1);

        fixture.fields[0] = fixture.fields[0].clone().with_name("alias_0");
        let error = catalog
            .compile_read(&fixture.frozen(), &Control::default())
            .unwrap_err();
        assert!(matches!(error, PureProviderProgramError::Provider(_)));
    }

    #[test]
    fn installed_pure_catalog_uses_provider_identity_before_private_decode() {
        let catalog = pure_catalog();
        let mut fixture = Fixture::projected();
        let unknown = ConnectorProviderId::parse("unknown-provider").unwrap();
        fixture.binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: unknown.clone(),
                instance_id: fixture.binding.descriptor().instance_id.clone(),
            },
            fixture.binding.catalog_handle().clone(),
        );
        assert!(matches!(
            catalog.compile_read(&fixture.frozen(), &Control::default()),
            Err(PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(id)))
                if id == unknown
        ));
    }

    #[test]
    fn installed_pure_catalog_preserves_each_original_control_cause() {
        let catalog = pure_catalog();
        let frozen = Fixture::projected().frozen();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                calls: Mutex::default(),
                fail: Some((cause, Stop::Entry)),
            };
            assert!(matches!(catalog.compile_read(&frozen, &control),
                Err(PureProviderProgramError::Control(actual)) if actual == cause));
            assert_eq!(control.calls.lock().unwrap().as_slice(), &[0]);
        }
    }

    #[test]
    fn full_program_reordered_duplicate_sources_preserve_aliases_residuals_and_rf() {
        let mut fixture = Fixture::projected();
        fixture.residual = TupleDomain::with_column_domains(BTreeMap::from([(
            ScanColumnId::new(0),
            Domain::single_value(ConnectorValue::Varchar(Arc::from("keep"))).unwrap(),
        )]))
        .unwrap();
        fixture.remaining = Some(ConnectorExpression::Constant {
            value: Some(ConnectorValue::Boolean(false)),
            value_type: ConnectorValueType::Boolean,
        });
        fixture
            .filters
            .push(StaticScanDynamicFilter::new(37, Arc::from("alias_0")));
        let original = fixture.frozen();
        let result = compile(&original, &Control::default()).unwrap();
        assert_eq!(result.frozen(), &original);
        assert_eq!(
            result.frozen().scan().assignments()[0].variable(),
            "alias_0"
        );
        assert_eq!(
            result.frozen().public_facts().schema().field(0).name(),
            "value"
        );
        assert_eq!(
            fixture
                .columns
                .iter()
                .map(PaimonColumn::output_ordinal)
                .collect::<Vec<_>>(),
            vec![1, 0, 1]
        );
        assert_eq!(result.frozen().scan().dynamic_filters()[0].filter_id(), 37);
        assert!(!result.frozen().scan().unenforced_predicate().is_all());
    }

    #[test]
    fn full_program_rejects_exact_field_name_carrier_nullability_id_and_nominal_mutations() {
        let mutations: [fn(&mut Fixture); 7] = [
            |f| f.fields[0] = f.fields[0].clone().with_name("alias_0"),
            |f| f.fields[0] = f.fields[0].clone().with_data_type(DataType::LargeUtf8),
            |f| f.fields[0] = f.fields[0].clone().with_nullable(false),
            |f| {
                f.fields[0] = f.fields[0]
                    .clone()
                    .with_metadata(HashMap::from([(FIELD_ID_KEY.into(), "99".into())]))
            },
            |f| f.fields[0] = f.fields[0].clone().with_metadata(HashMap::new()),
            |f| {
                let mut meta = f.fields[0].metadata().clone();
                meta.insert("foreign".into(), "extra".into());
                f.fields[0] = f.fields[0].clone().with_metadata(meta);
            },
            |f| f.logical[0] = ValueLogicalType::Json,
        ];
        for mutation in mutations {
            let mut fixture = Fixture::projected();
            mutation(&mut fixture);
            rejected(&fixture);
        }
    }

    #[test]
    fn full_program_rejects_provider_guarantees_and_wrong_source_version_or_selection() {
        for mutation in 0..6 {
            let mut fixture = Fixture::projected();
            let old = &fixture.source;
            let version = if mutation == 0 {
                ConnectorReadInputVersion::try_new([9_u8; 32].as_slice()).unwrap()
            } else {
                old.input_version().clone()
            };
            let selection = if mutation == 1 {
                [9; 32]
            } else {
                old.selection_digest()
            };
            let distribution = if mutation == 2 {
                ConnectorReadDistribution::Singleton
            } else {
                ConnectorReadDistribution::Unconstrained
            };
            let artifact = if mutation == 3 {
                ConnectorReadArtifactCoverage::exact([8; 32], [9; 32], vec![1]).unwrap()
            } else {
                ConnectorReadArtifactCoverage::NoArtifactInputs
            };
            fixture.source = ConnectorReadStaticFacts::try_new(
                version,
                selection,
                ConnectorReadProperties::try_new(distribution, Vec::new()).unwrap(),
                artifact,
                if mutation == 4 { vec![1] } else { vec![] },
            )
            .unwrap();
            if mutation == 5 {
                fixture.enforced = TupleDomain::none();
            }
            rejected(&fixture);
        }
    }

    #[test]
    fn full_program_rejects_conflicting_repeated_source_descriptors() {
        let mut fixture = Fixture::projected();
        let original = &fixture.columns[2];
        fixture.columns[2] = PaimonColumn::try_new(
            original.field_id(),
            "renamed",
            original.data_type(),
            original.nullable(),
            original.output_ordinal(),
        )
        .unwrap();
        fixture.fields[2] = fixture.fields[2].clone().with_name("renamed");
        rejected(&fixture);
        let mut fixture = Fixture::projected();
        let original = &fixture.columns[2];
        fixture.columns[2] = PaimonColumn::try_new(
            original.field_id(),
            original.name(),
            original.data_type(),
            original.nullable(),
            77,
        )
        .unwrap();
        rejected(&fixture);
    }

    #[test]
    fn full_program_rejects_malformed_private_bytes_header_and_relation_location() {
        let fixture = Fixture::projected();
        let draft = fixture.draft();
        let payload = fixture.payload(ConnectorCodecCategory::ReadTable, Bytes::new());
        {
            let changed = ConnectorReadRelationRecipeDraft::try_new(
                fixture.binding.clone(),
                ConnectorReadRelationPayload::new(
                    ConnectorReadRelationKind::Table,
                    payload,
                    draft.relation().view().clone(),
                ),
                draft.columns().to_vec(),
            )
            .unwrap();
            assert!(matches!(
                compile(&fixture.frozen_draft(changed), &Control::default()),
                Err(ConnectorReadProgramCompileError::Provider(_))
            ));
        }
        // All payload headers share a structurally valid but unsupported
        // revision, so the complete provider hook performs this rejection.
        let revision = |payload: &ConnectorEncodedPayload| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    payload.header().provider_id().clone(),
                    payload.header().catalog().clone(),
                    payload.header().category(),
                    ConnectorCodecRevision::try_new(PAIMON_READ_CODEC_REVISION + 1).unwrap(),
                ),
                payload.payload().clone(),
            )
        };
        let changed = ConnectorReadRelationRecipeDraft::try_new(
            fixture.binding.clone(),
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                revision(draft.relation().table()),
                revision(draft.relation().view()),
            ),
            draft.columns().iter().map(revision).collect(),
        )
        .unwrap();
        assert!(matches!(
            compile(&fixture.frozen_draft(changed), &Control::default()),
            Err(ConnectorReadProgramCompileError::Provider(_))
        ));
        let mut changed = Fixture::projected();
        changed.view = PaimonReadView::try_new(
            "s3://bucket/foreign",
            Some(41),
            7,
            *changed.view.schema_fingerprint(),
            *changed.view.read_recipe_digest(),
            None,
        )
        .unwrap();
        changed.source = source(&changed.view);
        rejected(&changed);
    }

    #[test]
    fn full_program_optional_empty_snapshot_has_the_exact_source_version() {
        let fixture = Fixture::projected_snapshot(None);
        assert_eq!(
            compile(&fixture.frozen(), &Control::default())
                .unwrap()
                .frozen(),
            &fixture.frozen()
        );
    }

    #[test]
    fn full_program_preserves_all_three_controls_at_entry_wire_quantum_and_publication() {
        let mut builder = spec::Schema::builder();
        let mut names = Vec::new();
        for i in 0..320 {
            let name = format!("key_{i}");
            builder = builder.column(
                &name,
                spec::DataType::BigInt(spec::BigIntType::with_nullable(false)),
            );
            names.push(name);
        }
        let schema = TableSchema::new(9, &builder.primary_key(names).build().unwrap());
        let fixture = Fixture::from_schema(&schema, &[319, 0, 319]);
        // Prost's canonical writer packs integer vectors into one field. An
        // unpacked repeated representation is also valid and lets the actual
        // private scanner observe each key occurrence before opaque decode.
        use prost::Message;
        let original = fixture.draft();
        let mut raw = crate::wire::dto::PaimonTablePayload::decode(
            original.relation().table().payload().as_ref(),
        )
        .unwrap();
        let ids = std::mem::take(&mut raw.primary_key_field_ids);
        let mut bytes = raw.encode_to_vec();
        for id in ids {
            prost::encoding::encode_key(6, prost::encoding::WireType::Varint, &mut bytes);
            prost::encoding::encode_varint(id as u64, &mut bytes);
        }
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            original.binding().clone(),
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                fixture.payload(ConnectorCodecCategory::ReadTable, Bytes::from(bytes)),
                original.relation().view().clone(),
            ),
            original.columns().to_vec(),
        )
        .unwrap();
        let frozen = fixture.frozen_draft(draft);
        let baseline = Control::default();
        compile(&frozen, &baseline).unwrap();
        let calls = baseline.calls.lock().unwrap().clone();
        assert!(
            calls.contains(&256),
            "the actual scanner must observe the unpacked repeated key occurrences"
        );
        assert!(calls.iter().all(|&n| n <= 256));
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in [Stop::Entry, Stop::Quantum, Stop::Call(calls.len())] {
                let owner = Control {
                    calls: Mutex::default(),
                    fail: Some((cause, stop)),
                };
                assert!(
                    matches!(compile(&frozen, &owner), Err(ConnectorReadProgramCompileError::Control(error)) if error == cause)
                );
                if matches!(stop, Stop::Quantum) {
                    assert_eq!(owner.calls.lock().unwrap().last(), Some(&256));
                }
            }
        }
    }

    #[test]
    fn full_program_field_author_matches_the_real_sdk_for_every_pai1_carrier() {
        let types = [
            spec::DataType::Boolean(spec::BooleanType::new()),
            spec::DataType::TinyInt(spec::TinyIntType::new()),
            spec::DataType::SmallInt(spec::SmallIntType::new()),
            spec::DataType::Int(spec::IntType::new()),
            spec::DataType::BigInt(spec::BigIntType::new()),
            spec::DataType::Float(spec::FloatType::new()),
            spec::DataType::Double(spec::DoubleType::new()),
            spec::DataType::Decimal(spec::DecimalType::with_nullable(true, 38, 38).unwrap()),
            spec::DataType::VarChar(spec::VarCharType::with_nullable(true, 100).unwrap()),
            spec::DataType::VarBinary(spec::VarBinaryType::try_new(true, 100).unwrap()),
            spec::DataType::Date(spec::DateType::new()),
            spec::DataType::Timestamp(spec::TimestampType::with_nullable(true, 0).unwrap()),
            spec::DataType::Timestamp(spec::TimestampType::with_nullable(true, 3).unwrap()),
            spec::DataType::Timestamp(spec::TimestampType::with_nullable(true, 4).unwrap()),
            spec::DataType::Timestamp(spec::TimestampType::with_nullable(true, 6).unwrap()),
        ];
        for (i, data_type) in types.into_iter().enumerate() {
            let name = format!("column_{i}");
            let field = DataField::new(i as i32, name.clone(), data_type.clone());
            let actual = paimon::arrow::build_target_arrow_schema(&[field]).unwrap();
            let schema = TableSchema::new(
                1,
                &spec::Schema::builder()
                    .column(&name, data_type)
                    .build()
                    .unwrap(),
            );
            let mut column = crate::metadata::columns_from_schema(&schema)
                .unwrap()
                .remove(0);
            column = PaimonColumn::try_new(
                i as i32,
                column.name(),
                column.data_type(),
                column.nullable(),
                0,
            )
            .unwrap();
            assert!(arrow_fields_exact(&column_field(&column), actual.field(0)));
        }
    }
}
