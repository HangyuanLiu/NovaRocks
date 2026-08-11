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

//! Provider-owned row-mutation admission and activation contract.
//! Design: ADR-0049 (docs/adr/ADR-0049-provider-row-mutation-strategy-identity-routes-and-cohorts.md)

use std::collections::HashSet;

use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorRequestContext,
    ConnectorSealedWriteCohortSet, ConnectorTableHandle, ConnectorWriteBaseVersion,
    ConnectorWriteCohortId, ConnectorWriteFieldToken, ConnectorWriteInputShape,
    ConnectorWriteOperationId, ConnectorWritePreparation, ConnectorWriteTargetRef,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
};

pub const CONNECTOR_ROW_MUTATION_CONTRACT_VERSION: u32 = 1;
pub const MAX_CONNECTOR_ROW_MUTATION_ROUTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorRowMutationIntent {
    Delete,
    Update,
    Merge {
        effects: Vec<ConnectorRowMutationEffect>,
    },
}

impl ConnectorRowMutationIntent {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Self::Merge { effects } = self {
            validate_effects(effects, "connector merge intent")?;
        }
        Ok(())
    }

    pub fn accepts(&self, effect: ConnectorRowMutationEffect) -> bool {
        match self {
            Self::Delete => effect == ConnectorRowMutationEffect::Delete,
            Self::Update => effect == ConnectorRowMutationEffect::Replace,
            Self::Merge { effects } => effects.contains(&effect),
        }
    }
}

/// SQL-visible semantics only. A value is never a deletion-vector, rewrite,
/// or table-format route discriminator.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorRowMutationEffect {
    Delete = 1,
    Replace = 2,
    Insert = 3,
}

/// Fixed-width opaque provider route key. Native plans reject every other
/// representation before a provider is reached.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteRouteId([u8; 32]);

impl ConnectorWriteRouteId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorMutationSourceField {
    token: ConnectorWriteFieldToken,
    field: Field,
    source_ordinal: u32,
}

impl ConnectorMutationSourceField {
    pub fn new(token: ConnectorWriteFieldToken, field: Field, source_ordinal: u32) -> Self {
        Self {
            token,
            field,
            source_ordinal,
        }
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub const fn source_ordinal(&self) -> u32 {
        self.source_ordinal
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorMutationTargetField {
    token: ConnectorWriteFieldToken,
    field: Field,
    target_ordinal: u32,
}

impl ConnectorMutationTargetField {
    pub fn new(token: ConnectorWriteFieldToken, field: Field, target_ordinal: u32) -> Self {
        Self {
            token,
            field,
            target_ordinal,
        }
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub const fn target_ordinal(&self) -> u32 {
        self.target_ordinal
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorMutationEffectField {
    token: ConnectorWriteFieldToken,
    field: Field,
    target_ordinal: u32,
}

impl ConnectorMutationEffectField {
    pub fn try_new(
        token: ConnectorWriteFieldToken,
        field: Field,
        target_ordinal: u32,
    ) -> Result<Self, ConnectorError> {
        if field.data_type() != &DataType::Int8 || field.is_nullable() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation effect field must be non-null Int8",
            ));
        }
        Ok(Self {
            token,
            field,
            target_ordinal,
        })
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub const fn target_ordinal(&self) -> u32 {
        self.target_ordinal
    }
}

/// A provider-signed match layout. Identity, before/after values and the
/// duplicate-detection tuple are token-bound, not inferred from column names.
#[derive(Clone)]
pub struct ConnectorMutationMatchContract {
    owner: ConnectorExecutionBindingKey,
    table: ConnectorTableHandle,
    base_version: ConnectorWriteBaseVersion,
    identity_fields: Vec<ConnectorMutationSourceField>,
    before_fields: Vec<ConnectorMutationTargetField>,
    after_fields: Vec<ConnectorMutationTargetField>,
    uniqueness_tokens: Vec<ConnectorWriteFieldToken>,
    effect_field: ConnectorMutationEffectField,
    digest: [u8; 32],
}

impl ConnectorMutationMatchContract {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        table: ConnectorTableHandle,
        base_version: ConnectorWriteBaseVersion,
        identity_fields: Vec<ConnectorMutationSourceField>,
        before_fields: Vec<ConnectorMutationTargetField>,
        after_fields: Vec<ConnectorMutationTargetField>,
        uniqueness_tokens: Vec<ConnectorWriteFieldToken>,
        effect_field: ConnectorMutationEffectField,
    ) -> Result<Self, ConnectorError> {
        if table.owner() != &owner.instance_id
            || identity_fields.is_empty()
            || uniqueness_tokens.is_empty()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation match contract needs an exact owner, identity, and uniqueness tuple",
            ));
        }
        base_version.validate()?;
        let mut tokens = HashSet::new();
        let mut source_ordinals = HashSet::new();
        for value in &identity_fields {
            if !tokens.insert(value.token) || !source_ordinals.insert(value.source_ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation identity has duplicate token or source ordinal",
                ));
            }
        }
        let mut target_ordinals = HashSet::new();
        for value in before_fields.iter().chain(&after_fields) {
            if !tokens.insert(value.token) || !target_ordinals.insert(value.target_ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation target field has duplicate token or ordinal",
                ));
            }
        }
        if !tokens.insert(effect_field.token)
            || !target_ordinals.insert(effect_field.target_ordinal)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation effect field conflicts with the match layout",
            ));
        }
        let mut unique = HashSet::new();
        if uniqueness_tokens
            .iter()
            .any(|token| !tokens.contains(token) || !unique.insert(*token))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation uniqueness tuple contains a foreign or duplicate token",
            ));
        }
        let digest = match_digest(
            &owner,
            &table,
            &base_version,
            &identity_fields,
            &before_fields,
            &after_fields,
            &uniqueness_tokens,
            &effect_field,
        );
        Ok(Self {
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            uniqueness_tokens,
            effect_field,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.owner.clone(),
            self.table.clone(),
            self.base_version.clone(),
            self.identity_fields.clone(),
            self.before_fields.clone(),
            self.after_fields.clone(),
            self.uniqueness_tokens.clone(),
            self.effect_field.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation match contract digest does not match contents",
            ));
        }
        Ok(())
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }
    pub fn base_version(&self) -> &ConnectorWriteBaseVersion {
        &self.base_version
    }
    pub fn identity_fields(&self) -> &[ConnectorMutationSourceField] {
        &self.identity_fields
    }
    pub fn before_fields(&self) -> &[ConnectorMutationTargetField] {
        &self.before_fields
    }
    pub fn after_fields(&self) -> &[ConnectorMutationTargetField] {
        &self.after_fields
    }
    pub fn uniqueness_tokens(&self) -> &[ConnectorWriteFieldToken] {
        &self.uniqueness_tokens
    }
    pub fn effect_field(&self) -> &ConnectorMutationEffectField {
        &self.effect_field
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorRowMutationStrategy {
    PositionDelete,
    DeletionVector,
    MergeOnRead,
    CopyOnWrite,
    EqualityDelete,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorMutationRouteInput {
    token: ConnectorWriteFieldToken,
    input_ordinal: u32,
}

impl ConnectorMutationRouteInput {
    pub const fn new(token: ConnectorWriteFieldToken, input_ordinal: u32) -> Self {
        Self {
            token,
            input_ordinal,
        }
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub const fn input_ordinal(&self) -> u32 {
        self.input_ordinal
    }
}

/// One opaque sink route. A route can accept more than one logical effect;
/// generic split sinks independently fan out one Replace to all such routes.
#[derive(Clone)]
pub struct ConnectorRowMutationRoute {
    route_id: ConnectorWriteRouteId,
    cohort_id: ConnectorWriteCohortId,
    accepted_effects: Vec<ConnectorRowMutationEffect>,
    input: ConnectorWriteInputShape,
    input_ordinals: Vec<ConnectorMutationRouteInput>,
    partition_fields: Vec<ConnectorWriteFieldToken>,
    preparation: ConnectorWritePreparation,
    digest: [u8; 32],
}

impl ConnectorRowMutationRoute {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        cohort_id: ConnectorWriteCohortId,
        accepted_effects: Vec<ConnectorRowMutationEffect>,
        input: ConnectorWriteInputShape,
        input_ordinals: Vec<ConnectorMutationRouteInput>,
        partition_fields: Vec<ConnectorWriteFieldToken>,
        preparation: ConnectorWritePreparation,
    ) -> Result<Self, ConnectorError> {
        validate_effects(&accepted_effects, "row-mutation route")?;
        input.validate()?;
        preparation.validate()?;
        let known: HashSet<_> = input
            .fields()
            .into_iter()
            .map(|field| field.token())
            .collect();
        let mut tokens = HashSet::new();
        let mut ordinals = HashSet::new();
        if input_ordinals.len() != known.len()
            || input_ordinals.iter().any(|input| {
                !known.contains(&input.token)
                    || !tokens.insert(input.token)
                    || !ordinals.insert(input.input_ordinal)
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation route input bindings are incomplete, foreign, or duplicate",
            ));
        }
        let mut partitions = HashSet::new();
        if partition_fields
            .iter()
            .any(|token| !known.contains(token) || !partitions.insert(*token))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation route partition token is foreign or duplicate",
            ));
        }
        let digest = route_digest(
            route_id,
            cohort_id,
            &accepted_effects,
            &input_ordinals,
            &partition_fields,
            &preparation,
        );
        Ok(Self {
            route_id,
            cohort_id,
            accepted_effects,
            input,
            input_ordinals,
            partition_fields,
            preparation,
            digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.route_id,
            self.cohort_id,
            self.accepted_effects.clone(),
            self.input.clone(),
            self.input_ordinals.clone(),
            self.partition_fields.clone(),
            self.preparation.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation route digest does not match contents",
            ));
        }
        Ok(())
    }
    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    pub fn accepted_effects(&self) -> &[ConnectorRowMutationEffect] {
        &self.accepted_effects
    }
    pub fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    pub fn input_ordinals(&self) -> &[ConnectorMutationRouteInput] {
        &self.input_ordinals
    }
    pub fn partition_fields(&self) -> &[ConnectorWriteFieldToken] {
        &self.partition_fields
    }
    pub fn preparation(&self) -> &ConnectorWritePreparation {
        &self.preparation
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Provider-signed, pure planning result. The payload is opaque to Core and
/// must be activated with this exact operation and retained write lease.
#[derive(Clone)]
pub struct ConnectorRowMutationPreparation {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    table: ConnectorTableHandle,
    target_ref: ConnectorWriteTargetRef,
    intent: ConnectorRowMutationIntent,
    base_version: ConnectorWriteBaseVersion,
    match_contract: ConnectorMutationMatchContract,
    strategy: ConnectorRowMutationStrategy,
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorRowMutationPreparation {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableHandle,
        target_ref: ConnectorWriteTargetRef,
        intent: ConnectorRowMutationIntent,
        base_version: ConnectorWriteBaseVersion,
        match_contract: ConnectorMutationMatchContract,
        strategy: ConnectorRowMutationStrategy,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if table.owner() != &owner.instance_id
            || match_contract.owner() != &owner
            || match_contract.table() != &table
            || match_contract.base_version() != &base_version
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation preparation does not match owner and table",
            ));
        }
        intent.validate()?;
        base_version.validate()?;
        match_contract.validate()?;
        validate_payload(&payload)?;
        let digest = preparation_digest(
            &owner,
            operation_id,
            &table,
            &target_ref,
            &intent,
            &base_version,
            &match_contract,
            strategy,
            &payload,
        );
        Ok(Self {
            owner,
            operation_id,
            table,
            target_ref,
            intent,
            base_version,
            match_contract,
            strategy,
            payload,
            digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.owner.clone(),
            self.operation_id,
            self.table.clone(),
            self.target_ref.clone(),
            self.intent.clone(),
            self.base_version.clone(),
            self.match_contract.clone(),
            self.strategy,
            self.payload.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation preparation digest does not match contents",
            ));
        }
        Ok(())
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }
    pub fn target_ref(&self) -> &ConnectorWriteTargetRef {
        &self.target_ref
    }
    pub fn intent(&self) -> &ConnectorRowMutationIntent {
        &self.intent
    }
    pub fn match_contract(&self) -> &ConnectorMutationMatchContract {
        &self.match_contract
    }
    pub fn base_version(&self) -> &ConnectorWriteBaseVersion {
        &self.base_version
    }
    pub const fn strategy(&self) -> ConnectorRowMutationStrategy {
        self.strategy
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorRowMutationPreparationRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub table: ConnectorTableHandle,
    pub target_ref: ConnectorWriteTargetRef,
    pub intent: ConnectorRowMutationIntent,
    pub context: ConnectorRequestContext,
}

impl ConnectorRowMutationPreparationRequest {
    pub fn validate(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if self.table.owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation request table is foreign to the exact lease",
            ));
        }
        self.intent.validate()
    }
}

#[derive(Clone)]
pub enum ConnectorRowMutationPreparationOutcome {
    Prepared(ConnectorRowMutationPreparation),
    Denied(ConnectorError),
}

/// A bounded, non-concatenated COW match result. The caller enforces the
/// minimum of request payload and execution-memory budgets before activation.
#[derive(Clone, Debug)]
pub struct ConnectorRowMutationSelection {
    batches: Vec<RecordBatch>,
    row_count: u64,
    byte_count: u64,
    digest: [u8; 32],
}

impl ConnectorRowMutationSelection {
    pub fn try_new(
        batches: Vec<RecordBatch>,
        max_rows: u64,
        max_bytes: u64,
    ) -> Result<Self, ConnectorError> {
        if max_rows == 0 || max_bytes == 0 || max_rows > max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation selection has invalid row or byte budgets",
            ));
        }
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for batch in &batches {
            rows = rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation selection row accounting overflowed",
                )
            })?;
            bytes = bytes
                .checked_add(batch.get_array_memory_size() as u64)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "row-mutation selection byte accounting overflowed",
                    )
                })?;
            if rows > max_rows || bytes > max_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation selection exceeds its row or byte budget",
                ));
            }
        }
        let digest = selection_digest(&batches, rows, bytes);
        Ok(Self {
            batches,
            row_count: rows,
            byte_count: bytes,
            digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if selection_digest(&self.batches, self.row_count, self.byte_count) != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation selection digest does not match batches",
            ));
        }
        Ok(())
    }
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorRowMutationCohortRecipe {
    cohort_id: ConnectorWriteCohortId,
    route_id: ConnectorWriteRouteId,
    payload: Bytes,
}

impl ConnectorRowMutationCohortRecipe {
    pub fn try_new(
        cohort_id: ConnectorWriteCohortId,
        route_id: ConnectorWriteRouteId,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_payload(&payload)?;
        Ok(Self {
            cohort_id,
            route_id,
            payload,
        })
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Clone)]
pub enum ConnectorRowMutationActivationRequest {
    Direct {
        preparation: ConnectorRowMutationPreparation,
        context: ConnectorRequestContext,
    },
    CopyOnWrite {
        preparation: ConnectorRowMutationPreparation,
        selection: ConnectorRowMutationSelection,
        context: ConnectorRequestContext,
    },
}

impl ConnectorRowMutationActivationRequest {
    pub fn preparation(&self) -> &ConnectorRowMutationPreparation {
        match self {
            Self::Direct { preparation, .. } | Self::CopyOnWrite { preparation, .. } => preparation,
        }
    }
    pub fn context(&self) -> &ConnectorRequestContext {
        match self {
            Self::Direct { context, .. } | Self::CopyOnWrite { context, .. } => context,
        }
    }
    pub fn validate(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        let preparation = self.preparation();
        preparation.validate()?;
        if preparation.owner() != owner {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation activation has a foreign owner",
            ));
        }
        match self {
            Self::Direct { .. }
                if preparation.strategy() == ConnectorRowMutationStrategy::CopyOnWrite =>
            {
                Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write requires a bounded selection",
                ))
            }
            Self::CopyOnWrite { .. }
                if preparation.strategy() != ConnectorRowMutationStrategy::CopyOnWrite =>
            {
                Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "only copy-on-write accepts a selection",
                ))
            }
            Self::CopyOnWrite { selection, .. } => selection.validate(),
            Self::Direct { .. } => Ok(()),
        }
    }
}

/// Provider-sealed physical execution result for one row-mutation operation.
///
/// The application can route its opaque cohorts but cannot assemble an
/// unbound set of routes.  In particular, the plan always retains the exact
/// row-mutation preparation that authenticated its operation and generation.
#[derive(Clone)]
pub struct ConnectorRowMutationExecutionPlan {
    preparation: ConnectorRowMutationPreparation,
    body: ConnectorRowMutationExecutionPlanBody,
    digest: [u8; 32],
}

#[derive(Clone)]
enum ConnectorRowMutationExecutionPlanBody {
    Direct {
        routes: Vec<ConnectorRowMutationRoute>,
    },
    CopyOnWrite {
        routes: Vec<ConnectorRowMutationRoute>,
        sealed_cohorts: ConnectorSealedWriteCohortSet,
        cohort_recipes: Vec<ConnectorRowMutationCohortRecipe>,
    },
}

impl ConnectorRowMutationExecutionPlan {
    pub fn try_direct(
        preparation: ConnectorRowMutationPreparation,
        routes: Vec<ConnectorRowMutationRoute>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            preparation,
            ConnectorRowMutationExecutionPlanBody::Direct { routes },
        )
    }
    pub fn try_copy_on_write(
        preparation: ConnectorRowMutationPreparation,
        routes: Vec<ConnectorRowMutationRoute>,
        sealed_cohorts: ConnectorSealedWriteCohortSet,
        cohort_recipes: Vec<ConnectorRowMutationCohortRecipe>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            preparation,
            ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
                routes,
                sealed_cohorts,
                cohort_recipes,
            },
        )
    }

    fn try_new(
        preparation: ConnectorRowMutationPreparation,
        body: ConnectorRowMutationExecutionPlanBody,
    ) -> Result<Self, ConnectorError> {
        preparation.validate()?;
        let routes = match &body {
            ConnectorRowMutationExecutionPlanBody::Direct { routes }
            | ConnectorRowMutationExecutionPlanBody::CopyOnWrite { routes, .. } => routes,
        };
        validate_routes(&routes)?;
        if routes.iter().any(|route| {
            route.preparation().owner() != preparation.owner()
                || route.preparation().table() != preparation.table()
                || route.preparation().base_version() != preparation.base_version()
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation execution route is foreign to its preparation",
            ));
        }
        if let ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
            sealed_cohorts,
            cohort_recipes,
            ..
        } = &body
        {
            if preparation.strategy() != ConnectorRowMutationStrategy::CopyOnWrite
                || cohort_recipes.is_empty()
                || sealed_cohorts.operation_id() != preparation.operation_id()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write execution plan does not match its preparation",
                ));
            }
            let ids: HashSet<_> = routes.iter().map(|route| route.route_id()).collect();
            let cohorts: HashSet<_> = sealed_cohorts
                .cohorts()
                .iter()
                .map(|cohort| cohort.cohort_id())
                .collect();
            let mut seen = HashSet::new();
            if cohort_recipes.iter().any(|recipe| {
                !ids.contains(&recipe.route_id)
                    || !cohorts.contains(&recipe.cohort_id)
                    || !seen.insert(recipe.cohort_id)
            }) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write recipe is foreign or duplicate",
                ));
            }
        } else if preparation.strategy() == ConnectorRowMutationStrategy::CopyOnWrite {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "copy-on-write preparation requires a copy-on-write execution plan",
            ));
        }
        let digest = execution_plan_digest(&preparation, &body);
        Ok(Self {
            preparation,
            body,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(self.preparation.clone(), self.body.clone())?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation execution plan digest does not match contents",
            ));
        }
        Ok(())
    }

    pub fn preparation(&self) -> &ConnectorRowMutationPreparation {
        &self.preparation
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        self.preparation.owner()
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.preparation.operation_id()
    }

    pub const fn source_digest(&self) -> [u8; 32] {
        self.preparation.digest()
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn routes(&self) -> &[ConnectorRowMutationRoute] {
        match &self.body {
            ConnectorRowMutationExecutionPlanBody::Direct { routes }
            | ConnectorRowMutationExecutionPlanBody::CopyOnWrite { routes, .. } => routes,
        }
    }

    /// Returns the immutable cohort set and Provider-private rewrite recipes
    /// only for a Copy-on-Write activation.  Callers may transport and seal
    /// these values, but must not decode recipe payloads.
    pub fn copy_on_write(
        &self,
    ) -> Option<(
        &ConnectorSealedWriteCohortSet,
        &[ConnectorRowMutationCohortRecipe],
    )> {
        match &self.body {
            ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
                sealed_cohorts,
                cohort_recipes,
                ..
            } => Some((sealed_cohorts, cohort_recipes)),
            ConnectorRowMutationExecutionPlanBody::Direct { .. } => None,
        }
    }
}

fn validate_effects(
    effects: &[ConnectorRowMutationEffect],
    subject: &str,
) -> Result<(), ConnectorError> {
    if effects.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("{subject} requires at least one effect"),
        ));
    }
    let mut seen = HashSet::new();
    if effects.iter().any(|effect| !seen.insert(*effect)) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("{subject} contains duplicate effects"),
        ));
    }
    Ok(())
}

fn validate_routes(routes: &[ConnectorRowMutationRoute]) -> Result<(), ConnectorError> {
    if routes.is_empty() || routes.len() > MAX_CONNECTOR_ROW_MUTATION_ROUTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation routes must be non-empty and bounded",
        ));
    }
    let mut seen = HashSet::new();
    for route in routes {
        route.validate()?;
        if !seen.insert(route.route_id()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation execution plan has duplicate route IDs",
            ));
        }
    }
    Ok(())
}

fn validate_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation provider payload exceeds hard limit",
        ));
    }
    Ok(())
}

fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
fn digest_owner(hasher: &mut Sha256, owner: &ConnectorExecutionBindingKey) {
    digest_bytes(hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
}
fn match_digest(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    base: &ConnectorWriteBaseVersion,
    identity: &[ConnectorMutationSourceField],
    before: &[ConnectorMutationTargetField],
    after: &[ConnectorMutationTargetField],
    unique: &[ConnectorWriteFieldToken],
    effect: &ConnectorMutationEffectField,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-match.v1\0");
    digest_owner(&mut hasher, owner);
    digest_bytes(&mut hasher, table.payload());
    hasher.update(base.digest());
    for field in identity {
        hasher.update(field.token.to_bytes());
        hasher.update(field.source_ordinal.to_be_bytes());
        digest_bytes(&mut hasher, format!("{:?}", field.field).as_bytes());
    }
    for field in before.iter().chain(after) {
        hasher.update(field.token.to_bytes());
        hasher.update(field.target_ordinal.to_be_bytes());
        digest_bytes(&mut hasher, format!("{:?}", field.field).as_bytes());
    }
    for token in unique {
        hasher.update(token.to_bytes());
    }
    hasher.update(effect.token.to_bytes());
    hasher.update(effect.target_ordinal.to_be_bytes());
    digest_bytes(&mut hasher, format!("{:?}", effect.field).as_bytes());
    hasher.finalize().into()
}
fn route_digest(
    route: ConnectorWriteRouteId,
    cohort: ConnectorWriteCohortId,
    effects: &[ConnectorRowMutationEffect],
    inputs: &[ConnectorMutationRouteInput],
    partitions: &[ConnectorWriteFieldToken],
    preparation: &ConnectorWritePreparation,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-route.v1\0");
    hasher.update(route.to_bytes());
    hasher.update(cohort.to_bytes());
    for effect in effects {
        hasher.update([effect_tag(*effect)]);
    }
    for input in inputs {
        hasher.update(input.token.to_bytes());
        hasher.update(input.input_ordinal.to_be_bytes());
    }
    for token in partitions {
        hasher.update(token.to_bytes());
    }
    hasher.update(preparation.digest());
    hasher.finalize().into()
}
fn execution_plan_digest(
    preparation: &ConnectorRowMutationPreparation,
    body: &ConnectorRowMutationExecutionPlanBody,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-execution-plan.v1\0");
    hasher.update(preparation.digest());
    match body {
        ConnectorRowMutationExecutionPlanBody::Direct { routes } => {
            hasher.update([1]);
            for route in routes {
                hasher.update(route.digest());
            }
        }
        ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
            routes,
            sealed_cohorts,
            cohort_recipes,
        } => {
            hasher.update([2]);
            for route in routes {
                hasher.update(route.digest());
            }
            hasher.update(sealed_cohorts.digest());
            for recipe in cohort_recipes {
                hasher.update(recipe.cohort_id().to_bytes());
                hasher.update(recipe.route_id().to_bytes());
                digest_bytes(&mut hasher, recipe.payload());
            }
        }
    }
    hasher.finalize().into()
}
fn preparation_digest(
    owner: &ConnectorExecutionBindingKey,
    operation: ConnectorWriteOperationId,
    table: &ConnectorTableHandle,
    target_ref: &ConnectorWriteTargetRef,
    intent: &ConnectorRowMutationIntent,
    base: &ConnectorWriteBaseVersion,
    contract: &ConnectorMutationMatchContract,
    strategy: ConnectorRowMutationStrategy,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-preparation.v1\0");
    digest_owner(&mut hasher, owner);
    hasher.update(operation.to_bytes());
    digest_bytes(&mut hasher, table.payload());
    digest_bytes(&mut hasher, target_ref.as_str().as_bytes());
    match intent {
        ConnectorRowMutationIntent::Delete => hasher.update([1]),
        ConnectorRowMutationIntent::Update => hasher.update([2]),
        ConnectorRowMutationIntent::Merge { effects } => {
            hasher.update([3]);
            for effect in effects {
                hasher.update([effect_tag(*effect)]);
            }
        }
    };
    hasher.update(base.digest());
    hasher.update(contract.digest());
    hasher.update([strategy_tag(strategy)]);
    digest_bytes(&mut hasher, payload);
    hasher.finalize().into()
}
fn selection_digest(batches: &[RecordBatch], rows: u64, bytes: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-selection.v1\0");
    hasher.update(rows.to_be_bytes());
    hasher.update(bytes.to_be_bytes());
    for batch in batches {
        hasher.update((batch.num_rows() as u64).to_be_bytes());
        hasher.update((batch.get_array_memory_size() as u64).to_be_bytes());
        digest_bytes(&mut hasher, format!("{:?}", batch.schema()).as_bytes());
    }
    hasher.finalize().into()
}
const fn effect_tag(effect: ConnectorRowMutationEffect) -> u8 {
    match effect {
        ConnectorRowMutationEffect::Delete => 1,
        ConnectorRowMutationEffect::Replace => 2,
        ConnectorRowMutationEffect::Insert => 3,
    }
}
const fn strategy_tag(strategy: ConnectorRowMutationStrategy) -> u8 {
    match strategy {
        ConnectorRowMutationStrategy::PositionDelete => 1,
        ConnectorRowMutationStrategy::DeletionVector => 2,
        ConnectorRowMutationStrategy::MergeOnRead => 3,
        ConnectorRowMutationStrategy::CopyOnWrite => 4,
        ConnectorRowMutationStrategy::EqualityDelete => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn selection_is_non_concat_and_bounded() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .expect("batch");
        let bytes = batch.get_array_memory_size() as u64;
        let selection = ConnectorRowMutationSelection::try_new(vec![batch.clone()], 2, bytes)
            .expect("selection");
        assert_eq!(selection.batches().len(), 1);
        assert_eq!(selection.row_count(), 2);
        assert_eq!(
            ConnectorRowMutationSelection::try_new(vec![batch], 1, bytes)
                .expect_err("rows")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn merge_rejects_empty_or_duplicate_effects() {
        assert_eq!(
            ConnectorRowMutationIntent::Merge { effects: vec![] }
                .validate()
                .expect_err("empty")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorRowMutationIntent::Merge {
                effects: vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Delete
                ]
            }
            .validate()
            .expect_err("duplicate")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
