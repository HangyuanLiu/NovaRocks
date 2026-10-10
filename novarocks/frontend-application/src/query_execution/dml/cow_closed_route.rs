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

// Preserve the original sealed route validation at the private COW boundary.
// whole-target preflight is a mandatory caller input, not a fallback constant.
use super::cow_closed_ast as closed;
use closed::BuildError;
use novarocks_parser::ast::{BinaryOperator, Expr, Ident, Query};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;

type Gate<'a> = &'a dyn Fn(&dyn arrow::array::Array, usize) -> closed::Result<()>;
type Check<'a> = &'a dyn Fn() -> closed::Result<()>;

type Input = novarocks_spi::connector::ConnectorWriteInputShape;
type FieldBinding = novarocks_spi::connector::ConnectorWriteFieldBinding;

// Original enum iteration order, with no fields() Vec or deep field clone.
fn input_slices(input: &Input) -> (&[FieldBinding], &[FieldBinding]) {
    match input {
        Input::Data { fields } => (fields, &[]),
        Input::RowLineage {
            data_fields,
            row_identity_fields,
        } => (data_fields, row_identity_fields),
        Input::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | Input::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => (identity_fields, partition_source_fields),
        Input::EqualityDelete { equality_fields } => (equality_fields, &[]),
    }
}
fn input_fields(input: &Input) -> impl Iterator<Item = &FieldBinding> {
    let (first, second) = input_slices(input);
    first.iter().chain(second.iter())
}
fn check_input_order(
    input: &Input,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
) -> closed::Result<usize> {
    let (first, second) = input_slices(input);
    let width = first
        .len()
        .checked_add(second.len())
        .ok_or(BuildError::ResourceExhausted)?;
    if route.input_ordinals().len() != width {
        return Err(BuildError::InvalidSource(
            "COW route input width differs from its target input",
        ));
    }
    for (ordinal, field) in route.input_ordinals().iter().zip(input_fields(input)) {
        if ordinal.token() != field.token() {
            return Err(BuildError::InvalidSource(
                "COW route token order differs from its target input",
            ));
        }
    }
    Ok(width)
}

fn selection_value_ast(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    row: novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal,
    field_ordinal: u32,
    field: &arrow::datatypes::Field,
    preflight: Gate<'_>,
    check: Check<'_>,
) -> closed::Result<Expr> {
    let view = selection.locate(row).ok_or(BuildError::InvalidSource(
        "COW selection ordinal is out of bounds",
    ))?;
    let array =
        view.batch()
            .columns()
            .get(field_ordinal as usize)
            .ok_or(BuildError::InvalidSource(
                "COW selection field ordinal is out of bounds",
            ))?;
    let value = closed::checked_value(array.as_ref(), view.row_index(), &preflight, &check)?;
    closed::cast(value, field.data_type())
}

pub(super) fn build_cow_append_query_closed(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    preflight: Gate<'_>,
    check: Check<'_>,
) -> closed::Result<Query> {
    route
        .validate_selection_contract(input, contract)
        .map_err(|e| BuildError::OriginalSemantic(e.to_string()))?;
    let input_width = check_input_order(input, route)?;
    let mut value_rows = closed::exact_vec(rows.len())?;
    for row in rows {
        check()?;
        let mut values = closed::exact_vec(input_width)?;
        for (_writer, binding) in input_fields(input).zip(route.selection_bindings()) {
            let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                token, ordinal, role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage,
            } = binding.source() else { return Err(BuildError::InvalidSource("COW append writer is not bound to an after-image")); };
            let signed = contract
                .selection_field(token)
                .ok_or(BuildError::InvalidSource(
                    "COW append carries a foreign selection token",
                ))?;
            if signed.ordinal() != ordinal
                || signed.role()
                    != novarocks_spi::connector::ConnectorMutationSelectionFieldRole::AfterImage
            {
                return Err(BuildError::InvalidSource(
                    "COW append selection binding differs from its signed field",
                ));
            }
            values.push(selection_value_ast(
                selection,
                *row,
                signed.ordinal(),
                signed.field(),
                preflight,
                check,
            )?);
        }
        value_rows.push(values);
    }
    let mut aliases: Vec<Ident> = closed::exact_vec(input_width)?;
    let mut projection = closed::exact_vec(input_width)?;
    for (ordinal, binding) in input_fields(input).enumerate() {
        let alias = closed::value_alias(ordinal)?;
        aliases.push(closed::ident(&alias, true)?);
        projection.push(closed::item(
            closed::cast(
                closed::column("__nr_values", &alias)?,
                binding.field().data_type(),
            )?,
            binding.field().name(),
        )?);
    }
    closed::select(
        projection,
        closed::values(value_rows, "__nr_values", aliases)?,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_cow_rewrite_query_closed(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    source: &novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    identity: &FrozenConnectorScanIdentity,
    preflight: Gate<'_>,
    check: Check<'_>,
) -> closed::Result<Query> {
    route
        .validate_selection_contract(input, contract)
        .map_err(|e| BuildError::OriginalSemantic(e.to_string()))?;
    let input_width = check_input_order(input, route)?;
    // Borrowed linear lookups preserve the signed token facts without allocating
    // copied HashMaps of the whole routing proof during AST construction.
    let selection_binding = |token| {
        route
            .selection_bindings()
            .iter()
            .find(|b| b.writer_token() == token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite token has no provider selection binding",
            ))
    };
    let mut inherited =
        route.selection_bindings().iter().filter(|b| {
            matches!(b.source(),
        novarocks_spi::connector::write_stack::ConnectorWriteValueSource::ProviderDerived(
            novarocks_spi::connector::write_stack::ConnectorWriteProviderDerivedValue::Inherit))
        });
    let written_version_matches = match (
        inherited.next(),
        inherited.next(),
        source.written_version_token(),
    ) {
        (None, None, None) => true,
        (Some(binding), None, Some(token)) => binding.writer_token() == token,
        _ => false,
    };
    if !written_version_matches {
        return Err(BuildError::InvalidSource(
            "COW rewrite written-version mapping differs from its frozen source",
        ));
    }
    let maximum_tokens = source
        .match_tokens()
        .len()
        .checked_add(input_width)
        .ok_or(BuildError::ResourceExhausted)?;
    // A scalar count pass makes the actual Vec capacity equal its final length.
    let is_new_after =
        |binding: &novarocks_spi::connector::ConnectorWriteFieldBinding| -> closed::Result<bool> {
            Ok(matches!(selection_binding(binding.token())?.source(), novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, ..
        }) && !source.match_tokens().contains(&binding.token()))
        };
    let mut token_count = source.match_tokens().len();
    for binding in input_fields(input) {
        if is_new_after(binding)? {
            token_count = token_count
                .checked_add(1)
                .ok_or(BuildError::ResourceExhausted)?;
        }
    }
    if token_count > maximum_tokens {
        return Err(BuildError::InvalidSource("COW route token cardinality"));
    }
    let mut values_tokens = closed::exact_vec(token_count)?;
    values_tokens.extend_from_slice(source.match_tokens());
    for binding in input_fields(input) {
        if is_new_after(binding)? {
            values_tokens.push(binding.token());
        }
    }
    let selection_field = |token| {
        let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            token: selection_token,
            ordinal,
            ..
        } = selection_binding(token)?.source()
        else {
            return Err(BuildError::InvalidSource(
                "COW derived writer value has no selection field",
            ));
        };
        let signed = contract
            .selection_field(selection_token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite carries a foreign selection token",
            ))?;
        if signed.ordinal() != ordinal {
            return Err(BuildError::InvalidSource(
                "COW rewrite selection binding has a tampered ordinal",
            ));
        }
        Ok::<_, BuildError>((signed.ordinal(), signed.field()))
    };
    let width = values_tokens
        .len()
        .checked_add(2)
        .ok_or(BuildError::ResourceExhausted)?;
    let mut value_rows = closed::exact_vec(rows.len())?;
    for row in rows {
        check()?;
        let mut values = closed::exact_vec(width)?;
        for token in &values_tokens {
            let (ordinal, field) = selection_field(*token)?;
            values.push(selection_value_ast(
                selection, *row, ordinal, field, preflight, check,
            )?);
        }
        values.push(closed::bool_value(true));
        values.push(selection_value_ast(
            selection,
            *row,
            contract.effect_field().target_ordinal(),
            contract.effect_field().field(),
            preflight,
            check,
        )?);
        value_rows.push(values);
    }
    let mut aliases = closed::exact_vec(width)?;
    for ordinal in 0..values_tokens.len() {
        aliases.push(closed::ident(&closed::value_alias(ordinal)?, true)?);
    }
    aliases.push(closed::ident("__nr_matched", true)?);
    aliases.push(closed::ident("__nr_effect", true)?);
    let matched = || closed::column("__nr_match", "__nr_matched").map(|e| closed::is_null(e, true));
    let position = |token| {
        values_tokens
            .iter()
            .position(|t| *t == token)
            .ok_or(BuildError::InvalidSource(
                "COW after-image field has no VALUES binding",
            ))
    };
    let scan_column = |token| {
        let binding = source
            .scan_bindings()
            .iter()
            .find(|b| b.token() == token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite field has no scan binding",
            ))?;
        let field = source
            .scan_schema()
            .fields()
            .get(binding.scan_ordinal() as usize)
            .ok_or(BuildError::InvalidSource(
                "COW scan binding is outside the frozen scan schema",
            ))?;
        closed::column("__nr_scan", field.name())
    };
    let mut projection = closed::exact_vec(input_width)?;
    for binding in input_fields(input) {
        let scan = scan_column(binding.token())?;
        let expression = match selection_binding(binding.token())?.source() {
            novarocks_spi::connector::write_stack::ConnectorWriteValueSource::ProviderDerived(
                novarocks_spi::connector::write_stack::ConnectorWriteProviderDerivedValue::Inherit) => {
                if Some(binding.token()) != source.written_version_token() { return Err(BuildError::InvalidSource("COW inherited writer token differs from its frozen source")); }
                closed::case(matched()?, closed::null(), scan)?
            }
            novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, ..
            } => closed::case(matched()?, closed::column("__nr_match", &closed::value_alias(position(binding.token())?)?)?, scan)?,
            novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::Identity, ..
            } => scan,
        };
        projection.push(closed::item(
            closed::cast(expression, binding.field().data_type())?,
            binding.field().name(),
        )?);
    }
    let mut joins = None;
    for token in source.match_tokens() {
        let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            token: selection_token,
            role:
                novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::Identity,
            ..
        } = selection_binding(*token)?.source()
        else {
            return Err(BuildError::InvalidSource(
                "COW match token is not bound to a signed uniqueness identity",
            ));
        };
        if !contract.uniqueness_tokens().contains(&selection_token) {
            return Err(BuildError::InvalidSource(
                "COW match token is not bound to a signed uniqueness identity",
            ));
        }
        let equality = closed::binary(
            scan_column(*token)?,
            BinaryOperator::Equal,
            closed::column("__nr_match", &closed::value_alias(position(*token)?)?)?,
        );
        joins = Some(match joins {
            None => equality,
            Some(previous) => closed::binary(previous, BinaryOperator::And, equality),
        });
    }
    let joins = joins.ok_or(BuildError::InvalidSource(
        "COW rewrite branch carries no match key",
    ))?;
    let filter = closed::binary(
        closed::is_null(closed::column("__nr_match", "__nr_effect")?, false),
        BinaryOperator::Or,
        closed::binary(
            closed::column("__nr_match", "__nr_effect")?,
            BinaryOperator::NotEqual,
            closed::number(novarocks_spi::connector::ConnectorRowMutationEffect::Delete as i8)?,
        ),
    );
    closed::select(
        projection,
        closed::table(
            identity.catalog(),
            identity.namespace(),
            identity.table(),
            "__nr_scan",
        )?,
        Some((closed::values(value_rows, "__nr_match", aliases)?, joins)),
        Some(filter),
    )
}

use super::cow_compile_receipt::{
    borrowed_value_footprint as value_footprint, recipe::Recipe, route_skeleton, type_footprint,
};
pub(super) fn map_footprint(f: value_footprint::Failure) -> BuildError {
    match f {
        value_footprint::Failure::ResourceExhausted => BuildError::ResourceExhausted,
        value_footprint::Failure::Stopped => {
            BuildError::InvalidSource("original COW scope stopped")
        }
        value_footprint::Failure::InvalidSource(s) => BuildError::InvalidSource(s),
        value_footprint::Failure::OriginalSemantic(s) => BuildError::OriginalSemantic(s.into()),
    }
}
fn slots<T>(len: usize) -> closed::Result<u64> {
    (len as u64)
        .checked_mul(std::mem::size_of::<T>() as u64)
        .ok_or(BuildError::ResourceExhausted)
}
fn add_bytes(a: u64, b: u64) -> closed::Result<u64> {
    a.checked_add(b).ok_or(BuildError::ResourceExhausted)
}

// Caller first validates the sealed target/proof/base/single-file facts and
// produces actual new target metadata layout. This scalar is NOT a public
// authority: the private all-target caller must derive it before any clone.
#[allow(clippy::too_many_arguments)]
pub(super) fn preflight_target(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &Input,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    rewrite: Option<(
        &novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
        &FrozenConnectorScanIdentity,
    )>,
    owned_target_metadata_upper: u64,
    compiler_handoff_upper: u64,
    recipe: &mut Recipe,
    check: Check<'_>,
) -> closed::Result<()> {
    check()?;
    route
        .validate_selection_contract(input, contract)
        .map_err(|e| BuildError::OriginalSemantic(e.to_string()))?;
    let input_width = check_input_order(input, route)?;
    let remaining = recipe.remaining_for_target().map_err(map_footprint)?;
    let selection_binding = |token| {
        route
            .selection_bindings()
            .iter()
            .find(|b| b.writer_token() == token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite token has no provider selection binding",
            ))
    };
    let selected = |token| {
        let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            token,
            ordinal,
            ..
        } = selection_binding(token)?.source()
        else {
            return Err(BuildError::InvalidSource(
                "COW derived writer value has no selection field",
            ));
        };
        let signed = contract
            .selection_field(token)
            .ok_or(BuildError::InvalidSource("COW foreign selection token"))?;
        if signed.ordinal() != ordinal {
            return Err(BuildError::InvalidSource("COW tampered selection ordinal"));
        }
        Ok::<_, BuildError>(signed)
    };
    let mut values_tokens = Vec::new();
    let mut template = match rewrite {
        None => {
            let transient = slots::<route_skeleton::AppendColumn<'_>>(input_width)?;
            if transient > remaining {
                return Err(BuildError::ResourceExhausted);
            }
            let mut append = closed::exact_vec(input_width)?;
            for binding in input_fields(input) {
                let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                    role,
                    ..
                } = selection_binding(binding.token())?.source()
                else {
                    return Err(BuildError::InvalidSource(
                        "COW append writer is not bound to an after-image",
                    ));
                };
                let signed = selected(binding.token())?;
                if role != novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage
                    || signed.role() != novarocks_spi::connector::ConnectorMutationSelectionFieldRole::AfterImage {
                    return Err(BuildError::InvalidSource("COW append selection differs from its signed field"));
                }
                append.push(route_skeleton::AppendColumn {
                    writer: binding.field(),
                    signed_selection: signed.field(),
                });
            }
            route_skeleton::append(
                rows.len() as u64,
                &append,
                owned_target_metadata_upper,
                transient,
            )
            .map_err(map_footprint)?
        }
        Some((source, identity)) => {
            let count = source.match_tokens().len().checked_add(input_fields(input).filter(|b| {
                matches!(route.selection_bindings().iter().find(|s| s.writer_token() == b.token()).map(|s| s.source()),
                    Some(novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                        role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, .. }))
                    && !source.match_tokens().contains(&b.token())
            }).count()).ok_or(BuildError::ResourceExhausted)?;
            // Every temporary Vec is charged before allocation. Vec<Token> is
            // separately charged by route_skeleton for the actual builder.
            let views = add_bytes(
                add_bytes(
                    slots::<route_skeleton::RewriteWriter<'_>>(input_width)?,
                    slots::<route_skeleton::MatchKey<'_>>(source.match_tokens().len())?,
                )?,
                slots::<&arrow::datatypes::Field>(count)?,
            )?;
            let preflight_transient = add_bytes(
                views,
                slots::<novarocks_spi::connector::ConnectorWriteFieldToken>(count)?,
            )?;
            if preflight_transient > remaining {
                return Err(BuildError::ResourceExhausted);
            }
            values_tokens = closed::exact_vec(count)?;
            values_tokens.extend_from_slice(source.match_tokens());
            for binding in input_fields(input) {
                if matches!(selection_binding(binding.token())?.source(), novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                    role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, .. })
                    && !values_tokens.contains(&binding.token()) { values_tokens.push(binding.token()); }
            }
            if values_tokens.len() != count {
                return Err(BuildError::InvalidSource("COW duplicated writer token"));
            }
            let mut value_fields = closed::exact_vec(count)?;
            for token in &values_tokens {
                value_fields.push(selected(*token)?.field());
            }
            let scan = |token| {
                let b = source
                    .scan_bindings()
                    .iter()
                    .find(|b| b.token() == token)
                    .ok_or(BuildError::InvalidSource("COW missing scan token"))?;
                source
                    .scan_schema()
                    .fields()
                    .get(b.scan_ordinal() as usize)
                    .map(|f| f.as_ref())
                    .ok_or(BuildError::InvalidSource("COW scan ordinal"))
            };
            let position = |token| {
                values_tokens
                    .iter()
                    .position(|t| *t == token)
                    .map(|n| n as u64)
                    .ok_or(BuildError::InvalidSource("COW missing VALUES token"))
            };
            let mut writers = closed::exact_vec(input_width)?;
            for binding in input_fields(input) {
                let value = match selection_binding(binding.token())?.source() {
                    novarocks_spi::connector::write_stack::ConnectorWriteValueSource::ProviderDerived(
                        novarocks_spi::connector::write_stack::ConnectorWriteProviderDerivedValue::Inherit) => {
                        if Some(binding.token()) != source.written_version_token() { return Err(BuildError::InvalidSource("COW inherited writer token")); }
                        route_skeleton::RewriteValue::InheritWrittenVersion
                    }
                    novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                        role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::Identity, .. } => route_skeleton::RewriteValue::Identity,
                    novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                        role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, .. } =>
                        route_skeleton::RewriteValue::AfterImage { values_ordinal: position(binding.token())? },
                };
                writers.push(route_skeleton::RewriteWriter {
                    writer: binding.field(),
                    original_scan_field: scan(binding.token())?,
                    value,
                });
            }
            let mut keys = closed::exact_vec(source.match_tokens().len())?;
            for token in source.match_tokens() {
                let signed = selected(*token)?;
                if signed.role()
                    != novarocks_spi::connector::ConnectorMutationSelectionFieldRole::Identity
                    || !contract.uniqueness_tokens().contains(&signed.token())
                {
                    return Err(BuildError::InvalidSource("COW unsigned match identity"));
                }
                keys.push(route_skeleton::MatchKey {
                    original_scan_field: scan(*token)?,
                    values_ordinal: position(*token)?,
                });
            }
            route_skeleton::rewrite(route_skeleton::Rewrite {
                rows: rows.len() as u64,
                original_values_fields: &value_fields,
                signed_effect_field: contract.effect_field().field(),
                writers: &writers,
                original_match_keys: &keys,
                frozen_catalog: identity.catalog(),
                frozen_namespace: identity.namespace(),
                frozen_table: identity.table(),
                owned_target_metadata_upper,
                construction_views_upper: views,
            })
            .map_err(map_footprint)?
        }
    };
    template.constructor_transient_upper = template
        .constructor_transient_upper
        .max(compiler_handoff_upper);
    // The whole original window, including all four compiler copies, rejects
    // before any row/cell AST constructor. No row recipe storage is created.
    let mut target = recipe.begin_target(template).map_err(map_footprint)?;
    for row in rows {
        check()?;
        match rewrite {
            None => {
                for binding in input_fields(input) {
                    let signed = selected(binding.token())?;
                    preflight_signed_cell(
                        selection,
                        *row,
                        signed.ordinal(),
                        signed.field(),
                        remaining,
                        &mut target,
                        check,
                    )?;
                }
            }
            Some(_) => {
                for token in &values_tokens {
                    let signed = selected(*token)?;
                    preflight_signed_cell(
                        selection,
                        *row,
                        signed.ordinal(),
                        signed.field(),
                        remaining,
                        &mut target,
                        check,
                    )?;
                }
                target
                    .fixed_cell(std::mem::size_of::<Expr>() as u64, 0)
                    .map_err(map_footprint)?;
                let effect = contract.effect_field();
                preflight_signed_cell(
                    selection,
                    *row,
                    effect.target_ordinal(),
                    effect.field(),
                    remaining,
                    &mut target,
                    check,
                )?;
            }
        }
    }
    check()?;
    recipe.finish_target(target).map_err(map_footprint)
}

#[allow(clippy::too_many_arguments)]
fn preflight_signed_cell(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    row: novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal,
    ordinal: u32,
    field: &arrow::datatypes::Field,
    remaining: u64,
    target: &mut super::cow_compile_receipt::recipe::TargetAccumulator,
    check: Check<'_>,
) -> closed::Result<()> {
    let view = selection
        .locate(row)
        .ok_or(BuildError::InvalidSource("COW selection ordinal"))?;
    let array = view
        .batch()
        .columns()
        .get(ordinal as usize)
        .ok_or(BuildError::InvalidSource("COW field ordinal"))?;
    let value = borrowed_checked(array.as_ref(), view.row_index(), remaining, check)?;
    target
        .selection_cast(
            value,
            type_footprint::exact_type_heap(field.data_type()).map_err(map_footprint)?,
        )
        .map_err(map_footprint)
}

pub(super) fn borrowed_checked(
    array: &dyn arrow::array::Array,
    row: usize,
    remaining: u64,
    check: Check<'_>,
) -> closed::Result<value_footprint::ValueFootprint> {
    // Keep the original check error object. The footprint's Stopped enum is
    // only a control signal, not a replacement for the caller's primary.
    let mut first_error = None;
    let outcome =
        value_footprint::borrowed_value_with_control(
            array,
            row,
            remaining,
            &mut || match check() {
                Ok(()) => Ok(()),
                Err(error) => {
                    first_error = Some(error);
                    Err(value_footprint::Failure::Stopped)
                }
            },
        );
    match first_error {
        Some(error) => Err(error),
        None => outcome.map_err(map_footprint),
    }
}

/// Exact borrowed-view scratch requested by the first pass. The caller checks
/// this together with the original session and target containers before any
/// first-pass Vec or query-local identity is constructed.
pub(super) fn prospective_views_upper(
    input: &Input,
    source: Option<&novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource>,
) -> closed::Result<u64> {
    let (first, second) = input_slices(input);
    let width = first
        .len()
        .checked_add(second.len())
        .ok_or(BuildError::ResourceExhausted)?;
    let views = if let Some(source) = source {
        let maximum_tokens = source
            .match_tokens()
            .len()
            .checked_add(width)
            .ok_or(BuildError::ResourceExhausted)?;
        add_bytes(
            add_bytes(
                slots::<route_skeleton::RewriteWriter<'_>>(width)?,
                slots::<route_skeleton::MatchKey<'_>>(source.match_tokens().len())?,
            )?,
            add_bytes(
                slots::<&arrow::datatypes::Field>(maximum_tokens)?,
                slots::<novarocks_spi::connector::ConnectorWriteFieldToken>(maximum_tokens)?,
            )?,
        )?
    } else {
        slots::<route_skeleton::AppendColumn<'_>>(width)?
    };
    add_bytes(
        views,
        super::cow_compile_receipt::skeleton_shape::generated_value_alias_bytes(usize::MAX as u64),
    )
}
