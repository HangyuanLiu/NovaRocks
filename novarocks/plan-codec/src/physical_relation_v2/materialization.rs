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

//! Selected owned relations through the original receiving grammar. No
//! provider interpretation or unchecked Relation clone is introduced here.

use super::*;

/// A loan of one original definition and its exact checked read/type owners.
/// Temporary single-definition index/Vec requests remain in the original
/// relation model even though they are dropped after moving out the value.
pub struct PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control> {
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    definition: &'wire wire::RelationDefinition,
    source: usize,
    limits: RelationProjectionLimits,
    facts: RelationProjectionFacts,
}
impl PreparedRelationMaterialization<'_, '_, '_, '_> {
    pub fn facts(&self) -> &RelationProjectionFacts {
        &self.facts
    }
    pub fn source_id(&self) -> u32 {
        self.definition.id
    }
    pub fn emit(self) -> Result<p::Relation, Error> {
        let mut work =
            CompileCheckpoints::try_new(self.namespace.original_control(), CompilePhase::Decode)?;
        let result = self.emit_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn emit_observed(
        self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<p::Relation, Error> {
        let mut decoded = decode_core(
            std::slice::from_ref(self.definition),
            self.namespace.reads,
            self.namespace.types,
            self.source,
            self.limits,
            work,
        )?;
        let relation = decoded.relations.pop();
        work.step()?;
        relation.ok_or_else(|| invalid("selected relation materialization produced no value"))
    }
}

pub fn prepare_relation_materialization<'borrow, 'loan, 'wire, 'control>(
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    id: u32,
    source: usize,
    limits: RelationProjectionLimits,
) -> Result<PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(namespace.original_control(), CompilePhase::Decode)?;
    let result = prepare_observed(namespace, id, source, limits, &mut work);
    finish(result, work)
}

/// The combined route shares one checkpoint meter across preparation and
/// materialization. The caller supplies the complete retained union invoice.
pub fn materialize_relation(
    namespace: &DecodedRelations<'_, '_, '_>,
    id: u32,
    source: usize,
    limits: RelationProjectionLimits,
) -> Result<p::Relation, Error> {
    let mut work = CompileCheckpoints::try_new(namespace.original_control(), CompilePhase::Decode)?;
    let result = prepare_observed(namespace, id, source, limits, &mut work)
        .and_then(|prepared| prepared.emit_observed(&mut work));
    finish(result, work)
}

pub(crate) fn prepare_observed<'borrow, 'loan, 'wire, 'control>(
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    id: u32,
    source: usize,
    limits: RelationProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control>, Error> {
    // Admission for the actual retained namespace floor walk and sparse index
    // lookup precedes either operation. It is not charged as output memory.
    let overhead = add(
        1024,
        add(
            mul(namespace.source_count(), 8)?,
            crate::btree_resources_v2::lookup_work(namespace.source_count()).map_err(invalid)?,
        )?,
    )?;
    cap(overhead, limits.max_work, work)?;
    floor(source, namespace.retained_floor_observed(work)?, work)?;
    let at = namespace
        .indices
        .find(id, |at| namespace.wire[at].id, work)?;
    work.step()?;
    let at = at.ok_or_else(|| invalid("selected relation ID is absent from original namespace"))?;
    let definition = &namespace.wire[at];
    let mut facts = preflight_decode_observed(
        std::slice::from_ref(definition),
        namespace.reads,
        namespace.types,
        source,
        limits,
        work,
    )?;
    // The original preflight bound includes its own emission. A consuming
    // materialization reuses that same author and grammar, including its
    // preflight, so charge two complete passes without copying formulas.
    facts.cumulative_work_upper_bound = add(overhead, mul(facts.cumulative_work_upper_bound, 2)?)?;
    cap(facts.cumulative_work_upper_bound, limits.max_work, work)?;
    Ok(PreparedRelationMaterialization {
        namespace,
        definition,
        source,
        limits,
        facts,
    })
}
