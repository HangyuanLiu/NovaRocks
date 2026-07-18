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

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::node::ExecPlan;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct FragmentContractVersion(u16);

impl FragmentContractVersion {
    pub(crate) const CURRENT: Self = Self(1);

    pub(crate) const fn new(value: u16) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct FragmentNodeId(i32);

impl FragmentNodeId {
    pub(crate) const fn new(value: i32) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RuntimeFilterId(i32);

impl RuntimeFilterId {
    pub(crate) const fn new(value: i32) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FragmentProgramOptions {
    contract_version: FragmentContractVersion,
}

impl FragmentProgramOptions {
    pub(crate) const fn new(contract_version: FragmentContractVersion) -> Self {
        Self { contract_version }
    }

    pub(crate) const fn contract_version(&self) -> FragmentContractVersion {
        self.contract_version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScanAssignmentKind {
    File,
    StarRocksTablet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScanSourceContract {
    assignment_kind: ScanAssignmentKind,
}

impl ScanSourceContract {
    pub(crate) const fn new(assignment_kind: ScanAssignmentKind) -> Self {
        Self { assignment_kind }
    }

    pub(crate) const fn assignment_kind(&self) -> ScanAssignmentKind {
        self.assignment_kind
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExchangeInputContract {
    expected_schema: ChunkSchemaRef,
}

impl ExchangeInputContract {
    pub(crate) fn new(expected_schema: ChunkSchemaRef) -> Self {
        Self { expected_schema }
    }

    pub(crate) fn expected_schema(&self) -> &ChunkSchemaRef {
        &self.expected_schema
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterContract {
    build_filters: BTreeSet<RuntimeFilterId>,
    probe_filters: BTreeSet<RuntimeFilterId>,
}

impl RuntimeFilterContract {
    pub(crate) fn new(
        build_filters: BTreeSet<RuntimeFilterId>,
        probe_filters: BTreeSet<RuntimeFilterId>,
    ) -> Self {
        Self {
            build_filters,
            probe_filters,
        }
    }

    pub(crate) fn build_filters(&self) -> &BTreeSet<RuntimeFilterId> {
        &self.build_filters
    }

    pub(crate) fn probe_filters(&self) -> &BTreeSet<RuntimeFilterId> {
        &self.probe_filters
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FragmentSinkKind {
    Result,
    Noop,
    DataStream,
    MultiCastDataStream,
    SplitDataStream,
    IcebergChangeStreamRouter,
    SchemaTable,
    IcebergTable,
    OlapTable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FragmentSinkAssignmentKind {
    StreamDestinations,
    DestinationGroups(NonZeroUsize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FragmentSinkAssignmentRequirement {
    None,
    Required(FragmentSinkAssignmentKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FragmentSinkSpec {
    kind: FragmentSinkKind,
    assignment_requirement: FragmentSinkAssignmentRequirement,
}

impl FragmentSinkSpec {
    pub(crate) fn try_for_kind(
        kind: FragmentSinkKind,
        destination_group_count: Option<NonZeroUsize>,
    ) -> Result<Self, FragmentBindingError> {
        use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
        use FragmentSinkAssignmentRequirement::{None, Required};

        let assignment_requirement = match kind {
            FragmentSinkKind::MultiCastDataStream
            | FragmentSinkKind::SplitDataStream
            | FragmentSinkKind::IcebergChangeStreamRouter => {
                let count = destination_group_count.ok_or_else(|| {
                    FragmentBindingError::new(
                        FragmentBindingTarget::Sink,
                        FragmentBindingErrorKind::InvalidAssignment,
                        format!("sink {kind:?} requires a destination group count"),
                    )
                })?;
                Required(DestinationGroups(count))
            }
            FragmentSinkKind::DataStream => {
                if let Some(count) = destination_group_count {
                    return Err(FragmentBindingError::new(
                        FragmentBindingTarget::Sink,
                        FragmentBindingErrorKind::InvalidAssignment,
                        format!(
                            "sink {kind:?} does not accept destination group count {}",
                            count.get()
                        ),
                    ));
                }
                Required(StreamDestinations)
            }
            FragmentSinkKind::Result
            | FragmentSinkKind::Noop
            | FragmentSinkKind::SchemaTable
            | FragmentSinkKind::IcebergTable
            | FragmentSinkKind::OlapTable => {
                if let Some(count) = destination_group_count {
                    return Err(FragmentBindingError::new(
                        FragmentBindingTarget::Sink,
                        FragmentBindingErrorKind::InvalidAssignment,
                        format!(
                            "sink {kind:?} does not accept destination group count {}",
                            count.get()
                        ),
                    ));
                }
                None
            }
        };
        Ok(Self {
            kind,
            assignment_requirement,
        })
    }

    pub(crate) const fn kind(&self) -> FragmentSinkKind {
        self.kind
    }

    pub(crate) const fn assignment_requirement(&self) -> FragmentSinkAssignmentRequirement {
        self.assignment_requirement
    }
}

#[derive(Debug)]
pub(crate) struct FragmentProgram {
    plan: ExecPlan,
    sink: FragmentSinkSpec,
    program_options: FragmentProgramOptions,
    scan_sources: BTreeMap<FragmentNodeId, ScanSourceContract>,
    exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
    runtime_filters: RuntimeFilterContract,
}

impl FragmentProgram {
    pub(crate) fn new(
        plan: ExecPlan,
        sink: FragmentSinkSpec,
        program_options: FragmentProgramOptions,
        scan_sources: BTreeMap<FragmentNodeId, ScanSourceContract>,
        exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
        runtime_filters: RuntimeFilterContract,
    ) -> Self {
        Self {
            plan,
            sink,
            program_options,
            scan_sources,
            exchange_inputs,
            runtime_filters,
        }
    }

    pub(crate) fn plan(&self) -> &ExecPlan {
        &self.plan
    }

    pub(crate) const fn sink(&self) -> &FragmentSinkSpec {
        &self.sink
    }

    pub(crate) const fn program_options(&self) -> &FragmentProgramOptions {
        &self.program_options
    }

    pub(crate) fn scan_sources(&self) -> &BTreeMap<FragmentNodeId, ScanSourceContract> {
        &self.scan_sources
    }

    pub(crate) fn exchange_inputs(&self) -> &BTreeMap<FragmentNodeId, ExchangeInputContract> {
        &self.exchange_inputs
    }

    pub(crate) const fn runtime_filters(&self) -> &RuntimeFilterContract {
        &self.runtime_filters
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprArena;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};

    use super::*;

    fn values_plan() -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 7,
                }),
            },
        }
    }

    #[test]
    fn stable_ids_are_typed_ordered_keys() {
        assert_eq!(FragmentContractVersion::CURRENT.get(), 1);
        assert_eq!(FragmentContractVersion::new(9).get(), 9);
        assert_eq!(FragmentNodeId::new(11).get(), 11);
        assert_eq!(RuntimeFilterId::new(11).get(), 11);

        let nodes = BTreeMap::from([(FragmentNodeId::new(3), "scan")]);
        assert_eq!(nodes.get(&FragmentNodeId::new(3)), Some(&"scan"));
        let filters = BTreeSet::from([RuntimeFilterId::new(5), RuntimeFilterId::new(2)]);
        assert_eq!(
            filters.iter().map(|id| id.get()).collect::<Vec<_>>(),
            vec![2, 5]
        );
    }

    #[test]
    fn sink_assignment_requirement_is_derived_from_kind() {
        use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
        use FragmentSinkAssignmentRequirement::{None, Required};
        use FragmentSinkKind::{
            DataStream, IcebergChangeStreamRouter, IcebergTable, MultiCastDataStream, Noop,
            OlapTable, Result, SchemaTable, SplitDataStream,
        };

        let two = NonZeroUsize::new(2).expect("non-zero group count");
        assert_eq!(
            FragmentSinkSpec::try_for_kind(DataStream, Option::<NonZeroUsize>::None)
                .expect("data stream sink")
                .assignment_requirement(),
            Required(StreamDestinations)
        );
        for kind in [Result, Noop, SchemaTable, IcebergTable, OlapTable] {
            assert_eq!(
                FragmentSinkSpec::try_for_kind(kind, Option::<NonZeroUsize>::None)
                    .expect("non-grouped sink")
                    .assignment_requirement(),
                None
            );
            let error = FragmentSinkSpec::try_for_kind(kind, Some(two))
                .expect_err("non-grouped sink rejects group count");
            assert_eq!(error.target(), FragmentBindingTarget::Sink);
            assert_eq!(error.kind(), FragmentBindingErrorKind::InvalidAssignment);
        }
        for kind in [
            MultiCastDataStream,
            SplitDataStream,
            IcebergChangeStreamRouter,
        ] {
            assert_eq!(
                FragmentSinkSpec::try_for_kind(kind, Some(two))
                    .expect("grouped sink")
                    .assignment_requirement(),
                Required(DestinationGroups(two))
            );
            let error = FragmentSinkSpec::try_for_kind(kind, Option::<NonZeroUsize>::None)
                .expect_err("grouped sink requires group count");
            assert_eq!(error.target(), FragmentBindingTarget::Sink);
            assert_eq!(error.kind(), FragmentBindingErrorKind::InvalidAssignment);
        }
        let error = FragmentSinkSpec::try_for_kind(DataStream, Some(two))
            .expect_err("data stream rejects group count");
        assert_eq!(error.target(), FragmentBindingTarget::Sink);
        assert_eq!(error.kind(), FragmentBindingErrorKind::InvalidAssignment);
    }

    #[test]
    fn program_exposes_immutable_static_contracts() {
        let scan_sources = BTreeMap::from([(
            FragmentNodeId::new(10),
            ScanSourceContract::new(ScanAssignmentKind::File),
        )]);
        let expected_schema = Arc::new(ChunkSchema::empty());
        let exchange_inputs = BTreeMap::from([(
            FragmentNodeId::new(20),
            ExchangeInputContract::new(Arc::clone(&expected_schema)),
        )]);
        let runtime_filters = RuntimeFilterContract::new(
            BTreeSet::from([RuntimeFilterId::new(30)]),
            BTreeSet::from([RuntimeFilterId::new(31)]),
        );
        let options = FragmentProgramOptions::new(FragmentContractVersion::CURRENT);
        let program = FragmentProgram::new(
            values_plan(),
            FragmentSinkSpec::try_for_kind(FragmentSinkKind::Result, None).expect("result sink"),
            options,
            scan_sources,
            exchange_inputs,
            runtime_filters,
        );

        assert!(matches!(program.plan().root.kind, ExecNodeKind::Values(_)));
        assert_eq!(program.sink().kind(), FragmentSinkKind::Result);
        assert_eq!(
            program.program_options().contract_version(),
            FragmentContractVersion::CURRENT
        );
        assert_eq!(
            program
                .scan_sources()
                .get(&FragmentNodeId::new(10))
                .map(ScanSourceContract::assignment_kind),
            Some(ScanAssignmentKind::File)
        );
        assert!(Arc::ptr_eq(
            program
                .exchange_inputs()
                .get(&FragmentNodeId::new(20))
                .expect("exchange contract")
                .expected_schema(),
            &expected_schema
        ));
        assert_eq!(
            program
                .runtime_filters()
                .build_filters()
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![30]
        );
        assert_eq!(
            program
                .runtime_filters()
                .probe_filters()
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![31]
        );
    }
}
