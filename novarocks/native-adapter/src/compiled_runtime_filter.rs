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

//! The runtime-filter facts one compiled task is bound with.
//!
//! A compiled program names every runtime-filter site by its plan-global
//! binding identity and declares one `RuntimeFilter` binding requirement per
//! site. The package it was compiled from carries its own slice of that
//! numbering in its cuts: each binding names one filter and one endpoint of
//! it. This module projects that slice once per task, so that
//!
//! - the host asks the query context for a session hosting exactly the
//!   program's bindings for this fragment instance, and
//! - the scan binder matches each frozen scan dynamic filter, which is keyed
//!   by its runtime-filter id, to the one consumer binding of that scan.
//!
//! This milestone prunes nothing at the connector: a compiled scan's
//! consumers are applied to its rows by the compiled scan source, and the
//! connector keeps the truthful complete-all dynamic filter.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_connector_contract::StaticScanDynamicFilter;
use novarocks_local_program::{BindingRequirement, BindingRequirements, FilterConsumerAtExpr};
use novarocks_physical_plan::{
    FragmentPackage, RuntimeFilterApplyPoint, RuntimeFilterBindingRole, RuntimeFilterEndpoint,
};

/// What one binding is at its endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompiledRuntimeFilterRole {
    Producer,
    /// `scan_source` says the consumer applies at a scan's source, which is
    /// exactly the consumer a frozen scan dynamic filter describes.
    Consumer {
        scan_source: bool,
    },
}

/// The filter and endpoint one plan-global binding identity names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledRuntimeFilterEndpoint {
    filter_id: u32,
    node: u32,
    role: CompiledRuntimeFilterRole,
}

impl CompiledRuntimeFilterEndpoint {
    pub const fn new(filter_id: u32, node: u32, role: CompiledRuntimeFilterRole) -> Self {
        Self {
            filter_id,
            node,
            role,
        }
    }

    pub const fn filter_id(&self) -> u32 {
        self.filter_id
    }

    /// The physical node of the endpoint, in the package's own fragment.
    pub const fn node(&self) -> u32 {
        self.node
    }

    pub const fn role(&self) -> CompiledRuntimeFilterRole {
        self.role
    }
}

/// Every runtime-filter binding one package numbers, by binding identity.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompiledRuntimeFilterEndpoints {
    by_binding: BTreeMap<u32, CompiledRuntimeFilterEndpoint>,
}

impl CompiledRuntimeFilterEndpoints {
    /// Projects the binding table of `package`'s cuts. Every binding must
    /// resolve to an endpoint of one of the package's own filter contracts,
    /// in the package's own fragment; anything else is refused rather than
    /// skipped.
    pub fn from_package(package: &FragmentPackage) -> Result<Self, String> {
        let fragment = package.fragment().id();
        let cuts = package.cuts();
        let filters = cuts
            .runtime_filters
            .iter()
            .map(|filter| (filter.id, filter))
            .collect::<BTreeMap<_, _>>();
        let local = |binding_id: u32, endpoint: &RuntimeFilterEndpoint| {
            if endpoint.fragment == fragment {
                Ok(endpoint.node.get())
            } else {
                Err(format!(
                    "runtime-filter binding_id={binding_id} names an endpoint in fragment {}, \
                     not in its package's fragment {}",
                    endpoint.fragment.get(),
                    fragment.get()
                ))
            }
        };
        let mut endpoints = Vec::with_capacity(cuts.runtime_filter_bindings.len());
        for cut in cuts.runtime_filter_bindings.iter() {
            let filter = filters.get(&cut.filter).ok_or_else(|| {
                format!(
                    "runtime-filter binding_id={} names filter {}, which the package's cuts do \
                     not carry",
                    cut.binding_id,
                    cut.filter.get()
                )
            })?;
            let endpoint = match cut.role {
                RuntimeFilterBindingRole::Producer(index) => {
                    let producer = filter.producers.get(index).ok_or_else(|| {
                        format!(
                            "runtime-filter binding_id={} names absent producer {index} of \
                             filter {}",
                            cut.binding_id,
                            filter.id.get()
                        )
                    })?;
                    CompiledRuntimeFilterEndpoint::new(
                        filter.id.get(),
                        local(cut.binding_id, &producer.endpoint)?,
                        CompiledRuntimeFilterRole::Producer,
                    )
                }
                RuntimeFilterBindingRole::Consumer(index) => {
                    let consumer = filter.consumers.get(index).ok_or_else(|| {
                        format!(
                            "runtime-filter binding_id={} names absent consumer {index} of \
                             filter {}",
                            cut.binding_id,
                            filter.id.get()
                        )
                    })?;
                    CompiledRuntimeFilterEndpoint::new(
                        filter.id.get(),
                        local(cut.binding_id, &consumer.endpoint)?,
                        CompiledRuntimeFilterRole::Consumer {
                            scan_source: consumer.apply_point
                                == RuntimeFilterApplyPoint::ScanSource,
                        },
                    )
                }
            };
            endpoints.push((cut.binding_id, endpoint));
        }
        Self::try_from_endpoints(endpoints)
    }

    /// One table from `(binding_id, endpoint)` pairs; a binding named twice
    /// is refused.
    pub fn try_from_endpoints(
        endpoints: impl IntoIterator<Item = (u32, CompiledRuntimeFilterEndpoint)>,
    ) -> Result<Self, String> {
        let mut by_binding = BTreeMap::new();
        for (binding_id, endpoint) in endpoints {
            if by_binding.insert(binding_id, endpoint).is_some() {
                return Err(format!(
                    "runtime-filter binding_id={binding_id} is numbered twice"
                ));
            }
        }
        Ok(Self { by_binding })
    }

    pub fn get(&self, binding_id: u32) -> Option<&CompiledRuntimeFilterEndpoint> {
        self.by_binding.get(&binding_id)
    }

    pub fn binding_ids(&self) -> BTreeSet<u32> {
        self.by_binding.keys().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.by_binding.is_empty()
    }
}

/// The runtime-filter bindings a compiled program's `requirements` name,
/// which must be exactly the bindings its package numbers.
///
/// The program's `RuntimeFilter` requirements are its statement of the
/// bindings it executes; Execution separately proves each one has exactly one
/// site. A program that requires a binding its package never numbered, or
/// that drops one its package numbers, would bind a session that disagrees
/// with what the frontend deployed for this fragment, so both are refused.
/// An empty answer is the ordinary case: the task binds no runtime filter.
pub fn program_runtime_filter_bindings(
    requirements: &BindingRequirements,
    endpoints: &CompiledRuntimeFilterEndpoints,
) -> Result<BTreeSet<u32>, String> {
    let mut bindings = BTreeSet::new();
    for requirement in requirements.entries() {
        let BindingRequirement::RuntimeFilter { binding_id } = requirement else {
            continue;
        };
        let binding_id = u32::try_from(*binding_id).map_err(|_| {
            format!("compiled program requires negative runtime-filter binding_id={binding_id}")
        })?;
        if !bindings.insert(binding_id) {
            return Err(format!(
                "compiled program requires runtime-filter binding_id={binding_id} twice"
            ));
        }
    }
    let numbered = endpoints.binding_ids();
    if let Some(binding_id) = bindings.difference(&numbered).next() {
        return Err(format!(
            "compiled program requires runtime-filter binding_id={binding_id}, which its \
             package does not number"
        ));
    }
    if let Some(binding_id) = numbered.difference(&bindings).next() {
        return Err(format!(
            "package runtime-filter binding_id={binding_id} has no requirement in its \
             compiled program"
        ));
    }
    Ok(bindings)
}

/// One compiled scan's frozen dynamic filters against its consumer sites.
///
/// A frozen dynamic filter is keyed by its runtime-filter id; a consumer site
/// is keyed by its plan-global binding identity. Every consumer site of the
/// scan must be a consumer endpoint of this scan node, and the scan-source
/// consumers and the dynamic filters must correspond one to one through the
/// package's binding table. A consumer applied elsewhere at the scan carries
/// no dynamic filter. Nothing here subscribes or prunes: the correspondence
/// is what lets the connector's filter later be keyed by binding.
pub fn validate_scan_dynamic_filters(
    scan_node: u32,
    dynamic_filters: &[StaticScanDynamicFilter],
    consumers: &[FilterConsumerAtExpr],
    endpoints: &CompiledRuntimeFilterEndpoints,
) -> Result<(), String> {
    let mut by_filter = BTreeMap::<u32, Vec<u32>>::new();
    for site in consumers {
        let binding_id = site.consumer.binding_id();
        let endpoint = endpoints.get(binding_id).ok_or_else(|| {
            format!("runtime-filter binding_id={binding_id} is not numbered by the scan's package")
        })?;
        let CompiledRuntimeFilterRole::Consumer { scan_source } = endpoint.role() else {
            return Err(format!(
                "runtime-filter binding_id={binding_id} is a producer, not a consumer of the scan"
            ));
        };
        if endpoint.node() != scan_node {
            return Err(format!(
                "runtime-filter binding_id={binding_id} consumes at node {}, not at this scan",
                endpoint.node()
            ));
        }
        if scan_source {
            by_filter
                .entry(endpoint.filter_id())
                .or_default()
                .push(binding_id);
        }
    }
    let mut frozen = BTreeSet::new();
    for dynamic in dynamic_filters {
        let filter_id = dynamic.filter_id();
        if !frozen.insert(filter_id) {
            return Err(format!(
                "frozen dynamic filter of runtime filter {filter_id} is stated twice"
            ));
        }
        match by_filter.get(&filter_id).map(Vec::as_slice) {
            Some([_]) => {}
            None | Some([]) => {
                return Err(format!(
                    "frozen dynamic filter of runtime filter {filter_id} has no scan-source \
                     consumer binding at this scan"
                ));
            }
            Some(bindings) => {
                return Err(format!(
                    "frozen dynamic filter of runtime filter {filter_id} corresponds to \
                     scan-source consumer bindings {bindings:?}, not exactly one"
                ));
            }
        }
    }
    if let Some((filter_id, bindings)) = by_filter
        .iter()
        .find(|(filter_id, _)| !frozen.contains(filter_id))
    {
        return Err(format!(
            "scan-source consumer bindings {bindings:?} of runtime filter {filter_id} have no \
             frozen dynamic filter"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_local_program::{
        FilterConsumerActivation, FilterNullSemantics, FilterReduction, ProgramExprId,
        StaticFilterConsumer, StaticFilterContract,
    };

    use super::*;

    const SCAN: u32 = 10;

    fn consumer(binding_id: u32) -> FilterConsumerAtExpr {
        FilterConsumerAtExpr {
            expr_id: ProgramExprId::new(0),
            consumer: StaticFilterConsumer::try_new(
                binding_id,
                7,
                FilterConsumerActivation::BlockingSnapshot,
                StaticFilterContract::membership(
                    &DataType::Int64,
                    FilterNullSemantics::NeverMatches,
                )
                .expect("membership contract"),
                FilterReduction::SetUnion,
            )
            .expect("blocking membership consumer"),
        }
    }

    fn dynamic(filter_id: u32) -> StaticScanDynamicFilter {
        StaticScanDynamicFilter::new(filter_id, Arc::from("v0"))
    }

    fn endpoints(
        entries: &[(u32, u32, u32, CompiledRuntimeFilterRole)],
    ) -> CompiledRuntimeFilterEndpoints {
        CompiledRuntimeFilterEndpoints::try_from_endpoints(entries.iter().map(
            |(binding_id, filter_id, node, role)| {
                (
                    *binding_id,
                    CompiledRuntimeFilterEndpoint::new(*filter_id, *node, *role),
                )
            },
        ))
        .expect("distinct bindings")
    }

    const SCAN_SOURCE: CompiledRuntimeFilterRole =
        CompiledRuntimeFilterRole::Consumer { scan_source: true };
    const NODE_OUTPUT: CompiledRuntimeFilterRole =
        CompiledRuntimeFilterRole::Consumer { scan_source: false };

    #[test]
    fn each_dynamic_filter_matches_exactly_one_scan_source_consumer_binding() {
        // Filter 7 is consumed at the scan source through binding 3; filter
        // 9 is consumed at the scan's output through binding 4 and carries no
        // dynamic filter.
        let table = endpoints(&[(3, 7, SCAN, SCAN_SOURCE), (4, 9, SCAN, NODE_OUTPUT)]);
        validate_scan_dynamic_filters(SCAN, &[dynamic(7)], &[consumer(3), consumer(4)], &table)
            .expect("filter 7 is binding 3's");
        validate_scan_dynamic_filters(SCAN, &[], &[], &table).expect("an unfiltered scan");
    }

    #[test]
    fn a_dynamic_filter_without_its_consumer_binding_is_refused() {
        let table = endpoints(&[(3, 7, SCAN, SCAN_SOURCE)]);
        let missing = validate_scan_dynamic_filters(SCAN, &[dynamic(7)], &[], &table)
            .expect_err("no site consumes filter 7");
        assert!(
            missing.contains("has no scan-source consumer binding"),
            "{missing}"
        );
        let other = validate_scan_dynamic_filters(SCAN, &[dynamic(8)], &[consumer(3)], &table)
            .expect_err("filter 8 is nobody's");
        assert!(other.contains("runtime filter 8"), "{other}");
    }

    #[test]
    fn a_scan_source_consumer_without_its_dynamic_filter_is_refused() {
        let table = endpoints(&[(3, 7, SCAN, SCAN_SOURCE)]);
        let error = validate_scan_dynamic_filters(SCAN, &[], &[consumer(3)], &table)
            .expect_err("binding 3 applies at the source");
        assert!(error.contains("have no frozen dynamic filter"), "{error}");
    }

    #[test]
    fn two_consumer_bindings_of_one_dynamic_filter_are_refused() {
        let table = endpoints(&[(3, 7, SCAN, SCAN_SOURCE), (5, 7, SCAN, SCAN_SOURCE)]);
        let error =
            validate_scan_dynamic_filters(SCAN, &[dynamic(7)], &[consumer(3), consumer(5)], &table)
                .expect_err("one dynamic filter, two bindings");
        assert!(error.contains("[3, 5], not exactly one"), "{error}");
        let twice = validate_scan_dynamic_filters(
            SCAN,
            &[dynamic(7), dynamic(7)],
            &[consumer(3)],
            &endpoints(&[(3, 7, SCAN, SCAN_SOURCE)]),
        )
        .expect_err("a dynamic filter stated twice");
        assert!(twice.contains("stated twice"), "{twice}");
    }

    #[test]
    fn a_site_binding_that_is_not_a_consumer_of_this_scan_is_refused() {
        let table = endpoints(&[
            (3, 7, SCAN + 1, SCAN_SOURCE),
            (4, 7, SCAN, CompiledRuntimeFilterRole::Producer),
        ]);
        let elsewhere = validate_scan_dynamic_filters(SCAN, &[], &[consumer(3)], &table)
            .expect_err("binding 3 consumes at another node");
        assert!(elsewhere.contains("not at this scan"), "{elsewhere}");
        let producer = validate_scan_dynamic_filters(SCAN, &[], &[consumer(4)], &table)
            .expect_err("binding 4 is a producer");
        assert!(producer.contains("is a producer"), "{producer}");
        let unnumbered = validate_scan_dynamic_filters(SCAN, &[], &[consumer(6)], &table)
            .expect_err("binding 6 is unnumbered");
        assert!(unnumbered.contains("not numbered"), "{unnumbered}");
    }

    fn requirements(binding_ids: &[i32]) -> BindingRequirements {
        BindingRequirements::try_new(
            binding_ids
                .iter()
                .map(|binding_id| BindingRequirement::RuntimeFilter {
                    binding_id: *binding_id,
                })
                .collect(),
        )
        .expect("distinct requirements")
    }

    #[test]
    fn a_program_binds_exactly_the_bindings_its_package_numbers() {
        let table = endpoints(&[
            (3, 7, SCAN, SCAN_SOURCE),
            (4, 7, SCAN + 1, CompiledRuntimeFilterRole::Producer),
        ]);
        assert_eq!(
            program_runtime_filter_bindings(&requirements(&[3, 4]), &table),
            Ok(BTreeSet::from([3, 4]))
        );
        // The ordinary case: no site, nothing numbered, nothing to bind.
        assert_eq!(
            program_runtime_filter_bindings(
                &requirements(&[]),
                &CompiledRuntimeFilterEndpoints::default()
            ),
            Ok(BTreeSet::new())
        );

        let invented = program_runtime_filter_bindings(&requirements(&[3, 4, 5]), &table)
            .expect_err("binding 5 is not the package's");
        assert!(
            invented.contains("binding_id=5, which its package does not number"),
            "{invented}"
        );
        let dropped = program_runtime_filter_bindings(&requirements(&[3]), &table)
            .expect_err("binding 4 has no site");
        assert!(
            dropped.contains("binding_id=4 has no requirement"),
            "{dropped}"
        );
        let negative = program_runtime_filter_bindings(
            &requirements(&[-1]),
            &CompiledRuntimeFilterEndpoints::default(),
        )
        .expect_err("a negative binding identity");
        assert!(negative.contains("negative"), "{negative}");
    }

    #[test]
    fn a_binding_numbered_twice_is_refused() {
        let endpoint = CompiledRuntimeFilterEndpoint::new(7, SCAN, SCAN_SOURCE);
        let error =
            CompiledRuntimeFilterEndpoints::try_from_endpoints([(3, endpoint), (3, endpoint)])
                .expect_err("binding 3 twice");
        assert!(error.contains("numbered twice"), "{error}");
    }
}
