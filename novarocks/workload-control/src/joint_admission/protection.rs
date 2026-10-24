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

//! Finite-step maximum-closure proofs for protected dependency intersections.
//!
//! Input is an owned hypothetical post-state, never admission authority. The
//! original WC authenticates old claims, freezes positive growth, builds the
//! bounded snapshot and commits only after checking its exact versions. All
//! graph work runs outside that authority's mutex. Allocation calls have finite
//! byte bounds; work units are not a hard real-time or allocator/RSS guarantee.

use super::CausalMask;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::task::Poll;

#[derive(Clone, Debug)]
pub(super) struct ExactProfile {
    // Strictly increasing opaque registry ordinals. Repeated exact profiles
    // are mathematically valid; the production ledger should aggregate them.
    pub(super) dependencies: Vec<usize>,
    pub(super) amounts: Vec<u128>,
    pub(super) causal: CausalMask,
}
impl ExactProfile {
    fn protected_amount(&self, coordinate: usize) -> u128 {
        if self.causal.finite(coordinate) {
            0
        } else {
            self.amounts[coordinate]
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct CoordinateLimit {
    pub(super) global: u128,
    // Per-dependency increment of the stallable source contribution.
    pub(super) protected_increment: u128,
    // One complete registered finite contribution, never a hard-cap refund.
    pub(super) finite_reserve: u128,
    pub(super) domains: Vec<u128>,
}

#[derive(Clone, Debug)]
pub(super) struct ProofInput {
    pub(super) profiles: Vec<ExactProfile>,
    pub(super) affected_dependencies: Vec<usize>,
    pub(super) affected_causal: CausalMask,
    // Exact coordinates whose constraints may increase; authenticated by WC's
    // original-to-post change analysis. Purely decreasing coordinates stay out.
    pub(super) checked_coordinates: Vec<usize>,
    pub(super) limits: Vec<CoordinateLimit>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SolverBounds {
    pub(super) max_domains: usize,
    pub(super) max_profiles: usize,
    pub(super) max_coordinates: usize,
    pub(super) max_profile_memberships: usize,
    pub(super) max_residual_arcs: usize,
    pub(super) max_snapshot_bytes: usize,
    pub(super) max_scratch_bytes: usize,
    // A resource ceiling across all polls, distinct from per-poll yielding.
    pub(super) max_total_work: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResourceKind {
    Domains,
    Profiles,
    Coordinates,
    ProfileMemberships,
    SnapshotBytes,
    ScratchBytes,
    ResidualArcs,
    TotalWork,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CheckError {
    MalformedSnapshot,
    ArithmeticOverflow,
    ResourceLimit(ResourceKind),
    ScratchAllocationFailed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConstraintKind {
    Global,
    Domain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Feasibility {
    Feasible,
    Infeasible {
        coordinate: usize,
        constraint: ConstraintKind,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StepReport {
    pub(super) used_work: usize,
    pub(super) result: Poll<Result<Feasibility, CheckError>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphPass {
    // CountGraph uses the same finite edge walk as construction.
    Count,
    Build,
}

// Each variant contains the exact continuation, never a partially issued grant.
#[derive(Clone, Copy, Debug)]
enum Phase {
    Header,
    Candidate {
        index: usize,
    },
    CheckedCoordinate {
        index: usize,
    },
    LimitHeader {
        coordinate: usize,
    },
    ProfileHeader {
        profile: usize,
    },
    ProfileDomain {
        profile: usize,
        index: usize,
    },
    ReserveEligibility,
    InitializeEligibility {
        profile: usize,
    },
    BeginCoordinate,
    HardGlobal {
        profile: usize,
        total: u128,
    },
    HardDomainTotal {
        profile: usize,
        total: u128,
    },
    HardDomainMembership {
        profile: usize,
        low: usize,
        high: usize,
        total: u128,
    },
    SumGlobal {
        profile: usize,
        total: u128,
    },
    BeginConstraint,
    DomainTotal {
        profile: usize,
        total: u128,
    },
    DomainMembership {
        profile: usize,
        low: usize,
        high: usize,
        total: u128,
    },
    FinishTotal {
        total: u128,
    },
    PrepareGraphReserve,
    ReserveGraph {
        array: usize,
    },
    InitializeGraphNode {
        node: usize,
    },
    GraphProfile {
        pass: GraphPass,
        profile: usize,
    },
    GraphMissing {
        pass: GraphPass,
        profile: usize,
        candidate: usize,
        profile_cursor: usize,
    },
    GraphDomain {
        pass: GraphPass,
        domain: usize,
    },
    ResetBfs {
        node: usize,
    },
    BfsPop,
    BfsEdges {
        node: usize,
        edge: Option<usize>,
    },
    InitializeCursors {
        node: usize,
    },
    Walk {
        node: usize,
    },
    Bottleneck {
        item: usize,
        amount: u128,
    },
    Augment {
        item: usize,
        amount: u128,
    },
    FinishAugment {
        amount: u128,
    },
    Poisoned,
}

#[derive(Clone, Copy, Debug)]
struct Edge {
    to: usize,
    next: Option<usize>,
    capacity: u128,
    // Reverse edge is index XOR 1 in the contiguous arena.
}

#[derive(Default)]
struct Graph {
    edges: Vec<Edge>,
    heads: Vec<Option<usize>>,
    levels: Vec<usize>,
    cursors: Vec<Option<usize>>,
    queue: Vec<usize>,
    queue_read: usize,
    path: Vec<usize>,
}

pub(super) struct ProtectionProof {
    input: ProofInput,
    bounds: SolverBounds,
    phase: Phase,
    done: Option<Result<Feasibility, CheckError>>,
    total_work: u64,
    domain_count: usize,
    memberships: usize,
    snapshot_bytes: usize,
    coordinate: usize,
    coordinate_cursor: usize,
    // None is global; Some is an index in affected_dependencies, not a key.
    required_domain: Option<usize>,
    cached_global_total: u128,
    allowance: u128,
    graph_total: u128,
    required_flow: u128,
    flow: u128,
    eligible: Vec<u8>,
    graph: Graph,
    node_count: usize,
    first_domain: usize,
    sink: usize,
    // Exact residual-arc count for this graph, not an N*K upper bound.
    edge_capacity: usize,
    graph_reserved: bool,
}

/// Actual graph arrays; the inline headers are already inside the driver.
/// One graph is reused across coordinates, never one graph per coordinate.
pub(super) fn graph_backing_bytes(
    profiles: usize,
    affected: usize,
    max_arcs: usize,
) -> Result<usize, crate::WorkError> {
    use crate::control_storage::{array_bytes, sum_bytes};
    let overflow = crate::WorkError::ArithmeticOverflow;
    let nodes = profiles
        .checked_add(affected)
        .and_then(|n| n.checked_add(2))
        .ok_or_else(|| overflow.clone())?;
    let arcs = profiles
        .checked_mul(affected.checked_add(1).ok_or_else(|| overflow.clone())?)
        .and_then(|n| n.checked_add(affected))
        .and_then(|n| n.checked_mul(2))
        .ok_or(overflow)?
        .min(max_arcs);
    sum_bytes([
        array_bytes::<u8>(profiles)?,
        array_bytes::<Edge>(arcs)?,
        array_bytes::<Option<usize>>(nodes)?
            .checked_mul(2)
            .ok_or(crate::WorkError::ArithmeticOverflow)?,
        array_bytes::<usize>(nodes)?
            .checked_mul(3)
            .ok_or(crate::WorkError::ArithmeticOverflow)?,
    ])
}

impl ProtectionProof {
    // Ownership transfer only. No input traversal, allocation, or proof runs here.
    pub(super) fn new(input: ProofInput, bounds: SolverBounds) -> Self {
        Self {
            input,
            bounds,
            phase: Phase::Header,
            done: None,
            total_work: 0,
            domain_count: 0,
            memberships: 0,
            snapshot_bytes: 0,
            coordinate: 0,
            coordinate_cursor: 0,
            required_domain: None,
            cached_global_total: 0,
            allowance: 0,
            graph_total: 0,
            required_flow: 0,
            flow: 0,
            eligible: Vec::new(),
            graph: Graph::default(),
            node_count: 0,
            first_domain: 0,
            sink: 0,
            edge_capacity: 0,
            graph_reserved: false,
        }
    }

    // The owner schedules a later poll after Pending. This synchronous kernel
    // does not self-wake or spin; each invocation executes at most budget steps.
    pub(super) fn poll_steps(&mut self, budget: NonZeroUsize) -> StepReport {
        let mut used_work = 0;
        while used_work < budget.get() && self.done.is_none() {
            if self.total_work >= self.bounds.max_total_work {
                self.done = Some(Err(CheckError::ResourceLimit(ResourceKind::TotalWork)));
                break;
            }
            self.total_work += 1; // Checked by the strict ceiling above.
            used_work += 1;
            if let Err(error) = self.step() {
                self.done = Some(Err(error));
            }
        }
        StepReport {
            used_work,
            result: self.done.map_or(Poll::Pending, Poll::Ready),
        }
    }

    pub(super) fn cancel(&mut self) {
        if self.done.is_none() {
            self.done = Some(Err(CheckError::Cancelled));
        }
    }

    /// Return the same owned shadow for the next exact-dependency batch group.
    /// This conveys no certificate or right to commit by itself.
    pub(super) fn into_input(self) -> ProofInput {
        self.input
    }

    pub(super) fn work_used(&self) -> u64 {
        self.total_work
    }

    fn fail_capacity(&mut self) {
        self.done = Some(Ok(Feasibility::Infeasible {
            coordinate: self.coordinate,
            constraint: if self.required_domain.is_some() {
                ConstraintKind::Domain
            } else {
                ConstraintKind::Global
            },
        }));
    }

    fn begin_protected_coordinate(&mut self) {
        self.required_domain = None;
        // A finite incoming coordinate changes no nonempty causal intersection.
        // Its complete charged-D hard caps have ALREADY been checked above.
        if self.input.affected_causal.finite(self.coordinate)
            || self.input.affected_dependencies.is_empty()
        {
            self.coordinate_cursor += 1;
            self.phase = Phase::BeginCoordinate;
        } else {
            self.phase = Phase::SumGlobal {
                profile: 0,
                total: 0,
            };
        }
    }

    fn next_constraint(&mut self) {
        let count = self.input.affected_dependencies.len();
        match self.required_domain {
            None if count != 0 => {
                self.required_domain = Some(0);
                self.phase = Phase::BeginConstraint;
            }
            Some(index) if index + 1 < count => {
                self.required_domain = Some(index + 1);
                self.phase = Phase::BeginConstraint;
            }
            _ => {
                self.coordinate_cursor += 1;
                self.required_domain = None;
                self.phase = Phase::BeginCoordinate;
            }
        }
    }

    fn add_snapshot_bytes(&mut self, amount: usize) -> Result<(), CheckError> {
        self.snapshot_bytes = checked_add(self.snapshot_bytes, amount)?;
        bounded(
            self.snapshot_bytes,
            self.bounds.max_snapshot_bytes,
            ResourceKind::SnapshotBytes,
        )
    }

    fn constraint_limit(&self) -> u128 {
        let limit = &self.input.limits[self.coordinate];
        match self.required_domain {
            None => limit.global,
            Some(index) => limit.domains[self.input.affected_dependencies[index]],
        }
    }

    fn protected_constraint_limit(&self) -> Result<u128, CheckError> {
        self.constraint_limit()
            .checked_sub(self.input.limits[self.coordinate].finite_reserve)
            .ok_or(CheckError::MalformedSnapshot)
    }

    fn add_edge(&mut self, from: usize, to: usize, capacity: u128) -> Result<(), CheckError> {
        let next_len = checked_add(self.graph.edges.len(), 2)?;
        bounded(
            next_len,
            self.bounds.max_residual_arcs,
            ResourceKind::ResidualArcs,
        )?;
        if next_len > self.edge_capacity {
            return Err(CheckError::MalformedSnapshot);
        }
        if next_len > self.graph.edges.capacity() {
            return Err(CheckError::ResourceLimit(ResourceKind::ScratchBytes));
        }
        let forward = self.graph.edges.len();
        self.graph.edges.push(Edge {
            to,
            next: self.graph.heads[from],
            capacity,
        });
        self.graph.edges.push(Edge {
            to: from,
            next: self.graph.heads[to],
            capacity: 0,
        });
        self.graph.heads[from] = Some(forward);
        self.graph.heads[to] = Some(forward + 1);
        Ok(())
    }

    fn check_actual_scratch(&self) -> Result<(), CheckError> {
        let mut bytes = checked_add(size_of::<Self>(), self.eligible.capacity())?;
        bytes = checked_add(
            bytes,
            checked_mul(self.graph.edges.capacity(), size_of::<Edge>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(self.graph.heads.capacity(), size_of::<Option<usize>>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(self.graph.levels.capacity(), size_of::<usize>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(self.graph.cursors.capacity(), size_of::<Option<usize>>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(self.graph.queue.capacity(), size_of::<usize>())?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(self.graph.path.capacity(), size_of::<usize>())?,
        )?;
        bounded(
            bytes,
            self.bounds.max_scratch_bytes,
            ResourceKind::ScratchBytes,
        )
    }

    fn graph_edge(
        &mut self,
        pass: GraphPass,
        from: usize,
        to: usize,
        capacity: u128,
    ) -> Result<(), CheckError> {
        match pass {
            GraphPass::Count => {
                let next = checked_add(self.edge_capacity, 2)?;
                bounded(
                    next,
                    self.bounds.max_residual_arcs,
                    ResourceKind::ResidualArcs,
                )?;
                self.edge_capacity = next;
                Ok(())
            }
            GraphPass::Build => self.add_edge(from, to, capacity),
        }
    }

    fn check_graph_reservation(&self) -> Result<(), CheckError> {
        // Existing node arrays are retained. A larger edge arena will replace
        // its empty predecessor before allocation, so the two never coexist.
        let mut bytes = checked_add(size_of::<Self>(), self.eligible.capacity())?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.graph.edges.capacity().max(self.edge_capacity),
                size_of::<Edge>(),
            )?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.graph.heads.capacity().max(self.node_count),
                size_of::<Option<usize>>(),
            )?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.graph.levels.capacity().max(self.node_count),
                size_of::<usize>(),
            )?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.graph.cursors.capacity().max(self.node_count),
                size_of::<Option<usize>>(),
            )?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.graph.queue.capacity().max(self.node_count),
                size_of::<usize>(),
            )?,
        )?;
        bytes = checked_add(
            bytes,
            checked_mul(
                self.graph.path.capacity().max(self.node_count),
                size_of::<usize>(),
            )?,
        )?;
        bounded(
            bytes,
            self.bounds.max_scratch_bytes,
            ResourceKind::ScratchBytes,
        )
    }

    fn start_graph(&mut self, total: u128) -> Result<(), CheckError> {
        self.graph_total = total;
        self.required_flow = total
            .checked_sub(self.allowance)
            .ok_or(CheckError::ArithmeticOverflow)?;
        self.flow = 0;
        // These are plain values without Drop callbacks. No live edge survives
        // into counting or a later arena replacement.
        self.graph.edges.clear();
        self.graph.path.clear();
        self.graph.queue.clear();
        self.graph.queue_read = 0;
        self.edge_capacity = 0;
        self.phase = Phase::GraphProfile {
            pass: GraphPass::Count,
            profile: 0,
        };
        Ok(())
    }

    fn step(&mut self) -> Result<(), CheckError> {
        let phase = std::mem::replace(&mut self.phase, Phase::Poisoned);
        match phase {
            Phase::Header => {
                if self.input.limits.is_empty() || !self.input.affected_causal.valid() {
                    return Err(CheckError::MalformedSnapshot);
                }
                // Actual Vec capacities count, not just lengths of live entries.
                bounded(
                    self.input.profiles.capacity(),
                    self.bounds.max_profiles,
                    ResourceKind::Profiles,
                )?;
                bounded(
                    self.input.limits.capacity(),
                    self.bounds.max_coordinates,
                    ResourceKind::Coordinates,
                )?;
                bounded(
                    self.input.checked_coordinates.capacity(),
                    self.bounds.max_coordinates,
                    ResourceKind::Coordinates,
                )?;
                self.domain_count = self.input.limits[0].domains.len();
                bounded(
                    self.domain_count,
                    self.bounds.max_domains,
                    ResourceKind::Domains,
                )?;
                bounded(
                    self.input.affected_dependencies.capacity(),
                    self.bounds.max_domains,
                    ResourceKind::Domains,
                )?;
                self.add_snapshot_bytes(size_of::<ProofInput>())?;
                self.add_snapshot_bytes(checked_mul(
                    self.input.profiles.capacity(),
                    size_of::<ExactProfile>(),
                )?)?;
                self.add_snapshot_bytes(checked_mul(
                    self.input.limits.capacity(),
                    size_of::<CoordinateLimit>(),
                )?)?;
                self.add_snapshot_bytes(checked_mul(
                    self.input.affected_dependencies.capacity(),
                    size_of::<usize>(),
                )?)?;
                self.add_snapshot_bytes(checked_mul(
                    self.input.checked_coordinates.capacity(),
                    size_of::<usize>(),
                )?)?;
                let profiles = self.input.profiles.len();
                let domains = self.input.affected_dependencies.len();
                self.first_domain = checked_add(profiles, 1)?;
                self.sink = checked_add(self.first_domain, domains)?;
                self.node_count = checked_add(self.sink, 1)?;
                // Only eligibility scratch is needed before the cached fast path.
                // Graph space is checked lazily at the first exact flow.
                bounded(
                    checked_add(size_of::<Self>(), profiles)?,
                    self.bounds.max_scratch_bytes,
                    ResourceKind::ScratchBytes,
                )?;
                self.phase = Phase::Candidate { index: 0 };
            }
            Phase::Candidate { index } => {
                if index == self.input.affected_dependencies.len() {
                    self.phase = Phase::CheckedCoordinate { index: 0 };
                } else {
                    let domain = self.input.affected_dependencies[index];
                    if domain >= self.domain_count
                        || (index != 0 && self.input.affected_dependencies[index - 1] >= domain)
                    {
                        return Err(CheckError::MalformedSnapshot);
                    }
                    self.phase = Phase::Candidate { index: index + 1 };
                }
            }
            Phase::CheckedCoordinate { index } => {
                if index == self.input.checked_coordinates.len() {
                    self.phase = Phase::LimitHeader { coordinate: 0 };
                } else {
                    let coordinate = self.input.checked_coordinates[index];
                    if coordinate >= self.input.limits.len()
                        || (index != 0 && self.input.checked_coordinates[index - 1] >= coordinate)
                    {
                        return Err(CheckError::MalformedSnapshot);
                    }
                    self.phase = Phase::CheckedCoordinate { index: index + 1 };
                }
            }
            Phase::LimitHeader { coordinate } => {
                if coordinate == self.input.limits.len() {
                    self.phase = Phase::ProfileHeader { profile: 0 };
                } else {
                    let domains = &self.input.limits[coordinate].domains;
                    if domains.len() != self.domain_count {
                        return Err(CheckError::MalformedSnapshot);
                    }
                    bounded(
                        domains.capacity(),
                        self.bounds.max_domains,
                        ResourceKind::Domains,
                    )?;
                    let bytes = checked_mul(domains.capacity(), size_of::<u128>())?;
                    self.add_snapshot_bytes(bytes)?;
                    self.phase = Phase::LimitHeader {
                        coordinate: coordinate + 1,
                    };
                }
            }
            Phase::ProfileHeader { profile } => {
                if profile == self.input.profiles.len() {
                    self.phase = Phase::ReserveEligibility;
                } else {
                    let row = &self.input.profiles[profile];
                    if row.amounts.len() != self.input.limits.len() || !row.causal.valid() {
                        return Err(CheckError::MalformedSnapshot);
                    }
                    bounded(
                        row.dependencies.capacity(),
                        self.bounds.max_domains,
                        ResourceKind::Domains,
                    )?;
                    bounded(
                        row.amounts.capacity(),
                        self.bounds.max_coordinates,
                        ResourceKind::Coordinates,
                    )?;
                    self.memberships = checked_add(self.memberships, row.dependencies.capacity())?;
                    bounded(
                        self.memberships,
                        self.bounds.max_profile_memberships,
                        ResourceKind::ProfileMemberships,
                    )?;
                    let bytes = checked_add(
                        checked_mul(row.dependencies.capacity(), size_of::<usize>())?,
                        checked_mul(row.amounts.capacity(), size_of::<u128>())?,
                    )?;
                    self.add_snapshot_bytes(bytes)?;
                    self.phase = Phase::ProfileDomain { profile, index: 0 };
                }
            }
            Phase::ProfileDomain { profile, index } => {
                let dependencies = &self.input.profiles[profile].dependencies;
                if index == dependencies.len() {
                    self.phase = Phase::ProfileHeader {
                        profile: profile + 1,
                    };
                } else {
                    if dependencies[index] >= self.domain_count
                        || (index != 0 && dependencies[index - 1] >= dependencies[index])
                    {
                        return Err(CheckError::MalformedSnapshot);
                    }
                    self.phase = Phase::ProfileDomain {
                        profile,
                        index: index + 1,
                    };
                }
            }
            Phase::ReserveEligibility => {
                reserve_exact(&mut self.eligible, self.input.profiles.len())?;
                self.check_actual_scratch()?;
                self.phase = Phase::InitializeEligibility { profile: 0 };
            }
            Phase::InitializeEligibility { profile } => {
                if profile == self.input.profiles.len() {
                    self.phase = Phase::BeginCoordinate;
                } else {
                    self.eligible.push(0);
                    self.phase = Phase::InitializeEligibility {
                        profile: profile + 1,
                    };
                }
            }
            Phase::BeginCoordinate => {
                if self.coordinate_cursor == self.input.checked_coordinates.len() {
                    self.done = Some(Ok(Feasibility::Feasible));
                } else {
                    self.coordinate = self.input.checked_coordinates[self.coordinate_cursor];
                    self.required_domain = None;
                    self.phase = Phase::HardGlobal {
                        profile: 0,
                        total: 0,
                    };
                }
            }
            Phase::HardGlobal { profile, total } => {
                if profile == self.input.profiles.len() {
                    if total > self.input.limits[self.coordinate].global {
                        self.fail_capacity();
                    } else if self.input.affected_dependencies.is_empty() {
                        self.begin_protected_coordinate();
                    } else {
                        self.required_domain = Some(0);
                        self.phase = Phase::HardDomainTotal {
                            profile: 0,
                            total: 0,
                        };
                    }
                } else {
                    let total = total
                        .checked_add(self.input.profiles[profile].amounts[self.coordinate])
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    self.phase = Phase::HardGlobal {
                        profile: profile + 1,
                        total,
                    };
                }
            }
            Phase::HardDomainTotal { profile, total } => {
                if profile == self.input.profiles.len() {
                    if total > self.constraint_limit() {
                        self.fail_capacity();
                    } else {
                        let domain = self.required_domain.ok_or(CheckError::MalformedSnapshot)?;
                        if domain + 1 < self.input.affected_dependencies.len() {
                            self.required_domain = Some(domain + 1);
                            self.phase = Phase::HardDomainTotal {
                                profile: 0,
                                total: 0,
                            };
                        } else {
                            self.begin_protected_coordinate();
                        }
                    }
                } else if self.input.profiles[profile].amounts[self.coordinate] == 0 {
                    self.phase = Phase::HardDomainTotal {
                        profile: profile + 1,
                        total,
                    };
                } else {
                    self.phase = Phase::HardDomainMembership {
                        profile,
                        low: 0,
                        high: self.input.profiles[profile].dependencies.len(),
                        total,
                    };
                }
            }
            Phase::HardDomainMembership {
                profile,
                low,
                high,
                total,
            } => {
                if low == high {
                    self.phase = Phase::HardDomainTotal {
                        profile: profile + 1,
                        total,
                    };
                } else {
                    let domain = self.required_domain.ok_or(CheckError::MalformedSnapshot)?;
                    let required = self.input.affected_dependencies[domain];
                    let middle = low + (high - low) / 2;
                    let observed = self.input.profiles[profile].dependencies[middle];
                    if observed < required {
                        self.phase = Phase::HardDomainMembership {
                            profile,
                            low: middle + 1,
                            high,
                            total,
                        };
                    } else if observed > required {
                        self.phase = Phase::HardDomainMembership {
                            profile,
                            low,
                            high: middle,
                            total,
                        };
                    } else {
                        let total = total
                            .checked_add(self.input.profiles[profile].amounts[self.coordinate])
                            .ok_or(CheckError::ArithmeticOverflow)?;
                        self.phase = Phase::HardDomainTotal {
                            profile: profile + 1,
                            total,
                        };
                    }
                }
            }
            Phase::SumGlobal { profile, total } => {
                if profile == self.input.profiles.len() {
                    self.cached_global_total = total;
                    self.required_domain = None;
                    self.phase = Phase::BeginConstraint;
                } else {
                    let total = total
                        .checked_add(self.input.profiles[profile].protected_amount(self.coordinate))
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    self.phase = Phase::SumGlobal {
                        profile: profile + 1,
                        total,
                    };
                }
            }
            Phase::BeginConstraint => {
                let limit = self.protected_constraint_limit()?;
                // Global total is cached from the immutable snapshot. It is an
                // upper bound for every domain's total; use it only to PASS.
                if self.required_domain.is_none() && self.cached_global_total > limit {
                    self.fail_capacity();
                } else {
                    let dimension = self
                        .input
                        .affected_dependencies
                        .len()
                        .checked_sub(usize::from(self.required_domain.is_some()))
                        .ok_or(CheckError::MalformedSnapshot)?;
                    let constant = (dimension as u128)
                        .checked_mul(self.input.limits[self.coordinate].protected_increment)
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    match limit.checked_sub(constant) {
                        None => self.fail_capacity(),
                        Some(allowance) => {
                            self.allowance = allowance;
                            if self.cached_global_total <= allowance {
                                self.next_constraint();
                            } else if self.required_domain.is_none() {
                                self.phase = Phase::FinishTotal {
                                    total: self.cached_global_total,
                                };
                            } else {
                                self.phase = Phase::DomainTotal {
                                    profile: 0,
                                    total: 0,
                                };
                            }
                        }
                    }
                }
            }
            Phase::DomainTotal { profile, total } => {
                if profile == self.input.profiles.len() {
                    self.phase = Phase::FinishTotal { total };
                } else {
                    self.eligible[profile] = 0;
                    if self.input.profiles[profile].protected_amount(self.coordinate) == 0 {
                        self.phase = Phase::DomainTotal {
                            profile: profile + 1,
                            total,
                        };
                    } else {
                        self.phase = Phase::DomainMembership {
                            profile,
                            low: 0,
                            high: self.input.profiles[profile].dependencies.len(),
                            total,
                        };
                    }
                }
            }
            Phase::DomainMembership {
                profile,
                low,
                high,
                total,
            } => {
                if low == high {
                    self.phase = Phase::DomainTotal {
                        profile: profile + 1,
                        total,
                    };
                } else {
                    let required_index =
                        self.required_domain.ok_or(CheckError::MalformedSnapshot)?;
                    let required = self.input.affected_dependencies[required_index];
                    let middle = low + (high - low) / 2;
                    let observed = self.input.profiles[profile].dependencies[middle];
                    if observed < required {
                        self.phase = Phase::DomainMembership {
                            profile,
                            low: middle + 1,
                            high,
                            total,
                        };
                    } else if observed > required {
                        self.phase = Phase::DomainMembership {
                            profile,
                            low,
                            high: middle,
                            total,
                        };
                    } else {
                        self.eligible[profile] = 1;
                        let total = total
                            .checked_add(
                                self.input.profiles[profile].protected_amount(self.coordinate),
                            )
                            .ok_or(CheckError::ArithmeticOverflow)?;
                        self.phase = Phase::DomainTotal {
                            profile: profile + 1,
                            total,
                        };
                    }
                }
            }
            Phase::FinishTotal { total } => {
                if total > self.protected_constraint_limit()? {
                    // Exact singleton cap (or the empty-set global cap).
                    self.fail_capacity();
                } else if total <= self.allowance {
                    self.next_constraint();
                } else {
                    self.start_graph(total)?;
                }
            }
            Phase::PrepareGraphReserve => {
                self.check_graph_reservation()?;
                if self.graph.edges.capacity() < self.edge_capacity {
                    // The old arena is empty. Destroy it before allocating a
                    // larger one, with no implicit realloc/copy or peak overlap.
                    self.graph.edges = Vec::new();
                    self.phase = Phase::ReserveGraph { array: 0 };
                } else if self.graph_reserved {
                    self.phase = Phase::InitializeGraphNode { node: 0 };
                } else {
                    self.phase = Phase::ReserveGraph { array: 1 };
                }
            }
            Phase::ReserveGraph { array } => {
                match array {
                    0 => reserve_exact(&mut self.graph.edges, self.edge_capacity)?,
                    1 => reserve_exact(&mut self.graph.heads, self.node_count)?,
                    2 => reserve_exact(&mut self.graph.levels, self.node_count)?,
                    3 => reserve_exact(&mut self.graph.cursors, self.node_count)?,
                    4 => reserve_exact(&mut self.graph.queue, self.node_count)?,
                    5 => reserve_exact(&mut self.graph.path, self.node_count)?,
                    _ => {
                        self.graph_reserved = true;
                        self.phase = Phase::InitializeGraphNode { node: 0 };
                        return Ok(());
                    }
                }
                self.check_actual_scratch()?;
                self.phase = if array == 0 && self.graph_reserved {
                    Phase::InitializeGraphNode { node: 0 }
                } else {
                    Phase::ReserveGraph { array: array + 1 }
                };
            }
            Phase::InitializeGraphNode { node } => {
                if node == self.node_count {
                    self.phase = Phase::GraphProfile {
                        pass: GraphPass::Build,
                        profile: 0,
                    };
                } else {
                    if node == self.graph.heads.len() {
                        self.graph.heads.push(None);
                        self.graph.levels.push(usize::MAX);
                        self.graph.cursors.push(None);
                    } else {
                        self.graph.heads[node] = None;
                        self.graph.levels[node] = usize::MAX;
                        self.graph.cursors[node] = None;
                    }
                    self.phase = Phase::InitializeGraphNode { node: node + 1 };
                }
            }
            Phase::GraphProfile { pass, profile } => {
                if profile == self.input.profiles.len() {
                    self.phase = Phase::GraphDomain { pass, domain: 0 };
                } else {
                    let amount = self.input.profiles[profile].protected_amount(self.coordinate);
                    if amount == 0
                        || (self.required_domain.is_some() && self.eligible[profile] == 0)
                    {
                        self.phase = Phase::GraphProfile {
                            pass,
                            profile: profile + 1,
                        };
                    } else {
                        self.graph_edge(pass, 0, profile + 1, amount)?;
                        self.phase = Phase::GraphMissing {
                            pass,
                            profile,
                            candidate: 0,
                            profile_cursor: 0,
                        };
                    }
                }
            }
            Phase::GraphMissing {
                pass,
                profile,
                candidate,
                profile_cursor,
            } => {
                if candidate == self.input.affected_dependencies.len() {
                    self.phase = Phase::GraphProfile {
                        pass,
                        profile: profile + 1,
                    };
                } else {
                    let domain = self.input.affected_dependencies[candidate];
                    let observed = self.input.profiles[profile]
                        .dependencies
                        .get(profile_cursor)
                        .copied();
                    if observed.is_some_and(|value| value < domain) {
                        self.phase = Phase::GraphMissing {
                            pass,
                            profile,
                            candidate,
                            profile_cursor: profile_cursor + 1,
                        };
                    } else {
                        if observed != Some(domain) {
                            // Count and build share this exact edge selection.
                            // Capacity W preserves the closure's numeric value.
                            self.graph_edge(
                                pass,
                                profile + 1,
                                self.first_domain + candidate,
                                self.graph_total,
                            )?;
                        }
                        self.phase = Phase::GraphMissing {
                            pass,
                            profile,
                            candidate: candidate + 1,
                            profile_cursor,
                        };
                    }
                }
            }
            Phase::GraphDomain { pass, domain } => {
                if domain == self.input.affected_dependencies.len() {
                    match pass {
                        GraphPass::Count => self.phase = Phase::PrepareGraphReserve,
                        GraphPass::Build => {
                            if self.graph.edges.len() != self.edge_capacity {
                                return Err(CheckError::MalformedSnapshot);
                            }
                            self.graph.queue.clear();
                            self.graph.queue_read = 0;
                            self.phase = Phase::ResetBfs { node: 0 };
                        }
                    }
                } else {
                    let increment = self.input.limits[self.coordinate].protected_increment;
                    if self.required_domain != Some(domain) && increment != 0 {
                        self.graph_edge(pass, self.first_domain + domain, self.sink, increment)?;
                    }
                    self.phase = Phase::GraphDomain {
                        pass,
                        domain: domain + 1,
                    };
                }
            }
            Phase::ResetBfs { node } => {
                if node == self.node_count {
                    self.graph.levels[0] = 0;
                    self.graph.queue.push(0);
                    self.phase = Phase::BfsPop;
                } else {
                    self.graph.levels[node] = usize::MAX;
                    self.phase = Phase::ResetBfs { node: node + 1 };
                }
            }
            Phase::BfsPop => {
                if self.graph.queue_read == self.graph.queue.len() {
                    if self.graph.levels[self.sink] == usize::MAX {
                        // Complete residual search, no augmenting path, and
                        // flow < required_flow: exact mathematical violation.
                        self.fail_capacity();
                    } else {
                        self.phase = Phase::InitializeCursors { node: 0 };
                    }
                } else {
                    let node = self.graph.queue[self.graph.queue_read];
                    self.graph.queue_read += 1;
                    self.phase = Phase::BfsEdges {
                        node,
                        edge: self.graph.heads[node],
                    };
                }
            }
            Phase::BfsEdges { node, edge } => {
                if let Some(index) = edge {
                    let edge = self.graph.edges[index];
                    if edge.capacity != 0 && self.graph.levels[edge.to] == usize::MAX {
                        self.graph.levels[edge.to] = self.graph.levels[node]
                            .checked_add(1)
                            .ok_or(CheckError::ArithmeticOverflow)?;
                        self.graph.queue.push(edge.to);
                    }
                    self.phase = Phase::BfsEdges {
                        node,
                        edge: edge.next,
                    };
                } else {
                    self.phase = Phase::BfsPop;
                }
            }
            Phase::InitializeCursors { node } => {
                if node == self.node_count {
                    self.graph.path.clear();
                    self.phase = Phase::Walk { node: 0 };
                } else {
                    self.graph.cursors[node] = self.graph.heads[node];
                    self.phase = Phase::InitializeCursors { node: node + 1 };
                }
            }
            Phase::Walk { node } => {
                if node == self.sink {
                    let amount = self
                        .required_flow
                        .checked_sub(self.flow)
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    self.phase = Phase::Bottleneck { item: 0, amount };
                } else if let Some(index) = self.graph.cursors[node] {
                    let edge = self.graph.edges[index];
                    let next_level = self.graph.levels[node]
                        .checked_add(1)
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    if edge.capacity != 0 && self.graph.levels[edge.to] == next_level {
                        self.graph.path.push(index);
                        self.phase = Phase::Walk { node: edge.to };
                    } else {
                        self.graph.cursors[node] = edge.next;
                        self.phase = Phase::Walk { node };
                    }
                } else {
                    self.graph.levels[node] = usize::MAX;
                    if let Some(incoming) = self.graph.path.pop() {
                        let parent = self.graph.edges[incoming ^ 1].to;
                        self.graph.cursors[parent] = self.graph.edges[incoming].next;
                        self.phase = Phase::Walk { node: parent };
                    } else {
                        self.graph.queue.clear();
                        self.graph.queue_read = 0;
                        self.phase = Phase::ResetBfs { node: 0 };
                    }
                }
            }
            Phase::Bottleneck { item, amount } => {
                if item == self.graph.path.len() {
                    self.phase = Phase::Augment { item: 0, amount };
                } else {
                    let amount = amount.min(self.graph.edges[self.graph.path[item]].capacity);
                    self.phase = Phase::Bottleneck {
                        item: item + 1,
                        amount,
                    };
                }
            }
            Phase::Augment { item, amount } => {
                if item == self.graph.path.len() {
                    self.phase = Phase::FinishAugment { amount };
                } else {
                    let edge = self.graph.path[item];
                    self.graph.edges[edge].capacity = self.graph.edges[edge]
                        .capacity
                        .checked_sub(amount)
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    self.graph.edges[edge ^ 1].capacity = self.graph.edges[edge ^ 1]
                        .capacity
                        .checked_add(amount)
                        .ok_or(CheckError::ArithmeticOverflow)?;
                    self.phase = Phase::Augment {
                        item: item + 1,
                        amount,
                    };
                }
            }
            Phase::FinishAugment { amount } => {
                self.flow = self
                    .flow
                    .checked_add(amount)
                    .ok_or(CheckError::ArithmeticOverflow)?;
                if self.flow >= self.required_flow {
                    self.next_constraint();
                } else {
                    self.graph.path.clear();
                    self.phase = Phase::Walk { node: 0 };
                }
            }
            Phase::Poisoned => return Err(CheckError::MalformedSnapshot),
        }
        Ok(())
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, CheckError> {
    left.checked_add(right)
        .ok_or(CheckError::ArithmeticOverflow)
}

fn checked_mul(left: usize, right: usize) -> Result<usize, CheckError> {
    left.checked_mul(right)
        .ok_or(CheckError::ArithmeticOverflow)
}

fn bounded(value: usize, limit: usize, kind: ResourceKind) -> Result<(), CheckError> {
    if value > limit {
        Err(CheckError::ResourceLimit(kind))
    } else {
        Ok(())
    }
}

fn reserve_exact<T>(storage: &mut Vec<T>, capacity: usize) -> Result<(), CheckError> {
    // Every allocation starts from an empty, unallocated vector. A larger
    // edge arena explicitly retires its empty predecessor first. Later pushes
    // cannot hide a realloc or copy of live elements inside a work step.
    if !storage.is_empty() || storage.capacity() != 0 {
        return Err(CheckError::MalformedSnapshot);
    }
    storage
        .try_reserve_exact(capacity)
        .map_err(|_| CheckError::ScratchAllocationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds() -> SolverBounds {
        SolverBounds {
            max_domains: 8,
            max_profiles: 32,
            max_coordinates: 4,
            max_profile_memberships: 256,
            max_residual_arcs: 2048,
            max_snapshot_bytes: 128 * 1024,
            max_scratch_bytes: 256 * 1024,
            max_total_work: 1_000_000,
        }
    }

    fn row(dependencies: &[usize], amounts: &[u128]) -> ExactProfile {
        ExactProfile {
            dependencies: dependencies.to_vec(),
            amounts: amounts.to_vec(),
            causal: CausalMask::NONE,
        }
    }

    fn limit(domains: usize, global: u128, local: u128, p: u128) -> CoordinateLimit {
        CoordinateLimit {
            global,
            protected_increment: p,
            finite_reserve: 0,
            domains: vec![local; domains],
        }
    }

    fn input(
        profiles: Vec<ExactProfile>,
        affected: &[usize],
        limits: Vec<CoordinateLimit>,
    ) -> ProofInput {
        ProofInput {
            profiles,
            affected_dependencies: affected.to_vec(),
            affected_causal: CausalMask::NONE,
            checked_coordinates: (0..limits.len()).collect(),
            limits,
        }
    }

    // Direct small subset formula: independent of flow, adjacency, eligibility,
    // continuation states, caches, and any original-change projection helper.
    fn oracle(input: &ProofInput, constraints: &[usize]) -> bool {
        assert!(constraints.len() <= 8);
        // Hard caps include ALL charged rows, independently of causal shape.
        for (coordinate, limit) in input.limits.iter().enumerate() {
            let hard: u128 = input.profiles.iter().map(|p| p.amounts[coordinate]).sum();
            if hard > limit.global {
                return false;
            }
            for domain in 0..limit.domains.len() {
                let hard: u128 = input
                    .profiles
                    .iter()
                    .filter(|p| p.dependencies.contains(&domain))
                    .map(|p| p.amounts[coordinate])
                    .sum();
                if hard > limit.domains[domain] {
                    return false;
                }
            }
        }
        for mask in 1usize..(1usize << constraints.len()) {
            let selected: Vec<_> = constraints
                .iter()
                .enumerate()
                .filter_map(|(bit, &domain)| (mask & (1usize << bit) != 0).then_some(domain))
                .collect();
            for (coordinate, limit) in input.limits.iter().enumerate() {
                let amount: u128 = input
                    .profiles
                    .iter()
                    .filter(|profile| {
                        (profile.causal.0
                            & (1u16 << (coordinate % crate::joint_admission::RESOURCE_COUNT)))
                            == 0
                            && selected
                                .iter()
                                .all(|domain| profile.dependencies.contains(domain))
                    })
                    .map(|profile| profile.amounts[coordinate])
                    .sum();
                if amount
                    + (selected.len() as u128) * limit.protected_increment
                    + limit.finite_reserve
                    > limit.global
                {
                    return false;
                }
                for &domain in &selected {
                    if amount
                        + ((selected.len() - 1) as u128) * limit.protected_increment
                        + limit.finite_reserve
                        > limit.domains[domain]
                    {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn run(
        input: ProofInput,
        bounds: SolverBounds,
        budget: usize,
    ) -> Result<Feasibility, CheckError> {
        let budget = NonZeroUsize::new(budget).unwrap();
        let mut proof = ProtectionProof::new(input, bounds);
        loop {
            let report = proof.poll_steps(budget);
            assert!(report.used_work <= budget.get());
            match report.result {
                Poll::Pending => assert_eq!(report.used_work, budget.get()),
                Poll::Ready(result) => {
                    assert_eq!(
                        proof.poll_steps(budget),
                        StepReport {
                            used_work: 0,
                            result: Poll::Ready(result)
                        }
                    );
                    return result;
                }
            }
        }
    }

    fn is_feasible(result: Result<Feasibility, CheckError>) -> bool {
        match result.expect("small oracle snapshot fits finite solver limits") {
            Feasibility::Feasible => true,
            Feasibility::Infeasible { .. } => false,
        }
    }

    #[test]
    fn exhaustive_certified_three_domain_ledgers_match_oracle_with_one_work_per_poll() {
        let universe = [0, 1, 2];
        let sets: Vec<Vec<usize>> = (0usize..8)
            .map(|mask| {
                (0usize..3)
                    .filter(|bit| mask & (1usize << *bit) != 0)
                    .collect()
            })
            .collect();
        let mut checked = 0;
        for ledger_mask in 0usize..256 {
            let profiles: Vec<_> = sets
                .iter()
                .enumerate()
                .filter(|(bit, _)| ledger_mask & (1usize << *bit) != 0)
                .map(|(_, set)| row(set, &[1]))
                .collect();
            let before = input(profiles, &universe, vec![limit(3, 4, 3, 1)]);
            if !oracle(&before, &universe) {
                continue;
            }
            for candidate in &sets {
                for amount in [1, 2] {
                    let mut post = before.clone();
                    post.affected_dependencies = candidate.clone();
                    post.profiles.push(row(candidate, &[amount]));
                    let expected = oracle(&post, &universe);
                    assert_eq!(
                        is_feasible(run(post, bounds(), 1)),
                        expected,
                        "ledger {ledger_mask}, candidate {candidate:?}, amount {amount}"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 100);
    }

    #[test]
    fn mixed_rows_keep_identical_hard_totals_but_only_stallable_high_order_membership() {
        for finite in [false, true] {
            let mut shadow = row(&[0, 1], &[1]);
            if finite {
                shadow.causal = CausalMask(1);
            }
            let post = input(
                vec![
                    row(&[0, 1], &[1]),
                    row(&[2], &[1]),
                    shadow,
                    row(&[0, 1], &[1]),
                ],
                &[0, 1],
                vec![limit(3, 4, 3, 1)],
            );
            // Both cases charge global=4 and d0=d1=3. The unrelated E row
            // prevents the protected-global fast path from proving the pair.
            let expected = oracle(&post, &[0, 1]);
            assert_eq!(expected, finite);
            for budget in [1, 7, 1000] {
                assert_eq!(is_feasible(run(post.clone(), bounds(), budget)), expected);
            }
        }
    }

    #[test]
    fn finite_incoming_keeps_every_charged_cap_without_inventing_causal_fanout() {
        let mut finite = row(&[0, 1, 2], &[1]);
        finite.causal = CausalMask(1);
        let mut post = input(
            vec![row(&[0], &[1]), finite],
            &[0, 1, 2],
            vec![limit(3, 3, 3, 1)],
        );
        post.affected_causal = CausalMask(1);
        // The sole supported nonempty E witness is d0. Incoming finite D is
        // still charged to all three domains, but creates no new causal set.
        assert!(oracle(&post, &[0]));
        assert!(is_feasible(run(post.clone(), bounds(), 1)));
        let mut hard_global = post.clone();
        hard_global.limits[0].global = 1;
        assert!(!oracle(&hard_global, &[0]));
        assert!(matches!(
            run(hard_global, bounds(), 1).unwrap(),
            Feasibility::Infeasible {
                constraint: ConstraintKind::Global,
                ..
            }
        ));
        let mut collateral = post;
        collateral.limits[0].domains[0] = 1;
        assert!(!oracle(&collateral, &[0]));
        assert!(matches!(
            run(collateral, bounds(), 1).unwrap(),
            Feasibility::Infeasible {
                constraint: ConstraintKind::Domain,
                ..
            }
        ));
    }

    #[test]
    fn finite_reserve_is_once_while_actual_finite_rows_keep_global_and_domain_caps() {
        let mut limits = limit(8, 24, 22, 2);
        limits.finite_reserve = 1;
        let native = row(&[0, 1, 2, 3, 4, 5, 6, 7], &[1]);
        let post = input(
            vec![native],
            &[0, 1, 2, 3, 4, 5, 6, 7],
            vec![limits.clone()],
        );
        assert!(oracle(&post, &post.affected_dependencies));
        assert!(is_feasible(run(post, bounds(), 1)));
        for (finite_amount, expected_constraint) in [
            (1, None),
            (2, Some(ConstraintKind::Domain)),
            (4, Some(ConstraintKind::Global)),
        ] {
            let mut finite = row(&[0], &[finite_amount]);
            finite.causal = CausalMask(1);
            let post = input(vec![row(&[0], &[21]), finite], &[0], vec![limits.clone()]);
            assert_eq!(oracle(&post, &[0]), expected_constraint.is_none());
            let result = run(post, bounds(), 1).unwrap();
            match expected_constraint {
                None => assert_eq!(result, Feasibility::Feasible),
                Some(expected) => assert_eq!(
                    result,
                    Feasibility::Infeasible {
                        coordinate: 0,
                        constraint: expected,
                    }
                ),
            }
        }
    }

    #[test]
    fn poll_partition_does_not_change_feasible_or_infeasible_results() {
        let cases = [
            input(
                vec![row(&[0], &[1]), row(&[1], &[1]), row(&[0, 1], &[1])],
                &[0, 1],
                vec![limit(2, 4, 3, 1)],
            ),
            input(
                vec![row(&[0, 1, 2], &[2])],
                &[0, 1, 2],
                vec![limit(3, 4, 3, 1)],
            ),
            input(
                vec![row(&[0, 1], &[1]), row(&[1, 2], &[1])],
                &[1, 2],
                vec![limit(3, 6, 2, 1)],
            ),
            input(
                vec![row(&[], &[2]), row(&[0, 2], &[1]), row(&[1, 2], &[1])],
                &[1, 2],
                vec![limit(3, 5, 4, 1)],
            ),
        ];
        for case in cases {
            let expected = oracle(&case, &case.affected_dependencies);
            for budget in [1, 2, 7, 64, 257] {
                assert_eq!(is_feasible(run(case.clone(), bounds(), budget)), expected);
            }
        }
    }

    #[test]
    fn cached_sufficient_pass_needs_no_graph_and_failed_shortcut_is_not_rejection() {
        let low = input(vec![row(&[0], &[1])], &[0], vec![limit(2, 6, 4, 1)]);
        let no_edges = SolverBounds {
            max_residual_arcs: 0,
            ..bounds()
        };
        assert_eq!(run(low, no_edges, 1), Ok(Feasibility::Feasible));
        let exact = input(
            vec![row(&[0], &[1]), row(&[1], &[1]), row(&[0, 1], &[1])],
            &[0, 1],
            vec![limit(2, 4, 3, 1)],
        );
        assert_eq!(run(exact, bounds(), 1), Ok(Feasibility::Feasible));
    }

    #[test]
    fn byte_coordinate_and_zero_protection_are_checked_independently() {
        let input = input(
            vec![row(&[0, 1], &[1, 8]), row(&[1, 2], &[1, 3])],
            &[1, 2],
            vec![limit(3, 6, 2, 1), limit(3, 20, 10, 0)],
        );
        assert_eq!(
            run(input, bounds(), 1),
            Ok(Feasibility::Infeasible {
                coordinate: 1,
                constraint: ConstraintKind::Domain,
            })
        );
    }

    #[test]
    fn empty_dependency_claims_still_contribute_to_global() {
        assert_eq!(
            run(
                input(vec![row(&[], &[3])], &[], vec![limit(0, 3, 0, 1)]),
                bounds(),
                1
            ),
            Ok(Feasibility::Feasible)
        );
        assert_eq!(
            run(
                input(vec![row(&[], &[4])], &[], vec![limit(0, 3, 0, 1)]),
                bounds(),
                1
            ),
            Ok(Feasibility::Infeasible {
                coordinate: 0,
                constraint: ConstraintKind::Global
            })
        );
    }

    #[test]
    fn resource_exhaustion_and_overflow_are_not_infeasible() {
        let exact = input(
            vec![row(&[0], &[1]), row(&[1], &[1]), row(&[0, 1], &[1])],
            &[0, 1],
            vec![limit(2, 4, 3, 1)],
        );
        assert_eq!(
            run(
                exact.clone(),
                SolverBounds {
                    max_residual_arcs: 0,
                    ..bounds()
                },
                1
            ),
            Err(CheckError::ResourceLimit(ResourceKind::ResidualArcs))
        );
        assert_eq!(
            run(
                exact.clone(),
                SolverBounds {
                    max_total_work: 1,
                    ..bounds()
                },
                1
            ),
            Err(CheckError::ResourceLimit(ResourceKind::TotalWork))
        );
        assert_eq!(
            run(
                exact,
                SolverBounds {
                    max_scratch_bytes: 1,
                    ..bounds()
                },
                1
            ),
            Err(CheckError::ResourceLimit(ResourceKind::ScratchBytes))
        );
        assert_eq!(
            run(
                input(
                    vec![row(&[], &[u128::MAX]), row(&[], &[1])],
                    &[],
                    vec![limit(0, u128::MAX, 0, 0)]
                ),
                bounds(),
                1
            ),
            Err(CheckError::ArithmeticOverflow)
        );
    }

    #[test]
    fn malformed_input_is_rejected_incrementally_without_a_capacity_verdict() {
        assert_eq!(
            run(
                input(vec![row(&[1, 0], &[1])], &[0], vec![limit(2, 4, 3, 1)]),
                bounds(),
                1
            ),
            Err(CheckError::MalformedSnapshot)
        );
        assert_eq!(
            run(
                input(vec![row(&[0], &[1])], &[0, 0], vec![limit(2, 4, 3, 1)]),
                bounds(),
                1
            ),
            Err(CheckError::MalformedSnapshot)
        );
    }

    #[test]
    fn cancellation_is_terminal_and_does_not_run_remaining_work() {
        let mut proof = ProtectionProof::new(
            input(vec![row(&[0], &[1])], &[0], vec![limit(2, 4, 3, 1)]),
            bounds(),
        );
        let budget = NonZeroUsize::new(1).unwrap();
        assert_eq!(proof.poll_steps(budget).result, Poll::Pending);
        proof.cancel();
        assert_eq!(
            proof.poll_steps(budget),
            StepReport {
                used_work: 0,
                result: Poll::Ready(Err(CheckError::Cancelled))
            }
        );
    }

    fn partial_augmentation_input() -> ProofInput {
        input(
            vec![row(&[0], &[1]), row(&[1], &[1]), row(&[0, 1], &[1])],
            &[0, 1],
            vec![limit(2, 4, 3, 1)],
        )
    }

    fn reach_partial_augmentation(proof: &mut ProtectionProof) {
        let budget = NonZeroUsize::new(1).unwrap();
        for _ in 0..10_000 {
            if matches!(proof.phase, Phase::Augment { item: 1, .. }) {
                assert!(proof.graph.path.len() > 1);
                assert_eq!(proof.flow, 0);
                assert!(proof.done.is_none());
                return;
            }
            assert_eq!(proof.poll_steps(budget).result, Poll::Pending);
        }
        panic!("fixture did not reach a partially applied augmentation");
    }

    #[test]
    fn cancellation_during_partial_augmentation_never_publishes_a_verdict() {
        let mut proof = ProtectionProof::new(partial_augmentation_input(), bounds());
        reach_partial_augmentation(&mut proof);
        proof.cancel();
        for budget in [1, 97] {
            assert_eq!(
                proof.poll_steps(NonZeroUsize::new(budget).unwrap()),
                StepReport {
                    used_work: 0,
                    result: Poll::Ready(Err(CheckError::Cancelled)),
                }
            );
        }
    }

    #[test]
    fn total_work_exhaustion_during_partial_augmentation_is_not_a_verdict() {
        // Discover this fixture's cut without depending on a brittle exact
        // transition count, then start a fresh proof with a fixed total ceiling.
        let mut probe = ProtectionProof::new(partial_augmentation_input(), bounds());
        reach_partial_augmentation(&mut probe);
        let total_limit = probe.total_work;
        let mut proof = ProtectionProof::new(
            partial_augmentation_input(),
            SolverBounds {
                max_total_work: total_limit,
                ..bounds()
            },
        );
        reach_partial_augmentation(&mut proof);
        assert_eq!(proof.total_work, total_limit);
        let expected = StepReport {
            used_work: 0,
            result: Poll::Ready(Err(CheckError::ResourceLimit(ResourceKind::TotalWork))),
        };
        assert_eq!(proof.poll_steps(NonZeroUsize::new(1).unwrap()), expected);
        assert_eq!(proof.poll_steps(NonZeroUsize::new(97).unwrap()), expected);
    }

    #[test]
    fn weighted_profiles_and_nonuniform_domain_caps_match_independent_oracle() {
        let universe = [0, 1, 2];
        let sets: Vec<Vec<usize>> = (0usize..8)
            .map(|mask| {
                (0usize..3)
                    .filter(|bit| mask & (1usize << *bit) != 0)
                    .collect()
            })
            .collect();
        let limits = [
            CoordinateLimit {
                global: 12,
                protected_increment: 2,
                finite_reserve: 0,
                domains: vec![5, 7, 9],
            },
            CoordinateLimit {
                global: 17,
                protected_increment: 3,
                finite_reserve: 0,
                domains: vec![8, 10, 12],
            },
        ];
        let mut feasible_count = 0;
        let mut infeasible_count = 0;
        for limit in limits {
            for ledger_mask in 0usize..256 {
                let profiles = sets
                    .iter()
                    .enumerate()
                    .filter(|(bit, _)| ledger_mask & (1usize << *bit) != 0)
                    .map(|(bit, set)| row(set, &[(bit % 3 + 1) as u128]))
                    .collect();
                let case = input(profiles, &universe, vec![limit.clone()]);
                let expected = oracle(&case, &universe);
                if expected {
                    feasible_count += 1;
                } else {
                    infeasible_count += 1;
                }
                for budget in [1, 11] {
                    assert_eq!(is_feasible(run(case.clone(), bounds(), budget)), expected);
                }
            }
        }
        assert!(feasible_count > 0);
        assert!(infeasible_count > 0);
    }

    #[test]
    fn sparse_actual_graph_fits_without_reserving_dense_profile_domain_product() {
        let universe: Vec<usize> = (0..8).collect();
        let profiles: Vec<_> = (0..16)
            .map(|index| {
                let dependencies: Vec<_> = universe
                    .iter()
                    .copied()
                    .filter(|domain| *domain != index % 8)
                    .collect();
                row(&dependencies, &[1])
            })
            .collect();
        let case = input(profiles, &universe, vec![limit(8, 24, 24, 2)]);
        assert!(oracle(&case, &universe));
        // The global graph has 16 source, 16 missing, and 8 sink edges.
        // Fixed-domain graphs are smaller. The obsolete dense upper bound
        // would request 304 residual arcs rather than these 80.
        let nodes = 16 + 8 + 2;
        let fixed_bytes = size_of::<ProtectionProof>()
            + 16
            + nodes * (2 * size_of::<Option<usize>>() + 3 * size_of::<usize>());
        let sparse_bytes = fixed_bytes + 80 * size_of::<Edge>() + 1024;
        assert!(sparse_bytes < fixed_bytes + 304 * size_of::<Edge>());
        assert_eq!(
            run(
                case,
                SolverBounds {
                    max_scratch_bytes: sparse_bytes,
                    ..bounds()
                },
                1,
            ),
            Ok(Feasibility::Feasible)
        );
    }

    #[test]
    fn later_coordinate_rebuilds_a_larger_actual_graph_without_live_edge_growth() {
        let case = input(
            vec![
                row(&[], &[3, 0]),
                row(&[0], &[0, 2]),
                row(&[1], &[0, 2]),
                row(&[2], &[0, 2]),
            ],
            &[0, 1, 2],
            vec![limit(3, 8, 8, 2), limit(3, 8, 8, 2)],
        );
        // Coordinate zero needs 14 residual arcs; coordinate one needs 24.
        // Both are mathematical passes, independently of any arena strategy.
        assert!(oracle(&case, &case.affected_dependencies));
        for budget in [1, 7, 64] {
            assert_eq!(
                run(case.clone(), bounds(), budget),
                Ok(Feasibility::Feasible)
            );
        }
    }
}
