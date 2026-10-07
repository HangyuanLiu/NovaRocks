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

//! Bounded validation of flat, preorder-encoded type trees.
//!
//! Native type carriers encode a nested type as one node array instead of
//! recursive protobuf messages, so protobuf nesting stays constant whatever
//! the type depth. A decoder validates the array here before it builds any
//! Arrow or logical value:
//!
//! - node 0 is the root and the nodes appear exactly in canonical preorder:
//!   every subtree is contiguous and children are visited in declared order;
//! - every node is reachable from the root exactly once, so the array is a
//!   tree, never a DAG, a cycle or a forest with orphans;
//! - the tree stays within the caller's depth and node budgets.
//!
//! The walk is iterative and its stack is bounded by the node count, which
//! itself is checked against the budget before the walk starts.

use std::fmt;

/// Budgets one flat tree is validated against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatTreeLimits {
    /// Deepest admitted level; the root is level 1.
    pub max_depth: usize,
    /// Largest admitted node count.
    pub max_nodes: usize,
}

/// One child reference of a node, in declared order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatEdge {
    /// Index of the referenced node in the same array.
    pub child: u32,
    /// Levels this edge adds: 1 for an ordinary nesting level, 0 for a
    /// physical wrapper that is not a level of its own.
    pub depth: u8,
}

/// Why a flat tree is not admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlatTreeViolation {
    Empty,
    TooManyNodes {
        nodes: usize,
        limit: usize,
    },
    TooDeep {
        node: usize,
        depth: usize,
        limit: usize,
    },
    ChildOutOfRange {
        parent: usize,
        child: u32,
    },
    /// A node was reached out of canonical preorder. This covers a reused
    /// child, a back edge to an ancestor and any non-canonical ordering.
    NonCanonical {
        expected: usize,
        found: usize,
    },
    /// Nodes from `first` onwards are never reached from the root.
    Unreachable {
        first: usize,
    },
    /// The nodes declare more child references than a tree of this size has
    /// edges, so at least one node is referenced twice.
    TooManyEdges {
        nodes: usize,
    },
}

impl fmt::Display for FlatTreeViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("flat type tree has no root node"),
            Self::TooManyNodes { nodes, limit } => {
                write!(
                    f,
                    "flat type tree has {nodes} nodes, above the limit {limit}"
                )
            }
            Self::TooDeep { node, depth, limit } => write!(
                f,
                "flat type tree node {node} is at depth {depth}, above the limit {limit}"
            ),
            Self::ChildOutOfRange { parent, child } => write!(
                f,
                "flat type tree node {parent} references missing node {child}"
            ),
            Self::NonCanonical { expected, found } => write!(
                f,
                "flat type tree reaches node {found} where canonical preorder requires node {expected}"
            ),
            Self::Unreachable { first } => {
                write!(
                    f,
                    "flat type tree node {first} is not reachable from the root"
                )
            }
            Self::TooManyEdges { nodes } => write!(
                f,
                "flat type tree of {nodes} nodes declares more child references than a tree has edges"
            ),
        }
    }
}

/// Validate one flat tree of `len` nodes. `children` writes the declared
/// child edges of a node into the provided buffer, in declared order, and may
/// reject the node itself (unknown kind, wrong arity, invalid parameters).
pub fn validate_preorder<E>(
    len: usize,
    limits: FlatTreeLimits,
    mut children: impl FnMut(usize, &mut Vec<FlatEdge>) -> Result<(), E>,
) -> Result<(), E>
where
    E: From<FlatTreeViolation>,
{
    if len == 0 {
        return Err(FlatTreeViolation::Empty.into());
    }
    if len > limits.max_nodes {
        return Err(FlatTreeViolation::TooManyNodes {
            nodes: len,
            limit: limits.max_nodes,
        }
        .into());
    }
    let mut stack: Vec<(u32, usize)> = Vec::with_capacity(len.min(64));
    stack.push((0, 1));
    let mut next = 0_usize;
    // A tree of `len` nodes has exactly `len - 1` edges. Checking the running
    // total keeps the stack bounded even when a node lists a child many times.
    let mut remaining_edges = len - 1;
    let mut edges = Vec::new();
    while let Some((node, depth)) = stack.pop() {
        let node = node as usize;
        if node != next {
            return Err(FlatTreeViolation::NonCanonical {
                expected: next,
                found: node,
            }
            .into());
        }
        if depth > limits.max_depth {
            return Err(FlatTreeViolation::TooDeep {
                node,
                depth,
                limit: limits.max_depth,
            }
            .into());
        }
        next += 1;
        edges.clear();
        children(node, &mut edges)?;
        remaining_edges = remaining_edges
            .checked_sub(edges.len())
            .ok_or(FlatTreeViolation::TooManyEdges { nodes: len })?;
        for edge in edges.iter().rev() {
            if edge.child as usize >= len {
                return Err(FlatTreeViolation::ChildOutOfRange {
                    parent: node,
                    child: edge.child,
                }
                .into());
            }
            stack.push((edge.child, depth + usize::from(edge.depth)));
        }
    }
    if next != len {
        return Err(FlatTreeViolation::Unreachable { first: next }.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tree given as each node's child list, every edge one level deep
    /// unless listed in `wrappers`.
    fn check(
        children: &[&[u32]],
        wrappers: &[(usize, u32)],
        max_depth: usize,
        max_nodes: usize,
    ) -> Result<(), FlatTreeViolation> {
        validate_preorder(
            children.len(),
            FlatTreeLimits {
                max_depth,
                max_nodes,
            },
            |node, edges| {
                for child in children[node] {
                    let depth = u8::from(!wrappers.contains(&(node, *child)));
                    edges.push(FlatEdge {
                        child: *child,
                        depth,
                    });
                }
                Ok::<(), FlatTreeViolation>(())
            },
        )
    }

    fn chain(len: usize) -> Vec<Vec<u32>> {
        (0..len)
            .map(|index| {
                if index + 1 < len {
                    vec![index as u32 + 1]
                } else {
                    Vec::new()
                }
            })
            .collect()
    }

    fn refs(nodes: &[Vec<u32>]) -> Vec<&[u32]> {
        nodes.iter().map(Vec::as_slice).collect()
    }

    #[test]
    fn depth_and_node_budgets_are_inclusive() {
        let nodes = chain(64);
        assert_eq!(check(&refs(&nodes), &[], 64, 4096), Ok(()));
        let nodes = chain(65);
        assert!(matches!(
            check(&refs(&nodes), &[], 64, 4096),
            Err(FlatTreeViolation::TooDeep { depth: 65, .. })
        ));
        let flat = (0..4096)
            .map(|index| {
                if index == 0 {
                    (1..4096).collect()
                } else {
                    Vec::new()
                }
            })
            .collect::<Vec<Vec<u32>>>();
        assert_eq!(check(&refs(&flat), &[], 2, 4096), Ok(()));
        assert!(matches!(
            check(&refs(&flat), &[], 2, 4095),
            Err(FlatTreeViolation::TooManyNodes { nodes: 4096, .. })
        ));
    }

    #[test]
    fn wrapper_edges_add_no_level() {
        let nodes = chain(4);
        assert!(check(&refs(&nodes), &[], 3, 16).is_err());
        assert_eq!(check(&refs(&nodes), &[(1, 2)], 3, 16), Ok(()));
    }

    #[test]
    fn malformed_shapes_are_rejected() {
        assert_eq!(check(&[], &[], 8, 8), Err(FlatTreeViolation::Empty));
        assert!(matches!(
            check(&[&[1, 9], &[], &[]], &[], 8, 8),
            Err(FlatTreeViolation::ChildOutOfRange { child: 9, .. })
        ));
        assert!(matches!(
            check(&[&[1, 1], &[]], &[], 8, 8),
            Err(FlatTreeViolation::TooManyEdges { .. })
        ));
        assert!(matches!(
            check(&[&[2, 1], &[], &[]], &[], 8, 8),
            Err(FlatTreeViolation::NonCanonical {
                expected: 1,
                found: 2
            })
        ));
        assert!(matches!(
            check(&[&[1], &[0]], &[], 8, 8),
            Err(FlatTreeViolation::TooManyEdges { .. })
        ));
        assert!(matches!(
            check(&[&[1], &[], &[]], &[], 8, 8),
            Err(FlatTreeViolation::Unreachable { first: 2 })
        ));
        // A child listed many times is refused before the stack can grow.
        let many = vec![1_u32; 1_000_000];
        assert!(matches!(
            check(&[many.as_slice(), &[]], &[], 8, 8),
            Err(FlatTreeViolation::TooManyEdges { .. })
        ));
    }
}
