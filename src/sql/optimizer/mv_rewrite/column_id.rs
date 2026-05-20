//! Canonical column identity for MV rewrite.
//!
//! Both the query's Operator tree and the candidate MV's Operator tree
//! are walked with the SAME MvColumnIdFactory so identical Iceberg base
//! fields and identical derived expressions produce identical
//! MvColumnIds. This is the foundation for query↔MV column matching
//! without relying on string-based ColumnRef names (which break under
//! SubqueryAlias and Project rename).
//!
//! Equivalence union-find groups columns connected by join-eq or
//! filter-eq predicates so e.g. `t1.a = t2.b` makes the two columns
//! interchangeable for matching purposes.

use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct MvColumnId(pub(crate) u32);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum MvColumnIdKey {
    /// Scan column: (base table Iceberg UUID, base field-id).
    Base { table_uuid: String, field_id: i32 },
    /// Derived column produced by a scalar expression.
    /// `expr_hash` is a stable hash of the canonical scalar expression
    /// expressed in terms of already-assigned MvColumnIds (commutative
    /// operators are sorted; constant-folded where possible).
    Derived { expr_hash: u64 },
    /// Aggregate output column.
    AggOutput {
        fn_name: String,
        args: Vec<MvColumnId>,
        group_hash: u64,
    },
}

#[derive(Default)]
pub(crate) struct MvColumnIdFactory {
    next: u32,
    forward: HashMap<MvColumnIdKey, MvColumnId>,
    display: HashMap<MvColumnId, String>,
}

impl MvColumnIdFactory {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn intern(&mut self, key: MvColumnIdKey, display: String) -> MvColumnId {
        if let Some(id) = self.forward.get(&key) {
            return *id;
        }
        let id = MvColumnId(self.next);
        self.next += 1;
        self.forward.insert(key, id);
        self.display.insert(id, display);
        id
    }

    pub(crate) fn display(&self, id: MvColumnId) -> Option<&str> {
        self.display.get(&id).map(String::as_str)
    }
}

/// Union-find of MvColumnIds representing equivalence classes (built
/// from join-eq and filter-eq predicates).
#[derive(Clone, Debug, Default)]
pub(crate) struct MvEquivalence {
    parent: HashMap<MvColumnId, MvColumnId>,
}

impl MvEquivalence {
    /// Find the root representative of `id`, with iterative path compression.
    ///
    /// The recursive formulation from the spec would require two mutable
    /// borrows of `self` simultaneously (look up parent, then recurse),
    /// which Rust's borrow checker disallows. The iterative two-pass
    /// version below is functionally identical: first walk to the root,
    /// then compress all nodes on the path.
    pub(crate) fn find(&mut self, id: MvColumnId) -> MvColumnId {
        // Walk to root, collecting the path.
        let mut path = vec![id];
        let mut cur = id;
        loop {
            let p = *self.parent.entry(cur).or_insert(cur);
            if p == cur {
                break;
            }
            path.push(p);
            cur = p;
        }
        let root = cur;
        // Path compression: point every node directly at the root.
        for node in path {
            self.parent.insert(node, root);
        }
        root
    }

    pub(crate) fn union(&mut self, a: MvColumnId, b: MvColumnId) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }

    pub(crate) fn equivalent(&mut self, a: MvColumnId, b: MvColumnId) -> bool {
        self.find(a) == self.find(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_returns_same_id() {
        let mut f = MvColumnIdFactory::new();
        let k = MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 5 };
        let a = f.intern(k.clone(), "t1.a".into());
        let b = f.intern(k, "t1.a".into());
        assert_eq!(a, b);
    }

    #[test]
    fn different_keys_return_different_ids() {
        let mut f = MvColumnIdFactory::new();
        let id1 = f.intern(
            MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 1 },
            "t1.a".into(),
        );
        let id2 = f.intern(
            MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 2 },
            "t1.b".into(),
        );
        assert_ne!(id1, id2);
    }

    #[test]
    fn cross_factory_keys_match_when_seed_matches() {
        // Two separate factories (e.g. one for query, one for MV) assign
        // distinct local IDs, but matching uses the KEY not the ID.
        let mut q = MvColumnIdFactory::new();
        let mut mv = MvColumnIdFactory::new();
        let k = MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 7 };
        let qid = q.intern(k.clone(), "t.x".into());
        let mvid = mv.intern(k.clone(), "t.x".into());
        // IDs need not be equal across factories — matching is by key.
        // The factory's intern() returns a stable ID for the same key
        // within ONE factory; cross-factory comparison goes through
        // matching the KEY, which is the contract relied on by
        // ColumnRewriter (Task 5).
        let _ = (qid, mvid);
        assert_eq!(q.forward.get(&k).copied(), Some(qid));
        assert_eq!(mv.forward.get(&k).copied(), Some(mvid));
    }

    #[test]
    fn equivalence_find_returns_self_when_uninited() {
        let mut eq = MvEquivalence::default();
        let id = MvColumnId(0);
        assert_eq!(eq.find(id), id);
    }

    #[test]
    fn equivalence_union_makes_them_equivalent() {
        let mut eq = MvEquivalence::default();
        let a = MvColumnId(0);
        let b = MvColumnId(1);
        let c = MvColumnId(2);
        assert!(!eq.equivalent(a, b));
        eq.union(a, b);
        assert!(eq.equivalent(a, b));
        eq.union(b, c);
        assert!(eq.equivalent(a, c)); // transitivity
    }

    #[test]
    fn derived_keys_canonicalize() {
        // Same hash → same ID (caller is responsible for canonicalization
        // before hashing — we just verify the factory honours equality).
        let mut f = MvColumnIdFactory::new();
        let id1 = f.intern(MvColumnIdKey::Derived { expr_hash: 0xDEAD }, "x+y".into());
        let id2 = f.intern(MvColumnIdKey::Derived { expr_hash: 0xDEAD }, "y+x".into());
        assert_eq!(id1, id2);
    }
}
