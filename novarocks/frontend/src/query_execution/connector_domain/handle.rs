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

//! Catalog-scoped engine wrappers around validated connector carriers.

use std::sync::Arc;

use novarocks_spi::connector::read_stack::{
    ConnectorReadRelation, ConnectorReadRelationKind, ConnectorReadSplit,
};

/// The catalog a relation or split belongs to.
///
/// Identity is the exact connector instance plus its incarnation: a handle
/// frozen against one control generation can never be replayed into another.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CatalogHandle {
    instance_id: Arc<str>,
    incarnation: [u8; 16],
}

impl CatalogHandle {
    pub(crate) fn new(instance_id: impl AsRef<str>, incarnation: [u8; 16]) -> Self {
        Self {
            instance_id: Arc::from(instance_id.as_ref()),
            incarnation,
        }
    }

    pub(crate) fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub(crate) const fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }
}

/// One relation a scan reads: catalog, connector transaction, and the concrete
/// connector handle, already validated by the protocol layer.
#[derive(Clone, Debug)]
pub(crate) struct TableHandle {
    catalog: CatalogHandle,
    relation: ConnectorReadRelation,
}

impl TableHandle {
    pub(crate) fn new(catalog: CatalogHandle, relation: ConnectorReadRelation) -> Self {
        Self { catalog, relation }
    }

    pub(crate) const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub(crate) const fn relation(&self) -> &ConnectorReadRelation {
        &self.relation
    }

    /// Which relation family this handle names. The engine branches on this to
    /// choose a distribution; it never reads inside the provider variant.
    pub(crate) fn relation_kind(&self) -> ConnectorReadRelationKind {
        self.relation.kind()
    }
}

/// One unit of schedulable read work, bound to the catalog that produced it.
#[derive(Clone, Debug)]
/// One split as the coordinator sees it, for placement only.
///
/// It carries no catalog: which connector generation a split belongs to is
/// decided by the scan node it was enumerated for, and the backend resolves the
/// provider from that node's own frozen table handle. Repeating it here would
/// create a second authority that could disagree with the first.
pub(crate) struct Split {
    split: ConnectorReadSplit,
}

impl Split {
    pub(crate) fn new(split: ConnectorReadSplit) -> Self {
        Self { split }
    }

    pub(crate) const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }

    /// Relative scheduling cost. Weight only influences placement; it never
    /// changes what the split reads.
    pub(crate) fn weight_raw(&self) -> u64 {
        self.split.facts().split_weight().raw_value()
    }

    pub(crate) fn retained_size_in_bytes(&self) -> u64 {
        self.split.facts().retained_size_in_bytes()
    }

    pub(crate) fn is_remotely_accessible(&self) -> bool {
        self.split.facts().remotely_accessible()
    }

    pub(crate) fn affinity_key(&self) -> Option<&str> {
        self.split.facts().affinity_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_catalog_handle_is_identified_by_instance_and_incarnation() {
        let first = CatalogHandle::new("ice", [1; 16]);
        let same = CatalogHandle::new("ice", [1; 16]);
        let replaced = CatalogHandle::new("ice", [2; 16]);
        assert_eq!(first, same);
        assert_ne!(first, replaced);
    }
}
