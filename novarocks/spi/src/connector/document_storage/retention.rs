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

use std::collections::HashSet;

use super::document::{exhausted, invalid};
use super::observation::validate_table;
use super::{ConnectorDocumentId, MAX_CONNECTOR_DOCUMENTS};
use crate::connector::{ConnectorTableIdentity, ConnectorTableObjectId};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorDocumentRetentionRoot {
    document: ConnectorDocumentId,
}

impl ConnectorDocumentRetentionRoot {
    pub const fn new(document: ConnectorDocumentId) -> Self {
        Self { document }
    }

    pub const fn document(&self) -> &ConnectorDocumentId {
        &self.document
    }
}

/// Roots supplied to the provider's envelope-level reachability walk. The
/// provider follows only public document references and physical attachments;
/// it never parses application payloads to discover hidden edges.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentRetentionGraph {
    roots: Vec<ConnectorDocumentRetentionRoot>,
}

impl ConnectorDocumentRetentionGraph {
    pub fn try_new(
        roots: Vec<ConnectorDocumentRetentionRoot>,
    ) -> Result<Self, crate::connector::ConnectorError> {
        if roots.len() > MAX_CONNECTOR_DOCUMENTS {
            return Err(exhausted("document retention roots exceed the item limit"));
        }
        let mut seen = HashSet::with_capacity(roots.len());
        if roots.iter().any(|root| !seen.insert(root.document())) {
            return Err(invalid("document retention graph contains duplicate roots"));
        }
        Ok(Self { roots })
    }

    pub fn roots(&self) -> &[ConnectorDocumentRetentionRoot] {
        &self.roots
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentRetentionConstraint {
    target: ConnectorTableIdentity,
    expected_object_id: ConnectorTableObjectId,
    graph: ConnectorDocumentRetentionGraph,
}

impl ConnectorDocumentRetentionConstraint {
    pub fn try_new(
        target: ConnectorTableIdentity,
        expected_object_id: ConnectorTableObjectId,
        graph: ConnectorDocumentRetentionGraph,
    ) -> Result<Self, crate::connector::ConnectorError> {
        validate_table(&target)?;
        Ok(Self {
            target,
            expected_object_id,
            graph,
        })
    }

    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }

    pub const fn expected_object_id(&self) -> &ConnectorTableObjectId {
        &self.expected_object_id
    }

    pub const fn graph(&self) -> &ConnectorDocumentRetentionGraph {
        &self.graph
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{
        ConnectorDocumentName, ConnectorDocumentOwner, ConnectorDocumentRevision,
    };

    #[test]
    fn retention_graph_rejects_duplicate_roots() {
        let id = ConnectorDocumentId::new(
            ConnectorDocumentOwner::parse("novarocks.mv").unwrap(),
            ConnectorDocumentName::parse("definition").unwrap(),
            ConnectorDocumentRevision::for_content(b"definition"),
        );
        let error = ConnectorDocumentRetentionGraph::try_new(vec![
            ConnectorDocumentRetentionRoot::new(id.clone()),
            ConnectorDocumentRetentionRoot::new(id),
        ])
        .unwrap_err();
        assert_eq!(
            error.kind(),
            crate::connector::ConnectorErrorKind::InvalidRequest
        );
    }
}
