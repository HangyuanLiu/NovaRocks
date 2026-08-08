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

use std::sync::Arc;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
    ConnectorNamespaceIdentity, ConnectorRequestContext, ConnectorViewDefinition,
    ConnectorViewIdentity,
};

#[derive(Clone)]
pub struct ConnectorViewRequest {
    pub view: ConnectorViewIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListViewsRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorViewMetadataValue {
    pub identity: ConnectorViewIdentity,
    pub definition: ConnectorViewDefinition,
    pub default_namespace: Arc<str>,
    pub column_names: Vec<Arc<str>>,
    pub comment: Option<Arc<str>>,
    pub properties: Vec<(Arc<str>, Arc<str>)>,
}

impl ConnectorViewMetadataValue {
    pub fn try_new(
        identity: ConnectorViewIdentity,
        definition: ConnectorViewDefinition,
        default_namespace: Arc<str>,
        column_names: Vec<Arc<str>>,
        comment: Option<Arc<str>>,
        mut properties: Vec<(Arc<str>, Arc<str>)>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if default_namespace.is_empty() || column_names.iter().any(|name| name.is_empty()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector view metadata contains an empty namespace or column name",
            ));
        }
        properties.sort_by(|left, right| left.0.cmp(&right.0));
        if properties
            .windows(2)
            .any(|pair| pair[0].0.as_ref() == pair[1].0.as_ref())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector view metadata contains duplicate property keys",
            ));
        }
        let bytes = identity.namespace.len()
            + identity.view.len()
            + definition.sql.len()
            + default_namespace.len()
            + column_names.iter().map(|name| name.len()).sum::<usize>()
            + comment.as_ref().map_or(0, |value| value.len())
            + properties
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>();
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector view metadata exceeds request total payload budget",
            ));
        }
        Ok(Self {
            identity,
            definition,
            default_namespace,
            column_names,
            comment,
            properties,
        })
    }
}

pub trait ConnectorViewMetadata: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;

    fn incarnation(&self) -> ConnectorInstanceIncarnation;

    fn view_exists(&self, request: ConnectorViewRequest) -> Result<bool, ConnectorError>;

    fn load_view(
        &self,
        request: ConnectorViewRequest,
    ) -> Result<ConnectorViewMetadataValue, ConnectorError>;

    fn list_views(
        &self,
        request: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError>;
}

pub(crate) fn validate_view_metadata_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    capability: &dyn ConnectorViewMetadata,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector view metadata capability owner does not match its control binding generation",
        ));
    }
    Ok(())
}
