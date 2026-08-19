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
    pub column_names: Vec<Arc<str>>,
    pub comment: Option<Arc<str>>,
    pub properties: Vec<(Arc<str>, Arc<str>)>,
}

impl ConnectorViewMetadataValue {
    pub fn try_new(
        identity: ConnectorViewIdentity,
        definition: ConnectorViewDefinition,
        column_names: Vec<Arc<str>>,
        comment: Option<Arc<str>>,
        mut properties: Vec<(Arc<str>, Arc<str>)>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if definition.raw_sql.is_empty()
            || definition.default_namespace.is_empty()
            || definition
                .default_catalog
                .as_ref()
                .is_some_and(|catalog| catalog.is_empty())
            || column_names.iter().any(|name| name.is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector view metadata contains an empty source field or column name",
            ));
        }
        if definition.source_format.is_some() && definition.default_catalog.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "versioned connector view source is missing its default catalog",
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
            + definition.raw_sql.len()
            + definition
                .default_catalog
                .as_ref()
                .map_or(0, |value| value.len())
            + definition.default_namespace.len()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorProviderId, ConnectorViewDialect,
        ConnectorViewSourceFormat,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct ViewCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
    }

    impl ConnectorViewMetadata for ViewCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }

        fn view_exists(&self, _: ConnectorViewRequest) -> Result<bool, ConnectorError> {
            Ok(false)
        }

        fn load_view(
            &self,
            _: ConnectorViewRequest,
        ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "missing view",
            ))
        }

        fn list_views(
            &self,
            _: ConnectorListViewsRequest,
        ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
            Ok(Vec::new())
        }
    }

    fn descriptor() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            1024,
        )
        .expect("valid connector request context")
    }

    fn identity() -> ConnectorViewIdentity {
        ConnectorViewIdentity {
            instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
            namespace: Arc::from("db"),
            view: Arc::from("v"),
        }
    }

    fn definition() -> ConnectorViewDefinition {
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            raw_sql: Arc::from("SELECT 1"),
            default_catalog: Some(Arc::from("catalog")),
            default_namespace: Arc::from("db"),
            source_format: Some(ConnectorViewSourceFormat::EffectiveUserSourceV1),
        }
    }

    #[test]
    fn spi5b_view_metadata_canonicalizes_properties() {
        let value = ConnectorViewMetadataValue::try_new(
            identity(),
            definition(),
            vec![Arc::from("c")],
            Some(Arc::from("comment")),
            vec![
                (Arc::from("z"), Arc::from("2")),
                (Arc::from("a"), Arc::from("1")),
            ],
            &context(),
        )
        .expect("valid view metadata");

        assert_eq!(value.properties[0].0.as_ref(), "a");
        assert_eq!(value.properties[1].0.as_ref(), "z");
    }

    #[test]
    fn spi5b_view_metadata_rejects_duplicate_properties() {
        let error = ConnectorViewMetadataValue::try_new(
            identity(),
            definition(),
            Vec::new(),
            None,
            vec![
                (Arc::from("a"), Arc::from("1")),
                (Arc::from("a"), Arc::from("2")),
            ],
            &context(),
        )
        .expect_err("duplicate properties are corrupt provider data");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5b_view_metadata_rejects_versioned_source_without_catalog() {
        let mut definition = definition();
        definition.default_catalog = None;
        let error = ConnectorViewMetadataValue::try_new(
            identity(),
            definition,
            Vec::new(),
            None,
            Vec::new(),
            &context(),
        )
        .expect_err("versioned source requires a frozen catalog");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5b_view_metadata_rejects_a_different_generation_owner() {
        let descriptor = descriptor();
        let expected_incarnation = ConnectorInstanceIncarnation::new();
        let capability = ViewCapability {
            descriptor: descriptor.clone(),
            incarnation: ConnectorInstanceIncarnation::new(),
        };

        let error = validate_view_metadata_owner(&descriptor, expected_incarnation, &capability)
            .expect_err("view capability must belong to the exact control generation");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
