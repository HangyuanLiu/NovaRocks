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

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use novarocks_frontend::deployment::{
    FeDeploymentViewSource, FeDeploymentViewSourceErrorKind, SqliteSingleFeDeploymentViewSource,
};
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_state_store::{StateStoreConfig, StateStoreLimitOverrides, StateStoreProviderConfig};

const EXPECTED_REVISION_HEX: &str =
    "e9864ac5f4d17bc604c7273b2246e98ddd2c66bfa39fc0d7bb1337087f66e387";

fn sqlite_config(cluster_id: &str, deployment_owner: &str) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: cluster_id.to_owned(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Sqlite {
            path: "/tmp/novarocks-state-store.sqlite".into(),
            deployment_owner: deployment_owner.to_owned(),
        },
    }
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("SQLite deployment source snapshot must be immediately ready"),
    }
}

fn hexadecimal(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[usize::from(byte >> 4)] as char);
        result.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    result
}

fn sqlite_source(cluster_id: &str, deployment_owner: &str) -> SqliteSingleFeDeploymentViewSource {
    SqliteSingleFeDeploymentViewSource::try_from_state_store_config(&sqlite_config(
        cluster_id,
        deployment_owner,
    ))
    .expect("valid SQLite configuration must create a deployment source")
}

#[test]
fn trait_object_source_is_callable() {
    let source: Arc<dyn FeDeploymentViewSource> = Arc::new(sqlite_source("cluster-a", "fe-a"));

    let snapshot: FeDeploymentView =
        ready(source.snapshot()).expect("trait object snapshot must succeed");

    assert_eq!(snapshot.active_fe_count, NonZeroUsize::new(1).unwrap());
}

#[test]
fn sqlite_source_returns_exact_single_fe_snapshot() {
    let source = sqlite_source("cluster-a", "fe-a");

    let snapshot = ready(source.snapshot()).expect("SQLite source snapshot must succeed");

    assert_eq!(snapshot.active_fe_count, NonZeroUsize::new(1).unwrap());
    assert_eq!(
        hexadecimal(snapshot.topology_revision.as_ref()),
        EXPECTED_REVISION_HEX
    );
}

#[test]
fn sqlite_revision_is_stable() {
    let source = sqlite_source("cluster-a", "fe-a");

    let first = ready(source.snapshot()).expect("first snapshot must succeed");
    let second = ready(source.snapshot()).expect("second snapshot must succeed");

    assert_eq!(first.topology_revision, second.topology_revision);
}

#[test]
fn sqlite_revision_changes_with_cluster_or_owner() {
    let baseline = ready(sqlite_source("cluster-a", "fe-a").snapshot())
        .expect("baseline snapshot must succeed");
    let other_cluster = ready(sqlite_source("cluster-b", "fe-a").snapshot())
        .expect("other-cluster snapshot must succeed");
    let other_owner = ready(sqlite_source("cluster-a", "fe-b").snapshot())
        .expect("other-owner snapshot must succeed");

    assert_ne!(baseline.topology_revision, other_cluster.topology_revision);
    assert_ne!(baseline.topology_revision, other_owner.topology_revision);
}

#[test]
fn rejects_invalid_sqlite_config() {
    let error = match SqliteSingleFeDeploymentViewSource::try_from_state_store_config(
        &sqlite_config("cluster-a", " "),
    ) {
        Ok(_) => panic!("invalid SQLite configuration must be rejected"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        FeDeploymentViewSourceErrorKind::InvalidConfiguration
    );
}

#[test]
fn rejects_deferred_provider_without_io() {
    let config = StateStoreConfig {
        cluster_id: "cluster-a".to_owned(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Mysql {
            database: "novarocks_control_plane".to_owned(),
        },
    };

    let error = match SqliteSingleFeDeploymentViewSource::try_from_state_store_config(&config) {
        Ok(_) => panic!("deferred provider must be rejected before any I/O"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        FeDeploymentViewSourceErrorKind::UnsupportedProvider
    );
}
