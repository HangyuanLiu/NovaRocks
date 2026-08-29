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

//! Provider-owned exact-generation write activation reservations.
//!
//! The reservation table is scoped to one control generation.  It records no
//! catalog client or Core service: callers use the generated activation proof
//! to construct their provider-private service and release it only after a
//! known terminal outcome.

use std::collections::HashMap;
use std::sync::Mutex;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorWriteActivation,
    ConnectorWriteActivationRequest, ConnectorWriteActivationSource, ConnectorWriteCohortId,
    ConnectorWriteOperationId, MAX_CONNECTOR_WRITE_ACTIVATIONS,
};

#[derive(Default)]
pub struct IcebergWriteActivationReservations {
    reservations: Mutex<HashMap<ConnectorWriteOperationId, [u8; 32]>>,
}

impl IcebergWriteActivationReservations {
    /// Reserve a bounded, exact-generation activation. Replays with the same
    /// request digest are idempotent; a conflicting request for the same
    /// operation cannot replace the original reservation.
    pub fn activate(
        &self,
        owner: &ConnectorProviderBindingKey,
        request: &ConnectorWriteActivationRequest,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        request.validate(owner)?;
        let operation_id = request.operation_id;
        let cohorts = match &request.source {
            ConnectorWriteActivationSource::Prepared(preparation) => vec![(
                ConnectorWriteCohortId::primary(operation_id),
                preparation.clone(),
            )],
            ConnectorWriteActivationSource::RowMutation(plan) => plan
                .routes()
                .iter()
                .map(|route| (route.cohort_id(), route.preparation().clone()))
                .collect::<Vec<(ConnectorWriteCohortId, _)>>(),
        };
        self.activate_cohorts(owner, request, cohorts)
    }

    /// Reserve an operation whose provider-owned planner froze more than one
    /// cohort from one signed preparation source.  Distributed rewrite is the
    /// only caller: generic SPI still exposes only `Prepared` and
    /// `RowMutation` activation sources, while the provider binds the complete
    /// cohort set into the resulting activation digest.
    pub(crate) fn activate_cohorts(
        &self,
        owner: &ConnectorProviderBindingKey,
        request: &ConnectorWriteActivationRequest,
        cohorts: Vec<(
            ConnectorWriteCohortId,
            novarocks_spi::connector::ConnectorWritePreparation,
        )>,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        request.validate(owner)?;
        let operation_id = request.operation_id;
        let activation = ConnectorWriteActivation::try_new(owner.clone(), request, cohorts)?;
        let mut reservations = self.reservations.lock().map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("Iceberg write activation reservation lock: {error}"),
            )
        })?;
        match reservations.get(&operation_id) {
            Some(existing) if existing == &activation.digest() => return Ok(activation),
            Some(_) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg write activation conflicts with an existing operation reservation",
                ));
            }
            None if reservations.len() >= MAX_CONNECTOR_WRITE_ACTIVATIONS => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg write activation reservation limit is exhausted",
                ));
            }
            None => {}
        }
        reservations.insert(operation_id, activation.digest());
        Ok(activation)
    }

    /// Release only a known terminal operation. CommitUnknown deliberately
    /// remains reserved until reconciliation resolves its external outcome.
    pub fn release(&self, operation_id: ConnectorWriteOperationId) -> Result<(), ConnectorError> {
        self.reservations
            .lock()
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("Iceberg write activation reservation lock: {error}"),
                )
            })?
            .remove(&operation_id);
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.reservations
            .lock()
            .expect("activation reservation lock")
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorRequestContext, ConnectorTableHandle,
        ConnectorWriteActivationIntent, ConnectorWriteBaseVersion, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteIntent,
        ConnectorWritePreparation, ConnectorWriteTargetRef, ProviderBindingEpoch,
    };

    use super::*;

    #[derive(Default)]
    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn key() -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ProviderBindingEpoch::new(),
        }
    }

    fn request(
        owner: &ConnectorProviderBindingKey,
        operation_id: ConnectorWriteOperationId,
        marker: &[u8],
    ) -> ConnectorWriteActivationRequest {
        ConnectorWriteActivationRequest {
            operation_id,
            source: ConnectorWriteActivationSource::Prepared(
                ConnectorWritePreparation::try_new(
                    owner.clone(),
                    ConnectorTableHandle::try_new(
                        owner.instance_id.clone(),
                        Bytes::from_static(b"table"),
                    )
                    .expect("table"),
                    ConnectorWriteTargetRef::main(),
                    ConnectorWriteIntent::Append,
                    ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base"))
                        .expect("base version"),
                    ConnectorWriteInputShape::Data {
                        fields: vec![ConnectorWriteFieldBinding::new(
                            ConnectorWriteFieldToken::from_bytes([1; 32]),
                            Field::new("value", DataType::Int64, true),
                        )],
                    },
                    Bytes::copy_from_slice(marker),
                )
                .expect("preparation"),
            ),
            intent: ConnectorWriteActivationIntent::Ordinary,
            context: ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(5),
                Arc::new(NeverCancelled),
                1024,
                4096,
            )
            .expect("context"),
        }
    }

    #[test]
    fn exact_generation_reservation_is_idempotent_conflict_safe_and_releasable() {
        let owner = key();
        let operation = ConnectorWriteOperationId::new();
        let reservations = IcebergWriteActivationReservations::default();
        let first = reservations
            .activate(&owner, &request(&owner, operation, b"first"))
            .expect("first activation");
        let replay = reservations
            .activate(&owner, &request(&owner, operation, b"first"))
            .expect("idempotent replay");
        assert_eq!(first.digest(), replay.digest());
        assert_eq!(reservations.len(), 1);
        let conflict = match reservations.activate(&owner, &request(&owner, operation, b"conflict"))
        {
            Ok(_) => panic!("conflicting activation must fail"),
            Err(error) => error,
        };
        assert_eq!(conflict.kind(), ConnectorErrorKind::InvalidRequest);

        reservations
            .release(operation)
            .expect("release known terminal");
        assert_eq!(reservations.len(), 0);
    }

    #[test]
    fn exact_generation_reservations_are_bounded() {
        let owner = key();
        let reservations = IcebergWriteActivationReservations::default();
        for _ in 0..MAX_CONNECTOR_WRITE_ACTIVATIONS {
            let operation = ConnectorWriteOperationId::new();
            reservations
                .activate(&owner, &request(&owner, operation, b"bounded"))
                .expect("activation within the provider bound");
        }
        let error = match reservations.activate(
            &owner,
            &request(&owner, ConnectorWriteOperationId::new(), b"overflow"),
        ) {
            Ok(_) => panic!("activation beyond the provider bound must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn provider_frozen_multi_cohort_activation_is_bounded_and_idempotent() {
        let owner = key();
        let operation = ConnectorWriteOperationId::new();
        let request = request(&owner, operation, b"rewrite");
        let preparation = match &request.source {
            ConnectorWriteActivationSource::Prepared(preparation) => preparation.clone(),
            ConnectorWriteActivationSource::RowMutation(_) => unreachable!(),
        };
        let first_id = ConnectorWriteCohortId::derive(operation, b"rewrite-test", [1; 32])
            .expect("first cohort");
        let second_id = ConnectorWriteCohortId::derive(operation, b"rewrite-test", [2; 32])
            .expect("second cohort");
        let cohorts = vec![
            (first_id, preparation.clone()),
            (second_id, preparation.clone()),
        ];
        let reservations = IcebergWriteActivationReservations::default();
        let first = reservations
            .activate_cohorts(&owner, &request, cohorts.clone())
            .expect("multi-cohort activation");
        let replay = reservations
            .activate_cohorts(&owner, &request, cohorts)
            .expect("multi-cohort replay");
        assert_eq!(first.digest(), replay.digest());
        assert_eq!(first.cohorts().len(), 2);

        let conflict =
            match reservations.activate_cohorts(&owner, &request, vec![(first_id, preparation)]) {
                Ok(_) => panic!("different cohort set must conflict"),
                Err(error) => error,
            };
        assert_eq!(conflict.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
