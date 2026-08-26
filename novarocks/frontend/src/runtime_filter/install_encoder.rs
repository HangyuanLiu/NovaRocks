//! Frontend-owned runtime-filter lifecycle contribution encoder.

use std::collections::BTreeMap;

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks_proto_models::{filter, novarocks as service};

use super::model::{FrontendRuntimeFilterDeployment, FrontendRuntimeFilterParticipant};

fn encoding_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// A Frontend-owned, deterministically ordered contribution table.  The caller
/// seals it with the Core schedule view; this type cannot attach itself to an
/// arbitrary query artifact.
pub(crate) struct EncodedRuntimeFilterDeployment {
    contributions: BTreeMap<usize, service::RuntimeFilterContribution>,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl EncodedRuntimeFilterDeployment {
    pub(crate) fn contributions(
        &self,
    ) -> impl ExactSizeIterator<Item = (usize, service::RuntimeFilterContribution)> + '_ {
        self.contributions
            .iter()
            .map(|(backend_idx, contribution)| (*backend_idx, contribution.clone()))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.contributions.is_empty()
    }
}

/// Encode the already-validated Frontend deployment into the canonical
/// Protocol contributions. Identity belongs to the enclosing participant
/// manifest, which the coordinator and the backend each derive by descriptor
/// traversal; the contribution carries content only.
pub(crate) fn encode_install_contributions(
    deployment: &FrontendRuntimeFilterDeployment,
) -> Result<EncodedRuntimeFilterDeployment, DistributedQueryError> {
    let mut contributions = BTreeMap::new();
    let lifecycle = deployment.lifecycle().to_wire();
    for participant in deployment.participants() {
        let contribution = encode_participant(lifecycle, participant);
        if contributions
            .insert(participant.backend_idx(), contribution)
            .is_some()
        {
            return Err(encoding_error(format!(
                "runtime filter deployment encoder repeats backend {}",
                participant.backend_idx()
            )));
        }
    }
    Ok(EncodedRuntimeFilterDeployment { contributions })
}

fn encode_participant(
    lifecycle: filter::RuntimeFilterQueryLifecycleOptions,
    participant: &FrontendRuntimeFilterParticipant,
) -> service::RuntimeFilterContribution {
    service::RuntimeFilterContribution {
        participant_id: participant.participant_id(),
        lifecycle: Some(lifecycle),
        install: Some(participant.install().clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::encode_participant;

    use crate::runtime_filter::model::{
        FrontendRuntimeFilterLifecycle, FrontendRuntimeFilterParticipant,
    };

    #[test]
    fn service_only_contribution_carries_the_typed_empty_install_and_lifecycle() {
        // Keep this assertion focused on the owner-local contribution shape;
        // constructing a sealed artifact belongs to the schedule-view seam.
        let participant = FrontendRuntimeFilterParticipant::service_only(3)
            .expect("service-only participant is valid");
        let lifecycle = FrontendRuntimeFilterLifecycle::new(10, 20, 30, 2, 40, 50, 60)
            .expect("lifecycle is valid");

        let contribution = encode_participant(lifecycle.to_wire(), &participant);

        assert_eq!(contribution.participant_id, participant.participant_id());
        assert_eq!(contribution.lifecycle, Some(lifecycle.to_wire()));
        let install = contribution.install.expect("typed install is required");
        assert!(install.core_channels.is_empty());
        assert!(install.routing_channels.is_empty());
    }
}
