//! Wire conversion for transport-neutral lifecycle identities.

use novarocks_proto_models::{common, novarocks};
pub use novarocks_types::{AttemptId, QueryExecutionId};

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

/// Decodes a generated execution identity without making the domain value
/// depend on protobuf DTOs.
pub fn decode_query_execution_id(
    src: &novarocks::QueryExecutionId,
) -> Result<QueryExecutionId, ProtocolError> {
    let query_id = src.query_id.as_ref().ok_or_else(|| {
        ProtocolError::new(
            FieldPath::root("query_execution_id").field("query_id"),
            ProtocolErrorKind::MissingField,
            "query id is required",
        )
    })?;
    let attempt_id = AttemptId::new(src.attempt_id).map_err(|error| {
        ProtocolError::new(
            FieldPath::root("query_execution_id").field("attempt_id"),
            ProtocolErrorKind::InvalidValue,
            error.to_string(),
        )
    })?;
    QueryExecutionId::new(
        novarocks_types::QueryId::new(query_id.hi, query_id.lo),
        attempt_id,
    )
    .map_err(|error| {
        ProtocolError::new(
            FieldPath::root("query_execution_id").field("query_id"),
            ProtocolErrorKind::InvalidValue,
            error.to_string(),
        )
    })
}

/// Encodes a transport-neutral execution identity for a generated wire DTO.
pub fn encode_query_execution_id(value: QueryExecutionId) -> novarocks::QueryExecutionId {
    novarocks::QueryExecutionId {
        query_id: Some(common::UniqueId {
            hi: value.query_id().high(),
            lo: value.query_id().low(),
        }),
        attempt_id: value.attempt_id().get(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AttemptId, QueryExecutionId, decode_query_execution_id, encode_query_execution_id,
    };
    use crate::ProtocolErrorKind;
    use novarocks_proto_models::{common, novarocks};
    use novarocks_types::QueryId;

    #[test]
    fn round_trips_a_query_execution_identity_through_proto() {
        let identity = QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(3).expect("nonzero attempt"),
        )
        .expect("nonzero query id");

        let encoded = encode_query_execution_id(identity);
        assert_eq!(decode_query_execution_id(&encoded), Ok(identity));
    }

    #[test]
    fn rejects_invalid_identity_values_in_decoder_order() {
        let missing_query_id = decode_query_execution_id(&novarocks::QueryExecutionId {
            attempt_id: 1,
            ..Default::default()
        })
        .expect_err("query id is required");
        assert_eq!(missing_query_id.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            missing_query_id.path().to_string(),
            "query_execution_id.query_id"
        );
        assert_eq!(missing_query_id.detail(), "query id is required");

        let zero_attempt_precedes_the_zero_query_id_check =
            decode_query_execution_id(&novarocks::QueryExecutionId {
                query_id: Some(common::UniqueId { hi: 0, lo: 0 }),
                attempt_id: 0,
            })
            .expect_err("attempt is validated first");
        assert_eq!(
            zero_attempt_precedes_the_zero_query_id_check.detail(),
            "attempt id must be nonzero"
        );

        let zero_query_id = decode_query_execution_id(&novarocks::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 0, lo: 0 }),
            attempt_id: 1,
        })
        .expect_err("zero query id is invalid");
        assert_eq!(zero_query_id.detail(), "query id must be nonzero");
    }
}
