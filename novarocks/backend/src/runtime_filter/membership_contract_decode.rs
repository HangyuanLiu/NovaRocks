//! Typed decoding for the membership portion of a runtime-filter wire contract.

use std::fmt;

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
};
use novarocks_proto_models::plan;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MembershipContractDecodeError {
    UnspecifiedNullSemantics,
    UnknownNullSemantics { raw: i32 },
    InvalidExecutionSchema { detail: String },
}

impl fmt::Display for MembershipContractDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnspecifiedNullSemantics => {
                formatter.write_str("runtime filter membership null semantics is unspecified")
            }
            Self::UnknownNullSemantics { raw } => {
                write!(
                    formatter,
                    "runtime filter membership null semantics is unknown: {raw}"
                )
            }
            Self::InvalidExecutionSchema { detail } => {
                write!(
                    formatter,
                    "runtime filter membership schema is invalid: {detail}"
                )
            }
        }
    }
}

impl std::error::Error for MembershipContractDecodeError {}

/// Rebuilds the Execution-owned membership schema from the carrier's only
/// type authority and the closed wire null-comparison semantics.
pub(crate) fn decode_membership_contract(
    context_data_type: &DataType,
    wire: &plan::RuntimeFilterMembershipContract,
) -> Result<RuntimeFilterMembershipSchema, MembershipContractDecodeError> {
    let null_semantics =
        match plan::RuntimeFilterMembershipNullSemantics::try_from(wire.null_semantics) {
            Ok(plan::RuntimeFilterMembershipNullSemantics::NeverMatches) => {
                RuntimeFilterNullSemantics::NeverMatches
            }
            Ok(plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual) => {
                RuntimeFilterNullSemantics::NullSafeEqual
            }
            Ok(plan::RuntimeFilterMembershipNullSemantics::Unspecified) => {
                return Err(MembershipContractDecodeError::UnspecifiedNullSemantics);
            }
            Err(_) => {
                return Err(MembershipContractDecodeError::UnknownNullSemantics {
                    raw: wire.null_semantics,
                });
            }
        };
    RuntimeFilterMembershipSchema::new(context_data_type, null_semantics).map_err(|error| {
        MembershipContractDecodeError::InvalidExecutionSchema {
            detail: error.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{MembershipContractDecodeError, decode_membership_contract};
    use novarocks_execution::runtime_filter::RuntimeFilterNullSemantics;
    use novarocks_proto_models::plan;

    fn contract(
        null_semantics: plan::RuntimeFilterMembershipNullSemantics,
    ) -> plan::RuntimeFilterMembershipContract {
        plan::RuntimeFilterMembershipContract {
            null_semantics: null_semantics as i32,
        }
    }

    #[test]
    fn rebuilds_membership_schemas_from_context_type_and_closed_semantics() {
        for (data_type, semantics, expected) in [
            (
                DataType::Int64,
                plan::RuntimeFilterMembershipNullSemantics::NeverMatches,
                RuntimeFilterNullSemantics::NeverMatches,
            ),
            (
                DataType::Decimal128(18, 2),
                plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual,
                RuntimeFilterNullSemantics::NullSafeEqual,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual,
                RuntimeFilterNullSemantics::NullSafeEqual,
            ),
        ] {
            let schema = decode_membership_contract(&data_type, &contract(semantics))
                .expect("supported context type and semantics rebuild the schema");
            assert_eq!(schema.data_type(), &data_type);
            assert_eq!(schema.null_semantics(), expected);
        }
    }

    #[test]
    fn rejects_unspecified_and_unknown_null_semantics() {
        assert_eq!(
            decode_membership_contract(
                &DataType::Int64,
                &contract(plan::RuntimeFilterMembershipNullSemantics::Unspecified),
            ),
            Err(MembershipContractDecodeError::UnspecifiedNullSemantics)
        );
        assert_eq!(
            decode_membership_contract(
                &DataType::Int64,
                &plan::RuntimeFilterMembershipContract { null_semantics: 99 },
            ),
            Err(MembershipContractDecodeError::UnknownNullSemantics { raw: 99 })
        );
    }

    #[test]
    fn rejects_context_types_without_an_execution_membership_schema() {
        assert!(matches!(
            decode_membership_contract(
                &DataType::Null,
                &contract(plan::RuntimeFilterMembershipNullSemantics::NeverMatches),
            ),
            Err(MembershipContractDecodeError::InvalidExecutionSchema { .. })
        ));
    }
}
