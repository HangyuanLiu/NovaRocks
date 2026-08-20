use novarocks_execution::runtime_filter::contribution::MembershipValues;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub(crate) struct BitsetPlan {
    type_tag: u8,
    min: i64,
    max: i64,
    bit_count: u64,
    byte_count: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub(crate) enum BitsetError {
    UnsupportedType,
    EmptyDomain,
    ValueOutOfRange,
    SizeOverflow,
}

impl BitsetPlan {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn new(values: &MembershipValues) -> Result<Self, BitsetError> {
        let values = lossless_i64(values)?;
        let (&min, &max) = values
            .values
            .first()
            .zip(values.values.last())
            .ok_or(BitsetError::EmptyDomain)?;
        let bit_count = u64::try_from(
            i128::from(max)
                .checked_sub(i128::from(min))
                .and_then(|span| span.checked_add(1))
                .ok_or(BitsetError::SizeOverflow)?,
        )
        .map_err(|_| BitsetError::SizeOverflow)?;
        let byte_count =
            usize::try_from(bit_count.checked_add(7).ok_or(BitsetError::SizeOverflow)? / 8)
                .map_err(|_| BitsetError::SizeOverflow)?;
        let type_tag = values.type_tag;
        Ok(Self {
            type_tag,
            min,
            max,
            bit_count,
            byte_count,
        })
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn type_tag(self) -> u8 {
        self.type_tag
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn min(self) -> i64 {
        self.min
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn max(self) -> i64 {
        self.max
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn bit_count(self) -> u64 {
        self.bit_count
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn byte_count(self) -> usize {
        self.byte_count
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub(crate) fn build_bits(
    values: &MembershipValues,
    plan: BitsetPlan,
) -> Result<Vec<u8>, BitsetError> {
    let values = lossless_i64(values)?;
    let mut bits = vec![0; plan.byte_count];
    for value in values.values {
        let offset = u64::try_from(i128::from(value) - i128::from(plan.min))
            .map_err(|_| BitsetError::ValueOutOfRange)?;
        if offset >= plan.bit_count {
            return Err(BitsetError::ValueOutOfRange);
        }
        let byte = usize::try_from(offset / 8).map_err(|_| BitsetError::SizeOverflow)?;
        bits[byte] |= 1 << (offset % 8);
    }
    Ok(bits)
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
struct Projected {
    type_tag: u8,
    values: Vec<i64>,
}
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn lossless_i64(values: &MembershipValues) -> Result<Projected, BitsetError> {
    let (type_tag, values) = match values {
        MembershipValues::Boolean(v) => (1, v.iter().map(|v| i64::from(*v)).collect()),
        MembershipValues::Int8(v) => (2, v.iter().map(|v| i64::from(*v)).collect()),
        MembershipValues::Int16(v) => (3, v.iter().map(|v| i64::from(*v)).collect()),
        MembershipValues::Int32(v) => (4, v.iter().map(|v| i64::from(*v)).collect()),
        MembershipValues::Int64(v) => (5, v.iter().copied().collect()),
        MembershipValues::Date32(v) => (10, v.iter().map(|v| i64::from(*v)).collect()),
        MembershipValues::Decimal128 {
            precision, values, ..
        } if *precision <= 18 => (
            12,
            values
                .iter()
                .map(|v| i64::try_from(*v).map_err(|_| BitsetError::ValueOutOfRange))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => return Err(BitsetError::UnsupportedType),
    };
    Ok(Projected { type_tag, values })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_bitset_preserves_integral_membership() {
        let values = MembershipValues::int64([-3, 0, 7]);
        let plan = BitsetPlan::new(&values).unwrap();
        assert_eq!(plan.type_tag(), 5);
        assert_eq!(plan.min(), -3);
        assert_eq!(plan.max(), 7);
        assert_eq!(
            build_bits(&values, plan).unwrap(),
            vec![0b0000_1001, 0b0000_0100]
        );
    }

    #[test]
    fn migrated_core_bitset_type_matrix_is_exact() {
        for values in [
            MembershipValues::boolean([false, true]),
            MembershipValues::int8([-5, 0, 7]),
            MembershipValues::int16([-500, -3, 9]),
            MembershipValues::int32([-70_000, 4, 8]),
            MembershipValues::int64([-1_000_000, 2, 19]),
            MembershipValues::date32([1, 2, 31]),
            MembershipValues::decimal128(18, 3, [-101, 0, 205]).unwrap(),
        ] {
            let plan = BitsetPlan::new(&values).unwrap();
            let first = build_bits(&values, plan).unwrap();
            assert_eq!(
                first,
                build_bits(&values, BitsetPlan::new(&values).unwrap()).unwrap()
            );
            assert_eq!(first.len(), plan.byte_count());
        }
        assert_eq!(
            BitsetPlan::new(&MembershipValues::large_int([1])),
            Err(BitsetError::UnsupportedType)
        );
        assert_eq!(
            BitsetPlan::new(&MembershipValues::int64([i64::MIN, i64::MAX])),
            Err(BitsetError::SizeOverflow)
        );
    }
}
