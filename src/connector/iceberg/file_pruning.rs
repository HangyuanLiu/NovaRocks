use std::collections::HashMap;

use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue};
use crate::common::scan_predicate::{ScanPredicate, ScanPredicateDomain, ScanPredicateSource};
use crate::sql::catalog::{IcebergColumnStats, IcebergDataFileInfo, IcebergPartitionValue};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IcebergFilePruningCounters {
    pub(crate) files_total: u128,
    pub(crate) files_selected: u128,
    pub(crate) files_pruned: u128,
    pub(crate) predicates: u128,
    pub(crate) partition_evaluated: u128,
    pub(crate) stats_evaluated: u128,
    pub(crate) unsupported: u128,
    pub(crate) unavailable: u128,
}

pub(crate) fn min_max_predicates_to_scan_predicates(
    predicates: &[MinMaxPredicate],
) -> Vec<ScanPredicate> {
    predicates
        .iter()
        .cloned()
        .map(|predicate| {
            ScanPredicate::from_min_max_predicate(predicate, ScanPredicateSource::Static)
        })
        .collect()
}

pub(crate) fn file_may_satisfy_min_max(
    file: &IcebergDataFileInfo,
    predicates: &[MinMaxPredicate],
) -> bool {
    let scan_predicates = min_max_predicates_to_scan_predicates(predicates);
    let mut counters = IcebergFilePruningCounters::default();
    file_may_satisfy_scan_predicates(file, &scan_predicates, &mut counters)
}

pub(crate) fn file_may_satisfy_scan_predicates(
    file: &IcebergDataFileInfo,
    predicates: &[ScanPredicate],
    counters: &mut IcebergFilePruningCounters,
) -> bool {
    counters.files_total += 1;
    counters.predicates += predicates.len() as u128;

    if predicates.is_empty() {
        counters.files_selected += 1;
        return true;
    }

    for predicate in predicates {
        if let Some(decision) = partition_may_satisfy_predicate(file, predicate) {
            match decision {
                PredicateDecision::Evaluated(may_satisfy) => {
                    counters.partition_evaluated += 1;
                    if !may_satisfy {
                        counters.files_pruned += 1;
                        return false;
                    }
                    continue;
                }
                PredicateDecision::Unsupported => {}
            }
        }

        match stats_may_satisfy_predicate(file.column_stats.as_ref(), predicate) {
            PredicateDecision::Evaluated(may_satisfy) => {
                counters.stats_evaluated += 1;
                if !may_satisfy {
                    counters.files_pruned += 1;
                    return false;
                }
            }
            PredicateDecision::Unsupported => {
                counters.unsupported += 1;
            }
        }
    }

    counters.files_selected += 1;
    true
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PredicateDecision {
    Evaluated(bool),
    Unsupported,
}

fn partition_may_satisfy_predicate(
    file: &IcebergDataFileInfo,
    predicate: &ScanPredicate,
) -> Option<PredicateDecision> {
    let partition = file.partition_values.iter().find(|value| {
        value.transform.eq_ignore_ascii_case("identity")
            && value.source_column.eq_ignore_ascii_case(predicate.column())
    })?;
    let Some(value) = partition.value.as_ref() else {
        return Some(PredicateDecision::Evaluated(false));
    };
    Some(partition_value_may_satisfy_predicate(value, predicate))
}

fn partition_value_may_satisfy_predicate(
    partition_value: &IcebergPartitionValue,
    predicate: &ScanPredicate,
) -> PredicateDecision {
    match predicate.domain() {
        ScanPredicateDomain::Range { op, value } => {
            partition_value_may_satisfy_range(partition_value, *op, value)
        }
        ScanPredicateDomain::DiscreteSet { values, .. } => {
            partition_value_may_satisfy_discrete_set(partition_value, values)
        }
    }
}

fn partition_value_may_satisfy_range(
    partition_value: &IcebergPartitionValue,
    op: MinMaxPredicateOp,
    value: &MinMaxPredicateValue,
) -> PredicateDecision {
    match partition_value {
        IcebergPartitionValue::Boolean(v) => match value.as_bool() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_i64(
                i64::from(*v),
                i64::from(*v),
                op,
                i64::from(value),
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Int32(v) => match value.as_i64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_i64(
                i64::from(*v),
                i64::from(*v),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Int64(v) => match value.as_i64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_i64(*v, *v, op, value)),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Float(v) => match value.as_f64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_f64(
                f64::from(*v),
                f64::from(*v),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Double(v) => match value.as_f64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_f64(*v, *v, op, value)),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::String(v) => match value.as_bytes() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_bytes(
                v.as_bytes(),
                v.as_bytes(),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Binary(v) => match value.as_bytes() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_bytes(
                v.as_slice(),
                v.as_slice(),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
    }
}

fn partition_value_may_satisfy_discrete_set(
    partition_value: &IcebergPartitionValue,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let any_match = match partition_value {
        IcebergPartitionValue::Boolean(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_bool)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == *v)),
        IcebergPartitionValue::Int32(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_i64)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == i64::from(*v))),
        IcebergPartitionValue::Int64(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_i64)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == *v)),
        IcebergPartitionValue::Float(v) => {
            if v.is_nan() {
                None
            } else {
                values
                    .iter()
                    .map(MinMaxPredicateValue::as_f64)
                    .collect::<Option<Vec<_>>>()
                    .and_then(|values| {
                        if values.iter().any(|value| value.is_nan()) {
                            None
                        } else {
                            Some(values.into_iter().any(|value| value == f64::from(*v)))
                        }
                    })
            }
        }
        IcebergPartitionValue::Double(v) => {
            if v.is_nan() {
                None
            } else {
                values
                    .iter()
                    .map(MinMaxPredicateValue::as_f64)
                    .collect::<Option<Vec<_>>>()
                    .and_then(|values| {
                        if values.iter().any(|value| value.is_nan()) {
                            None
                        } else {
                            Some(values.into_iter().any(|value| value == *v))
                        }
                    })
            }
        }
        IcebergPartitionValue::String(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_bytes)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == v.as_bytes())),
        IcebergPartitionValue::Binary(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_bytes)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == v.as_slice())),
    };

    match any_match {
        Some(any_match) => PredicateDecision::Evaluated(any_match),
        None => PredicateDecision::Unsupported,
    }
}

fn stats_may_satisfy_predicate(
    column_stats: Option<&HashMap<String, IcebergColumnStats>>,
    predicate: &ScanPredicate,
) -> PredicateDecision {
    let Some(column_stats) = column_stats else {
        return PredicateDecision::Unsupported;
    };
    let Some(stats) = find_column_stats(column_stats, predicate.column()) else {
        return PredicateDecision::Unsupported;
    };

    match predicate.domain() {
        ScanPredicateDomain::Range { op, value } => stats_may_satisfy_range(stats, *op, value),
        ScanPredicateDomain::DiscreteSet { values, .. } => {
            stats_may_satisfy_discrete_set(stats, values)
        }
    }
}

fn find_column_stats<'a>(
    column_stats: &'a HashMap<String, IcebergColumnStats>,
    column: &str,
) -> Option<&'a IcebergColumnStats> {
    column_stats.get(column).or_else(|| {
        column_stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, stats)| stats)
    })
}

fn stats_may_satisfy_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: &MinMaxPredicateValue,
) -> PredicateDecision {
    if let Some(value) = value.as_bool() {
        return stats_may_satisfy_bool_range(stats, op, value);
    }
    if let Some(value) = value.as_i64() {
        return stats_may_satisfy_i64_range(stats, op, value);
    }
    if let Some(value) = value.as_f64() {
        return stats_may_satisfy_f64_range(stats, op, value);
    }
    if let Some(value) = value.as_bytes() {
        return stats_may_satisfy_bytes_range(stats, op, value);
    }
    PredicateDecision::Unsupported
}

fn stats_may_satisfy_bool_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: bool,
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(range_may_satisfy_i64(
        i64::from(lower),
        i64::from(upper),
        op,
        i64::from(value),
    ))
}

fn stats_may_satisfy_i64_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: i64,
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(range_may_satisfy_i64(lower, upper, op, value))
}

fn stats_may_satisfy_f64_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: f64,
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    if lower.is_nan() || upper.is_nan() || value.is_nan() {
        return PredicateDecision::Unsupported;
    }
    PredicateDecision::Evaluated(range_may_satisfy_f64(lower, upper, op, value))
}

fn stats_may_satisfy_bytes_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: &[u8],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(range_may_satisfy_bytes(lower, upper, op, value))
}

fn stats_may_satisfy_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(first) = values.first() else {
        return PredicateDecision::Unsupported;
    };

    if first.as_bool().is_some() {
        return stats_may_satisfy_bool_discrete_set(stats, values);
    }
    if first.as_i64().is_some() {
        return stats_may_satisfy_i64_discrete_set(stats, values);
    }
    if first.as_f64().is_some() {
        return stats_may_satisfy_f64_discrete_set(stats, values);
    }
    if first.as_bytes().is_some() {
        return stats_may_satisfy_bytes_discrete_set(stats, values);
    }

    PredicateDecision::Unsupported
}

fn stats_may_satisfy_bool_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_bool)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    let lower = i64::from(lower);
    let upper = i64::from(upper);
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .map(i64::from)
            .any(|value| lower <= value && value <= upper),
    )
}

fn stats_may_satisfy_i64_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_i64)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .any(|value| lower <= value && value <= upper),
    )
}

fn stats_may_satisfy_f64_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_f64)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    if lower.is_nan() || upper.is_nan() || values.iter().any(|value| value.is_nan()) {
        return PredicateDecision::Unsupported;
    }
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .any(|value| lower <= value && value <= upper),
    )
}

fn stats_may_satisfy_bytes_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_bytes)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .any(|value| lower <= value && value <= upper),
    )
}

fn range_may_satisfy_i64(lower: i64, upper: i64, op: MinMaxPredicateOp, value: i64) -> bool {
    match op {
        MinMaxPredicateOp::Le => lower <= value,
        MinMaxPredicateOp::Ge => upper >= value,
        MinMaxPredicateOp::Lt => lower < value,
        MinMaxPredicateOp::Gt => upper > value,
        MinMaxPredicateOp::Eq => lower <= value && value <= upper,
    }
}

fn range_may_satisfy_f64(lower: f64, upper: f64, op: MinMaxPredicateOp, value: f64) -> bool {
    if lower.is_nan() || upper.is_nan() || value.is_nan() {
        return true;
    }
    match op {
        MinMaxPredicateOp::Le => lower <= value,
        MinMaxPredicateOp::Ge => upper >= value,
        MinMaxPredicateOp::Lt => lower < value,
        MinMaxPredicateOp::Gt => upper > value,
        MinMaxPredicateOp::Eq => lower <= value && value <= upper,
    }
}

fn range_may_satisfy_bytes(
    lower: &[u8],
    upper: &[u8],
    op: MinMaxPredicateOp,
    value: &[u8],
) -> bool {
    match op {
        MinMaxPredicateOp::Le => lower <= value,
        MinMaxPredicateOp::Ge => upper >= value,
        MinMaxPredicateOp::Lt => lower < value,
        MinMaxPredicateOp::Gt => upper > value,
        MinMaxPredicateOp::Eq => lower <= value && value <= upper,
    }
}

fn decode_bool_bound(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [0] => Some(false),
        [1] => Some(true),
        _ => None,
    }
}

fn decode_i64_bound(bytes: &[u8]) -> Option<i64> {
    match bytes.len() {
        1 => bytes.first().copied().map(i64::from),
        4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(i64::from(i32::from_le_bytes(arr)))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(i64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn decode_f64_bound(bytes: &[u8]) -> Option<f64> {
    match bytes.len() {
        4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(f64::from(f32::from_le_bytes(arr)))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(f64::from_le_bytes(arr))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::common::scan_predicate::{ScanPredicate, ScanPredicateSource};
    use crate::sql::catalog::{IcebergColumnStats, IcebergDataFileInfo};

    use super::{IcebergFilePruningCounters, file_may_satisfy_scan_predicates};

    #[test]
    fn range_predicate_skips_file_when_stats_do_not_overlap() {
        let file = data_file_with_i64_stats("k1", 10, 20);
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Gt {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(30),
            },
            ScanPredicateSource::Static,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_pruned, 1);
    }

    #[test]
    fn discrete_set_skips_file_when_values_are_outside_file_bounds() {
        let file = data_file_with_i64_stats("k1", 100, 200);
        let predicate = ScanPredicate::discrete_set(
            "k1".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(2),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete set");
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_pruned, 1);
    }

    #[test]
    fn missing_stats_keeps_file() {
        let file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Le {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(0),
            },
            ScanPredicateSource::RuntimeMinMax,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.unsupported, 1);
    }

    #[test]
    fn identity_partition_point_can_skip_file() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        file.partition_values.push(
            crate::sql::catalog::IcebergPartitionFieldValue::identity_int64_for_test("k1", 7),
        );
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Eq {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(9),
            },
            ScanPredicateSource::Static,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.partition_evaluated, 1);
    }

    fn data_file_with_i64_stats(column: &str, lower: i64, upper: i64) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        file.column_stats = Some(HashMap::from([(
            column.to_string(),
            IcebergColumnStats {
                null_count: None,
                value_count: None,
                column_size: None,
                lower_bound: Some(lower.to_le_bytes().to_vec()),
                upper_bound: Some(upper.to_le_bytes().to_vec()),
            },
        )]));
        file
    }
}
