use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::estimate::arith::{MAX_ROW_COUNT, damped_conjunction, sat_mul};
use crate::sql::optimizer::statistics::{ANTI_JOIN_SELECTIVITY, Confidence, SEMI_JOIN_SELECTIVITY};

pub struct JoinCardInput {
    pub left: (f64, Confidence),
    pub right: (f64, Confidence),
    pub kind: JoinKind,
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    pub non_equi_selectivity: Option<(f64, Confidence)>,
}

pub fn estimate_join_cardinality(input: &JoinCardInput) -> (f64, Confidence) {
    let (left_rows, left_saturated) = row_count(input.left.0);
    let (right_rows, right_saturated) = row_count(input.right.0);
    let input_saturated = left_saturated || right_saturated;

    let mut confidence_inputs = vec![input.left.1, input.right.1];
    let (rows, saturated, used_default_or_invalid) = match input.kind {
        JoinKind::Cross => {
            let (rows, saturated) = sat_mul(left_rows, right_rows);
            (rows, saturated, false)
        }
        JoinKind::Inner => {
            let (rows, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (rows.max(1.0), saturated, used_default_or_invalid)
        }
        JoinKind::LeftOuter => {
            let (inner, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (inner.max(left_rows), saturated, used_default_or_invalid)
        }
        JoinKind::RightOuter => {
            let (inner, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (inner.max(right_rows), saturated, used_default_or_invalid)
        }
        JoinKind::FullOuter => {
            let (inner, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (
                inner.max(left_rows).max(right_rows),
                saturated,
                used_default_or_invalid,
            )
        }
        JoinKind::LeftSemi => {
            let (selectivity, used_default_or_invalid) =
                semi_selectivity(input, &mut confidence_inputs);
            let (rows, saturated) = bounded_side_rows(left_rows, selectivity);
            (rows, saturated, used_default_or_invalid)
        }
        JoinKind::RightSemi => {
            let (selectivity, used_default_or_invalid) =
                semi_selectivity(input, &mut confidence_inputs);
            let (rows, saturated) = bounded_side_rows(right_rows, selectivity);
            (rows, saturated, used_default_or_invalid)
        }
        JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            let (rows, saturated) = bounded_side_rows(left_rows, ANTI_JOIN_SELECTIVITY);
            (rows, saturated, true)
        }
        JoinKind::RightAnti => {
            let (rows, saturated) = bounded_side_rows(right_rows, ANTI_JOIN_SELECTIVITY);
            (rows, saturated, true)
        }
    };

    if input_saturated || saturated || rows >= MAX_ROW_COUNT {
        return (MAX_ROW_COUNT.min(rows), Confidence::Fallback);
    }

    let combined_input_conf = confidence_inputs
        .into_iter()
        .reduce(Confidence::combine)
        .unwrap_or(Confidence::Estimated);
    (
        rows,
        Confidence::derive(&[combined_input_conf], used_default_or_invalid),
    )
}

fn row_count(rows: f64) -> (f64, bool) {
    if rows.is_nan() {
        return (1.0, true);
    }
    sat_mul(rows.max(1.0), 1.0)
}

fn inner_rows(
    left_rows: f64,
    right_rows: f64,
    input: &JoinCardInput,
    confidence_inputs: &mut Vec<Confidence>,
) -> (f64, bool, bool) {
    let key_selectivity = if input.eq_key_ndvs.is_empty() {
        1.0
    } else {
        let mut selectivities = Vec::with_capacity(input.eq_key_ndvs.len());
        let mut used_default_or_invalid = false;
        for &(left_ndv, right_ndv, confidence) in &input.eq_key_ndvs {
            confidence_inputs.push(confidence);
            let (denominator, invalid_ndv) = ndv_denominator(left_ndv, right_ndv);
            used_default_or_invalid |= invalid_ndv;
            selectivities.push(1.0 / denominator);
        }
        return inner_rows_with_key_selectivity(
            left_rows,
            right_rows,
            input,
            confidence_inputs,
            damped_conjunction(&selectivities),
            used_default_or_invalid,
        );
    };

    inner_rows_with_key_selectivity(
        left_rows,
        right_rows,
        input,
        confidence_inputs,
        key_selectivity,
        false,
    )
}

fn inner_rows_with_key_selectivity(
    left_rows: f64,
    right_rows: f64,
    input: &JoinCardInput,
    confidence_inputs: &mut Vec<Confidence>,
    key_selectivity: f64,
    mut used_default_or_invalid: bool,
) -> (f64, bool, bool) {
    let non_equi = non_equi_selectivity(input, confidence_inputs);
    used_default_or_invalid |= non_equi.1;
    let (product, product_saturated) = sat_mul(left_rows, right_rows);
    let (rows, selectivity_saturated) = sat_mul(product, key_selectivity * non_equi.0);
    (
        rows.max(1.0),
        product_saturated || selectivity_saturated,
        used_default_or_invalid,
    )
}

fn ndv_denominator(left_ndv: f64, right_ndv: f64) -> (f64, bool) {
    let left = valid_ndv(left_ndv);
    let right = valid_ndv(right_ndv);
    let denominator = match (left, right) {
        (Some(left), Some(right)) => left.max(right),
        (Some(ndv), None) | (None, Some(ndv)) => ndv,
        (None, None) => 1.0,
    };
    (denominator, left.is_none() || right.is_none())
}

fn valid_ndv(ndv: f64) -> Option<f64> {
    if ndv.is_finite() && ndv > 0.0 {
        Some(ndv.max(1.0))
    } else {
        None
    }
}

fn non_equi_selectivity(
    input: &JoinCardInput,
    confidence_inputs: &mut Vec<Confidence>,
) -> (f64, bool) {
    if let Some((selectivity, confidence)) = input.non_equi_selectivity {
        confidence_inputs.push(confidence);
        clamp_selectivity(selectivity)
    } else {
        (1.0, false)
    }
}

fn semi_selectivity(input: &JoinCardInput, confidence_inputs: &mut Vec<Confidence>) -> (f64, bool) {
    if let Some((selectivity, confidence)) = input.non_equi_selectivity {
        confidence_inputs.push(confidence);
        clamp_selectivity(selectivity)
    } else {
        (SEMI_JOIN_SELECTIVITY, true)
    }
}

fn bounded_side_rows(side_rows: f64, selectivity: f64) -> (f64, bool) {
    let (selectivity, invalid_selectivity) = clamp_selectivity(selectivity);
    let (rows, saturated) = sat_mul(side_rows, selectivity);
    (rows.clamp(1.0, side_rows), saturated || invalid_selectivity)
}

fn clamp_selectivity(selectivity: f64) -> (f64, bool) {
    if selectivity.is_finite() {
        (
            selectivity.clamp(0.0, 1.0),
            !(0.0..=1.0).contains(&selectivity),
        )
    } else {
        (1.0, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;
    use crate::sql::optimizer::statistics::Confidence;

    fn inp(kind: JoinKind, l: f64, r: f64, keys: Vec<(f64, f64)>) -> JoinCardInput {
        JoinCardInput {
            left: (l, Confidence::Estimated),
            right: (r, Confidence::Estimated),
            kind,
            eq_key_ndvs: keys
                .into_iter()
                .map(|(a, b)| (a, b, Confidence::Estimated))
                .collect(),
            non_equi_selectivity: None,
        }
    }

    #[test]
    fn single_key_inner_matches_containment() {
        let (rows, _) =
            estimate_join_cardinality(&inp(JoinKind::Inner, 1000.0, 800.0, vec![(100.0, 50.0)]));
        assert!((rows - 8000.0).abs() < 1.0, "got {rows}");
    }

    #[test]
    fn multikey_inner_does_not_collapse_or_inflate() {
        let (rows, _) = estimate_join_cardinality(&inp(
            JoinKind::Inner,
            1000.0,
            1000.0,
            vec![(100.0, 100.0), (100.0, 100.0)],
        ));
        assert!(
            rows < 10000.0 && rows > 1.0,
            "multikey should reduce below single-key but not collapse: {rows}"
        );
        assert!((rows - 1000.0).abs() < 50.0, "got {rows}");
    }

    #[test]
    fn outer_join_at_least_preserved_side() {
        let (rows, _) =
            estimate_join_cardinality(&inp(JoinKind::LeftOuter, 5000.0, 10.0, vec![(1e6, 1e6)]));
        assert!(rows >= 5000.0, "left outer must keep >= left rows: {rows}");
    }

    #[test]
    fn cross_join_saturates_with_fallback() {
        let (rows, conf) = estimate_join_cardinality(&inp(JoinKind::Cross, 1e9, 1e9, vec![]));
        assert_eq!(rows, crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT);
        assert_eq!(conf, Confidence::Fallback);
    }

    #[test]
    fn inner_join_reports_fallback_when_intermediate_product_saturates() {
        let (rows, conf) =
            estimate_join_cardinality(&inp(JoinKind::Inner, 1e12, 1e12, vec![(1e12, 1e12)]));
        assert!(rows < crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT);
        assert_eq!(conf, Confidence::Fallback);
    }

    #[test]
    fn default_semi_and_anti_selectivity_are_fallback() {
        let exact_input = JoinCardInput {
            left: (1000.0, Confidence::Exact),
            right: (50.0, Confidence::Exact),
            kind: JoinKind::LeftSemi,
            eq_key_ndvs: vec![],
            non_equi_selectivity: None,
        };
        let (_, semi_conf) = estimate_join_cardinality(&exact_input);
        assert_eq!(semi_conf, Confidence::Fallback);

        let anti_input = JoinCardInput {
            kind: JoinKind::LeftAnti,
            ..exact_input
        };
        let (_, anti_conf) = estimate_join_cardinality(&anti_input);
        assert_eq!(anti_conf, Confidence::Fallback);
    }

    #[test]
    fn invalid_ndv_and_selectivity_degrade_confidence() {
        let invalid_ndv_input = JoinCardInput {
            left: (1000.0, Confidence::Exact),
            right: (1000.0, Confidence::Exact),
            kind: JoinKind::Inner,
            eq_key_ndvs: vec![(f64::NAN, -1.0, Confidence::Exact)],
            non_equi_selectivity: None,
        };
        let (ndv_rows, ndv_conf) = estimate_join_cardinality(&invalid_ndv_input);
        assert!(ndv_rows.is_finite());
        assert_eq!(ndv_conf, Confidence::Fallback);

        let invalid_selectivity_input = JoinCardInput {
            eq_key_ndvs: vec![],
            non_equi_selectivity: Some((f64::INFINITY, Confidence::Exact)),
            ..invalid_ndv_input
        };
        let (sel_rows, sel_conf) = estimate_join_cardinality(&invalid_selectivity_input);
        assert!(sel_rows.is_finite());
        assert_eq!(sel_conf, Confidence::Fallback);
    }

    #[test]
    fn semi_and_anti_bounded_by_left() {
        let (semi, _) =
            estimate_join_cardinality(&inp(JoinKind::LeftSemi, 1000.0, 50.0, vec![(10.0, 10.0)]));
        assert!(semi <= 1000.0 && semi >= 1.0);
        let (anti, _) =
            estimate_join_cardinality(&inp(JoinKind::LeftAnti, 1000.0, 50.0, vec![(10.0, 10.0)]));
        assert!(anti <= 1000.0 && anti >= 1.0);
    }
}
