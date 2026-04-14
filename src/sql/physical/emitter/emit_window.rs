/// Group window expressions by their (partition_by, order_by) signature.
pub fn group_win_exprs_by_sig(exprs: &[crate::sql::plan::WindowExpr]) -> Vec<Vec<usize>> {
    let sig = |e: &crate::sql::plan::WindowExpr| -> String {
        format!(
            "{:?}|{:?}",
            e.partition_by
                .iter()
                .map(|p| format!("{:?}", p.kind))
                .collect::<Vec<_>>(),
            e.order_by
                .iter()
                .map(|o| format!("{:?}:{}", o.expr.kind, o.asc))
                .collect::<Vec<_>>(),
        )
    };
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (i, e) in exprs.iter().enumerate() {
        let s = sig(e);
        if let Some(g) = groups.iter_mut().find(|(gs, _)| *gs == s) {
            g.1.push(i);
        } else {
            groups.push((s, vec![i]));
        }
    }
    groups.into_iter().map(|(_, indices)| indices).collect()
}
