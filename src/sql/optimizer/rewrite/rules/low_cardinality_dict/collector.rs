//! Collector pass for `LowCardinalityDictionaryRewrite`.
//!
//! Walks the logical plan once, top-to-bottom, and asks the
//! `QueryDictionaryProvider` for an active dictionary snapshot on every
//! string-typed column produced by a `Scan`. Eligible mappings get
//! registered into the rule-local `DictionaryRewriteContext` which the
//! subsequent rewriter pass consumes.
//!
//! The collector deliberately does not look at non-scan plumbing
//! (Aggregate / Sort / Project / Join etc.): the rewriter consults the
//! same `DictionaryRewriteContext` and applies node-specific behavior
//! there. That keeps the collector focused on snapshot discovery.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

use super::context::{DictionaryRewriteContext, ScanColumnKey};
use super::expr::is_string_like;

pub(crate) fn collect(
    plan: &LogicalPlan,
    rewrite_ctx: &RewriteContext,
) -> Result<DictionaryRewriteContext, String> {
    let mut dict_ctx = DictionaryRewriteContext::default();
    let Some(provider) = rewrite_ctx.dictionary_provider() else {
        return Ok(dict_ctx);
    };
    walk(plan, provider.as_ref(), &mut dict_ctx)?;
    Ok(dict_ctx)
}

fn walk(
    plan: &LogicalPlan,
    provider: &dyn crate::sql::optimizer::rewrite::context::QueryDictionaryProvider,
    dict_ctx: &mut DictionaryRewriteContext,
) -> Result<(), String> {
    match plan {
        LogicalPlan::Scan(scan) => {
            visit_scan(scan, provider, dict_ctx)?;
        }
        LogicalPlan::Filter(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Project(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Aggregate(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Sort(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Limit(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Window(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::TableFunction(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::SubqueryAlias(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Repeat(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::CTEProduce(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Decode(node) => walk(&node.input, provider, dict_ctx)?,
        LogicalPlan::Join(node) => {
            // TODO(task-8): joins with matching dict snapshots on both
            // sides could keep dict ids through the equi-join; today
            // the rewriter inserts a Decode boundary instead.
            walk(&node.left, provider, dict_ctx)?;
            walk(&node.right, provider, dict_ctx)?;
        }
        LogicalPlan::CTEAnchor(node) => {
            walk(&node.produce, provider, dict_ctx)?;
            walk(&node.consumer, provider, dict_ctx)?;
        }
        LogicalPlan::Union(node) => {
            // TODO(task-8): UNION ALL with matching dicts on every leg
            // can propagate dict columns upward; for Task 7 we treat
            // every set op as a decode boundary.
            for input in &node.inputs {
                walk(input, provider, dict_ctx)?;
            }
        }
        LogicalPlan::Intersect(node) => {
            for input in &node.inputs {
                walk(input, provider, dict_ctx)?;
            }
        }
        LogicalPlan::Except(node) => {
            for input in &node.inputs {
                walk(input, provider, dict_ctx)?;
            }
        }
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {}
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
    Ok(())
}

fn visit_scan(
    scan: &ScanNode,
    provider: &dyn crate::sql::optimizer::rewrite::context::QueryDictionaryProvider,
    dict_ctx: &mut DictionaryRewriteContext,
) -> Result<(), String> {
    for col in &scan.columns {
        if !is_string_like(&col.data_type) {
            continue;
        }
        // Respect pruning: if the scan has been pruned and this column
        // is not in the required set, skip it.
        if let Some(required) = &scan.required_columns {
            let lower = col.name.to_ascii_lowercase();
            if !required.iter().any(|r| r.to_ascii_lowercase() == lower) {
                continue;
            }
        }
        let snapshot = provider.load_active_snapshot(&scan.table, &scan.database, &col.name)?;
        if let Some(snapshot) = snapshot {
            let key = ScanColumnKey::new(&scan.database, &scan.table.name, &col.name);
            dict_ctx.register_scan_column(key, snapshot);
        }
    }
    Ok(())
}
