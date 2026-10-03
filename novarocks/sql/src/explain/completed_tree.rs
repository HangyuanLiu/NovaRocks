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

//! EXPLAIN over a completed plan, as the operator tree a reader expects.
//!
//! What EXPLAIN is for is seeing the operators: which relations are read, in
//! what order they are joined, where the rows are exchanged. That is what the
//! sealed planner printed and what plan-shape assertions are written against,
//! so it is what this prints -- from the completed plan, which is now where
//! those facts live. The plan contract itself -- value and expression
//! definitions, partition spaces, filter attachments, cut evidence -- is a
//! different question with its own level, in [`super::completed`].
//!
//! Names come from the plan's own display annotations. A value the statement
//! named is printed by that name; one it did not is printed by its identity,
//! which is the only honest thing to call it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    AnnotationSubject, Distribution, ExprId, ExprKind, Fragment, FragmentId, FragmentSink,
    LiteralValue, NodeId, PhysicalNode, PhysicalPlan, PlanAnnotation, Relation, SortExpr, ValueId,
};

use crate::compiler::SqlCompileError;
use crate::explain::ExplainLevel;
use novarocks_type_contract::{CompileCheckpoints, PureCompileControl};

/// One completed plan, printed as its operator tree.
pub fn render_completed_plan_tree(
    plan: &PhysicalPlan,
    level: ExplainLevel,
    control: &dyn PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    super::format_checked(control, |work| {
        let context = TreeContext::new(plan, level, work)?;
        let mut out = Vec::new();
        if context.costs() {
            for annotation in plan.annotations() {
                if annotation.subject == AnnotationSubject::Plan
                    && annotation.key.as_ref()
                        == crate::optimizer::stats_input::TABLE_STATISTICS_ANNOTATION_KEY
                {
                    out.push(super::copy_text(&annotation.value, work)?);
                }
                work.step()?;
            }
        }
        context.render_runtime_filters(&mut out, work)?;
        for (display_id, fragment_id) in context.fragment_order(work)?.into_iter().enumerate() {
            let Some(fragment) = plan.fragments().get(&fragment_id) else {
                work.step()?;
                continue;
            };
            if context.detailed() {
                out.push(format!("PLAN FRAGMENT {display_id}"));
                out.push("  OUTPUT EXPRS: *".to_string());
                out.push(format!(
                    "  PARTITION: {}",
                    context.distribution_label(
                        fragment_id,
                        fragment.root_output_distribution(),
                        work
                    )?
                ));
                context.render_sink(fragment, &mut out, work)?;
            }
            context.render_node(fragment, fragment.root(), 0, &mut out, work)?;
            work.step()?;
        }
        Ok(out)
    })
}

/// What a fragment's own rows are laid out as, read at its root.
trait RootDistribution {
    fn root_output_distribution(&self) -> &Distribution;
}

impl RootDistribution for Fragment {
    fn root_output_distribution(&self) -> &Distribution {
        self.nodes()
            .get(&self.root())
            .map_or(&Distribution::Unconstrained, |node| {
                &node.output_properties.distribution
            })
    }
}

struct TreeContext<'a> {
    plan: &'a PhysicalPlan,
    level: ExplainLevel,
    node_annotations: BTreeMap<(FragmentId, NodeId), Vec<&'a PlanAnnotation>>,
    value_names: BTreeMap<(FragmentId, ValueId), &'a str>,
    /// What a reader calls each node.
    ///
    /// A node's identity is unique within its fragment, which is all the plan
    /// needs; a reader looking at every fragment at once needs a name that is
    /// unique across them, so each node is numbered where it is met.
    display_ids: BTreeMap<(FragmentId, NodeId), usize>,
}

impl<'a> TreeContext<'a> {
    fn new(
        plan: &'a PhysicalPlan,
        level: ExplainLevel,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, SqlCompileError> {
        let mut node_annotations: BTreeMap<_, Vec<&PlanAnnotation>> = BTreeMap::new();
        let mut value_names = BTreeMap::new();
        for annotation in plan.annotations() {
            match annotation.subject {
                AnnotationSubject::Node(fragment, node) => {
                    node_annotations
                        .entry((fragment, node))
                        .or_default()
                        .push(annotation);
                }
                AnnotationSubject::Value(fragment, value)
                    if annotation.key.as_ref() == "sql.display_name" =>
                {
                    value_names.insert((fragment, value), annotation.value.as_ref());
                }
                _ => {}
            }
            work.step()?;
        }
        let mut context = Self {
            plan,
            level,
            node_annotations,
            value_names,
            display_ids: BTreeMap::new(),
        };
        let mut next = 0_usize;
        for fragment_id in context.fragment_order(work)? {
            let Some(fragment) = plan.fragments().get(&fragment_id) else {
                work.step()?;
                continue;
            };
            let mut pending = vec![fragment.root()];
            while let Some(node_id) = pending.pop() {
                let visited = context
                    .display_ids
                    .insert((fragment_id, node_id), next)
                    .is_some();
                work.step()?;
                if visited {
                    continue;
                }
                next = next.saturating_add(1);
                if let Some(node) = fragment.nodes().get(&node_id) {
                    for input in node.inputs.iter().rev() {
                        pending.push(*input);
                        work.step()?;
                    }
                }
            }
        }
        Ok(context)
    }

    fn display_id(&self, fragment: FragmentId, node: NodeId) -> usize {
        self.display_ids
            .get(&(fragment, node))
            .copied()
            .unwrap_or_else(|| node.get() as usize)
    }

    const fn detailed(&self) -> bool {
        !matches!(self.level, ExplainLevel::Normal)
    }

    const fn costs(&self) -> bool {
        matches!(self.level, ExplainLevel::Costs | ExplainLevel::Analyze)
    }

    /// Levels that describe how a node will be executed rather than what it
    /// costs. `COSTS` deliberately is not one: it answers a narrower question
    /// and says only what bears on the number it prints.
    const fn verbose(&self) -> bool {
        matches!(self.level, ExplainLevel::Verbose | ExplainLevel::Analyze)
    }

    /// The fragment that delivers the statement's rows first, then the rest.
    ///
    /// A reader follows the plan from what it answers back to what it reads,
    /// and the numbering is the reader's, not the plan's.
    fn fragment_order(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<FragmentId>, SqlCompileError> {
        let root = self
            .plan
            .result_port()
            .map_or_else(
                || self.plan.fragments().keys().next().copied(),
                |port| Some(port.fragment),
            )
            .unwrap_or(FragmentId::new(0));
        let mut order = vec![root];
        work.step()?;
        for fragment in self.plan.fragments().keys() {
            if *fragment != root {
                order.push(*fragment);
            }
            work.step()?;
        }
        Ok(order)
    }

    fn node_annotation(
        &self,
        fragment: FragmentId,
        node: NodeId,
        key: &str,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a str>, SqlCompileError> {
        let Some(annotations) = self.node_annotations.get(&(fragment, node)) else {
            work.step()?;
            return Ok(None);
        };
        for annotation in annotations {
            let matches = annotation.key.as_ref() == key;
            work.step()?;
            if matches {
                return Ok(Some(annotation.value.as_ref()));
            }
        }
        Ok(None)
    }

    /// What a value is called, which is what the statement called it.
    fn value_name(
        &self,
        fragment: FragmentId,
        value: ValueId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<String, SqlCompileError> {
        let name = self.value_names.get(&(fragment, value));
        work.step()?;
        match name {
            Some(name) => super::copy_text(name, work),
            None => Ok(format!("v{}", value.get())),
        }
    }

    /// The filters one side of a join builds for another side to read.
    ///
    /// A reader asks two things of a runtime filter: what it carries, and
    /// where each end of it sits. Both ends name the expression they are
    /// bound to, because that is what makes a filter checkable against the
    /// join it came from.
    fn render_runtime_filters(
        &self,
        out: &mut Vec<String>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        use novarocks_physical_plan::RuntimeFilterDomain;

        if !self.detailed() || self.plan.runtime_filters().is_empty() {
            work.step()?;
            return Ok(());
        }
        out.push("RUNTIME FILTER GRAPH".to_string());
        for filter in self.plan.runtime_filters().values() {
            match &filter.domain {
                RuntimeFilterDomain::Membership { .. } => {
                    out.push(format!("  runtime filter channel {}", filter.id.get()));
                }
                RuntimeFilterDomain::Ordered {
                    key,
                    inclusive,
                    comparator: _,
                } => {
                    out.push("  runtime filter".to_string());
                    out.push(format!(
                        "    domain = OrderedBound(key={} {} NULLS {}, inclusive={inclusive})",
                        key.ty.data_type,
                        match key.direction {
                            novarocks_physical_plan::SortDirection::Ascending => "ASC",
                            novarocks_physical_plan::SortDirection::Descending => "DESC",
                        },
                        match key.null_ordering {
                            novarocks_physical_plan::NullOrdering::First => "FIRST",
                            novarocks_physical_plan::NullOrdering::Last => "LAST",
                        }
                    ));
                }
            }
            for producer in filter.producers.iter() {
                out.push(format!(
                    "    producer binding {}, fragment = {}, node = {}, expr = ({})",
                    filter.id.get(),
                    producer.endpoint.fragment.get(),
                    self.display_id(producer.endpoint.fragment, producer.endpoint.node),
                    self.endpoint_text(&producer.endpoint, work)?
                ));
                work.step()?;
            }
            for consumer in filter.consumers.iter() {
                out.push(format!(
                    "    consumer binding {}, fragment = {}, node = {}, expr = ({}), activation = {}",
                    filter.id.get(),
                    consumer.endpoint.fragment.get(),
                    self.display_id(consumer.endpoint.fragment, consumer.endpoint.node),
                    self.endpoint_text(&consumer.endpoint, work)?,
                    activation_text(&consumer.activation)
                ));
                work.step()?;
            }
            work.step()?;
        }

        work.step()?;
        Ok(())
    }

    /// The values one end of a filter is bound to.
    fn endpoint_text(
        &self,
        endpoint: &novarocks_physical_plan::RuntimeFilterEndpoint,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<String, SqlCompileError> {
        let values = endpoint
            .values
            .iter()
            .map(|value| self.value_name(endpoint.fragment, *value, work))
            .collect::<Result<Vec<_>, SqlCompileError>>()?;
        super::join_text(&values, ", ", work)
    }

    fn render_sink(
        &self,
        fragment: &Fragment,
        out: &mut Vec<String>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        match fragment.sink() {
            FragmentSink::Stream { edge } => self.render_edge_sink(*edge, out, work)?,
            FragmentSink::Multicast { edges } => {
                for edge in edges.iter() {
                    self.render_edge_sink(*edge, out, work)?;
                }
            }
            FragmentSink::Router { routes, .. } => {
                for route in routes.iter() {
                    self.render_edge_sink(route.edge, out, work)?;
                }
            }
            FragmentSink::Result | FragmentSink::Noop => {}
        }

        work.step()?;
        Ok(())
    }

    fn render_edge_sink(
        &self,
        edge: novarocks_physical_plan::EdgeId,
        out: &mut Vec<String>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        let Some(edge) = self.plan.edges().get(&edge) else {
            work.step()?;
            return Ok(());
        };
        out.push("  STREAM DATA SINK".to_string());
        out.push(format!(
            "    EXCHANGE ID: {}",
            self.display_id(edge.destination.fragment, edge.destination.node)
        ));
        out.push(format!(
            "    PARTITION: {}",
            self.distribution_label(
                edge.destination.fragment,
                &edge.partitioning.destination,
                work
            )?
        ));

        work.step()?;
        Ok(())
    }

    /// One node and everything under it.
    ///
    /// Recursion is bounded by the tree-depth the contract already validates,
    /// so the traversal reads the way the output does.
    fn render_node(
        &self,
        fragment: &Fragment,
        node_id: NodeId,
        indent: usize,
        out: &mut Vec<String>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        let pad = "  ".repeat(indent);
        let Some(node) = fragment.nodes().get(&node_id) else {
            out.push(format!("{pad}{}:UNKNOWN", node_id.get()));
            work.step()?;
            return Ok(());
        };
        self.render_node_lines(fragment, node, &pad, out, work)?;
        for input in node.inputs.iter() {
            self.render_node(fragment, *input, indent.saturating_add(1), out, work)?;
        }

        work.step()?;
        Ok(())
    }

    fn stats_suffix(
        &self,
        fragment: FragmentId,
        node: NodeId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<String, SqlCompileError> {
        if !self.detailed() {
            work.step()?;
            return Ok(String::new());
        }
        let Some(statistics) =
            self.node_annotation(fragment, node, "optimizer.statistics", work)?
        else {
            work.step()?;
            return Ok(String::new());
        };
        let mut rows = "?".to_string();
        let mut confidence = String::new();
        for field in statistics.split(", ") {
            if let Some(value) = field.strip_prefix("rows=") {
                rows = row_count_text(value);
            } else if let Some(value) = field.strip_prefix("conf=")
                && self.costs()
            {
                confidence = format!(" conf={value}");
            }
            work.step()?;
        }
        let value = format!(" stats={{rows={rows}{confidence}}}");
        work.step()?;
        Ok(value)
    }

    /// What the cost model decided about broadcasting this join.
    ///
    /// The verdict is the part a reader acts on, so it shows from Verbose; the
    /// numbers behind it belong with the other costs.
    fn broadcast_suffix(
        &self,
        fragment: FragmentId,
        node: NodeId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<String, SqlCompileError> {
        if !self.detailed() {
            work.step()?;
            return Ok(String::new());
        }
        let Some(decision) = self.node_annotation(fragment, node, "optimizer.broadcast", work)?
        else {
            work.step()?;
            return Ok(String::new());
        };
        let mut verdict = "unknown";
        for field in decision.split(", ") {
            let value = field.strip_prefix("verdict=");
            work.step()?;
            if let Some(value) = value {
                verdict = value;
                break;
            }
        }
        let mut suffix = format!(" bcast_verdict={verdict}");
        if self.costs() {
            let _ = write!(suffix, " bcast[{decision}]");
        }
        work.step()?;
        Ok(suffix)
    }
}

/// One estimated row count, as a count.
///
/// The estimate is arithmetic over fractions and arrives as one; a reader
/// counts rows. An estimate of none and an estimate too large to mean
/// anything both say so rather than printing a number.
fn row_count_text(value: &str) -> String {
    /// Above this the estimate has stopped being a number a reader can use.
    const UNBOUNDED: f64 = 1e15;

    let Ok(rows) = value.parse::<f64>() else {
        return value.to_string();
    };
    if rows.is_nan() || rows <= 0.0 {
        "?".to_string()
    } else if rows.is_infinite() || rows >= UNBOUNDED {
        ">=1e15".to_string()
    } else {
        format!("{}", rows.round() as i64)
    }
}

/// The relation a scan reads, without the alias the statement gave it.
///
/// The header prints the name as the statement wrote it, alias and all; the
/// line below names the relation itself, which is what a reader checks
/// against the catalog.
/// Whether this scan's shape admits min/max pruning.
///
/// Two things have to hold: the scan reads data, not a metadata relation --
/// a metadata relation's rows describe the table rather than being it -- and
/// every column it projects is one whose type a reader can state bounds for.
/// This is a property of the shape, not evidence that bounds exist.
fn scan_admits_min_max_stats(
    fragment: &Fragment,
    relation: &Relation,
    node: &PhysicalNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    if !matches!(relation, Relation::Data(_)) {
        work.step()?;
        return Ok(false);
    }
    for value in node.output.columns.iter() {
        let admits = fragment
            .values()
            .get(value)
            .is_some_and(|definition| type_states_bounds(&definition.ty.data_type));
        work.step()?;
        if !admits {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether a reader can state a min and a max for this type.
fn type_states_bounds(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

fn relation_table(relation: &str) -> &str {
    relation
        .split_once(" (alias=")
        .map_or(relation, |(table, _)| table)
}

impl TreeContext<'_> {
    /// How a layout places its rows, and what it places them by.
    fn distribution_label(
        &self,
        fragment: FragmentId,
        distribution: &Distribution,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<String, SqlCompileError> {
        let value = match distribution {
            Distribution::Singleton | Distribution::Unconstrained => "UNPARTITIONED".to_string(),
            Distribution::RoundRobin => "RANDOM".to_string(),
            Distribution::Broadcast => "BROADCAST".to_string(),
            Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => {
                let values = keys
                    .iter()
                    .map(|value| self.value_name(fragment, *value, work))
                    .collect::<Result<Vec<_>, SqlCompileError>>()?;
                let keys = super::join_text(&values, ", ", work)?;
                if matches!(distribution, Distribution::Hash { .. }) {
                    format!("HASH_PARTITIONED ({keys})")
                } else {
                    format!("BUCKET_SHUFFLE_HASH_PARTITIONED ({keys})")
                }
            }
        };
        work.step()?;
        Ok(value)
    }
}

/// Diagnostic expansion borrows the actual definitions and selected constants.
/// It is not a semantic identity or a source-language reconstruction API.
fn expression_text(
    context: &TreeContext<'_>,
    fragment: &Fragment,
    expr: ExprId,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    if depth > 64 {
        work.step()?;
        return Ok("...".into());
    }
    let Some(node) = fragment.expressions().get(expr) else {
        work.step()?;
        return Ok(format!("e{}", expr.get()));
    };
    macro_rules! inner {
        ($id:expr) => {
            expression_text(context, fragment, $id, depth.saturating_add(1), work)?
        };
    }
    let value = match &node.kind {
        ExprKind::Value(value) => context.value_name(fragment.id(), *value, work)?,
        ExprKind::Literal(value) => literal_text_observed(value, work)?,
        ExprKind::Constant(reference) => {
            let value = context
                .plan
                .constants()
                .resolve_observed(*reference, &node.ty, work)
                .map_err(|error| match error {
                    novarocks_physical_plan::ConstantReferenceError::Control(cause) => cause.into(),
                    novarocks_physical_plan::ConstantReferenceError::Constant(error) => {
                        error.into()
                    }
                    other => SqlCompileError::Compilation(other.to_string()),
                })?;
            work.flush()?;
            crate::constant::format_constant_observed(&value, work.control())?
        }
        ExprKind::LambdaParameter { ordinal, .. } => format!("arg{ordinal}"),
        ExprKind::Lambda { body, .. } => format!("-> {}", inner!(*body)),
        ExprKind::Unary { op, expr } => {
            use novarocks_physical_plan::UnaryOperator;
            let op = match op {
                UnaryOperator::Plus => "+",
                UnaryOperator::Minus => "-",
                UnaryOperator::Not => "NOT ",
                UnaryOperator::BitwiseNot => "~",
            };
            format!("{op}{}", inner!(*expr))
        }
        ExprKind::Binary {
            left, op, right, ..
        } => {
            format!(
                "{} {} {}",
                inner!(*left),
                binary_operator_text(*op),
                inner!(*right)
            )
        }
        ExprKind::Conjunction { args } => {
            joined_expressions(context, fragment, args, " AND ", depth, work)?
        }
        ExprKind::Disjunction { args } => {
            let text = joined_expressions(context, fragment, args, " OR ", depth, work)?;
            if depth > 0 { format!("({text})") } else { text }
        }
        ExprKind::IsNull { expr, negated } => format!(
            "{} IS {}NULL",
            inner!(*expr),
            if *negated { "NOT " } else { "" }
        ),
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => format!(
            "{} IS {}{}",
            inner!(*expr),
            if *negated { "NOT " } else { "" },
            if *value { "TRUE" } else { "FALSE" }
        ),
        ExprKind::Cast { expr, target, .. } => format!("CAST({} AS {target})", inner!(*expr)),
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let prefix = inner!(*expr);
            let list = joined_expressions(context, fragment, list, ", ", depth, work)?;
            format!("{prefix} {}IN ({list})", if *negated { "NOT " } else { "" })
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "{} {}BETWEEN {} AND {}",
            inner!(*expr),
            if *negated { "NOT " } else { "" },
            inner!(*low),
            inner!(*high)
        ),
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => format!(
            "{} {}LIKE {}",
            inner!(*expr),
            if *negated { "NOT " } else { "" },
            inner!(*pattern)
        ),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut out = String::from("CASE");
            if let Some(operand) = operand {
                super::append_text(&mut out, " ", work)?;
                super::append_text(&mut out, &inner!(*operand), work)?;
            }
            for (when, then) in when_then.iter() {
                super::append_text(&mut out, " WHEN ", work)?;
                super::append_text(&mut out, &inner!(*when), work)?;
                super::append_text(&mut out, " THEN ", work)?;
                super::append_text(&mut out, &inner!(*then), work)?;
                work.step()?;
            }
            if let Some(otherwise) = else_expr {
                super::append_text(&mut out, " ELSE ", work)?;
                super::append_text(&mut out, &inner!(*otherwise), work)?;
            }
            super::append_text(&mut out, " END", work)?;
            out
        }
        ExprKind::FunctionCall { function, args } => {
            let text = joined_expressions(context, fragment, args, ", ", depth, work)?;
            format!("{}({text})", function_text(&function.function_id))
        }
        ExprKind::WindowCall {
            function,
            distinct,
            args,
            ..
        } => {
            let text = joined_expressions(context, fragment, args, ", ", depth, work)?;
            format!(
                "{}({}{text})",
                function_text(&function.function_id),
                if *distinct { "DISTINCT " } else { "" }
            )
        }
    };
    work.step()?;
    Ok(value)
}

fn joined_expressions(
    context: &TreeContext<'_>,
    fragment: &Fragment,
    items: &[ExprId],
    separator: &str,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let mut out = String::new();
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            super::append_text(&mut out, separator, work)?;
        }
        let text = expression_text(context, fragment, *item, depth.saturating_add(1), work)?;
        super::append_text(&mut out, &text, work)?;
        work.step()?;
    }
    Ok(out)
}

fn literal_text_observed(
    value: &LiteralValue,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let text = match value {
        LiteralValue::Utf8(value) => {
            let mut out = String::from("'");
            super::append_text(&mut out, value, work)?;
            out.push('\'');
            out
        }
        LiteralValue::Binary(value) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut out = String::from("X'");
            for byte in value.iter() {
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 15) as usize] as char);
                work.step()?;
            }
            out.push('\'');
            out
        }
        // Fixed-width source scalar formatting remains the original opaque
        // diagnostic implementation; it is not a conversion or type author.
        _ => {
            work.flush()?;
            let text = literal_text(value).to_string();
            work.step()?;
            work.flush()?;
            text
        }
    };
    Ok(text)
}

fn function_text(function: &novarocks_physical_plan::FunctionId) -> &str {
    // A function is registered as `builtin.scalar/coalesce/v1`; a statement
    // wrote the middle part and that is what names it here.
    let id = function.as_str();
    id.split('/').nth(1).unwrap_or(id)
}

fn binary_operator_text(op: novarocks_physical_plan::BinaryOperator) -> &'static str {
    use novarocks_physical_plan::BinaryOperator;
    match op {
        BinaryOperator::Eq => "=",
        BinaryOperator::EqForNull => "<=>",
        BinaryOperator::NotEq => "!=",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Add => "+",
        BinaryOperator::Subtract => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::BitAnd => "&",
        BinaryOperator::BitOr => "|",
        BinaryOperator::BitXor => "^",
    }
}

struct LiteralText<'a>(&'a LiteralValue);

fn literal_text(value: &LiteralValue) -> LiteralText<'_> {
    LiteralText(value)
}

impl std::fmt::Display for LiteralText<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            LiteralValue::Null => formatter.write_str("NULL"),
            LiteralValue::Boolean(value) => write!(formatter, "{value}"),
            LiteralValue::Int64(value) => write!(formatter, "{value}"),
            LiteralValue::UInt64(value) => write!(formatter, "{value}"),
            LiteralValue::Float64Bits(bits) => write!(formatter, "{}", f64::from_bits(*bits)),
            LiteralValue::LargeInt(value) | LiteralValue::Decimal128(value) => {
                write!(formatter, "{value}")
            }
            LiteralValue::Decimal256(bytes) => {
                write!(
                    formatter,
                    "{}",
                    arrow::datatypes::i256::from_be_bytes(*bytes)
                )
            }
            LiteralValue::Utf8(value) => write!(formatter, "'{value}'"),
            LiteralValue::Binary(value) => {
                formatter.write_str("X'")?;
                for byte in value.iter() {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str("'")
            }
            LiteralValue::Date32(value) => write!(formatter, "{value}"),
            LiteralValue::Time64(value) | LiteralValue::Timestamp(value) => {
                write!(formatter, "{value}")
            }
            LiteralValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } => {
                write!(
                    formatter,
                    "INTERVAL(months={months}, days={days}, nanoseconds={nanoseconds})"
                )
            }
        }
    }
}

/// One sort key, with the direction and null placement it establishes.
fn sort_item_text(
    context: &TreeContext<'_>,
    fragment: &Fragment,
    item: &SortExpr,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let text = expression_text(context, fragment, item.expr, 0, work)?;
    let value = format!(
        "{} {} NULLS {}",
        text,
        match item.direction {
            novarocks_physical_plan::SortDirection::Ascending => "ASC",
            novarocks_physical_plan::SortDirection::Descending => "DESC",
        },
        match item.null_ordering {
            novarocks_physical_plan::NullOrdering::First => "FIRST",
            novarocks_physical_plan::NullOrdering::Last => "LAST",
        }
    );
    work.step()?;
    Ok(value)
}
fn sort_items_text(
    context: &TreeContext<'_>,
    fragment: &Fragment,
    items: &[SortExpr],
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, SqlCompileError> {
    let items = items
        .iter()
        .map(|item| sort_item_text(context, fragment, item, work))
        .collect::<Result<Vec<_>, SqlCompileError>>()?;
    super::join_text(&items, ", ", work)
}

impl TreeContext<'_> {
    fn expr(
        &self,
        fragment: &Fragment,
        expr: ExprId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<String, SqlCompileError> {
        expression_text(self, fragment, expr, 0, work)
    }

    #[allow(clippy::too_many_lines)]
    fn render_node_lines(
        &self,
        fragment: &Fragment,
        node: &PhysicalNode,
        pad: &str,
        out: &mut Vec<String>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        use novarocks_physical_plan::NodeKind;

        let prefix = format!("{pad}{}:", self.display_id(fragment.id(), node.id));
        let stats = self.stats_suffix(fragment.id(), node.id, work)?;
        match &node.kind {
            NodeKind::Scan {
                relation: frozen,
                residuals,
                derived_values,
                ..
            } => {
                let relation = self
                    .node_annotation(fragment.id(), node.id, "sql.relation", work)?
                    .unwrap_or("relation");
                out.push(format!("{prefix}SCAN {relation}{stats}"));
                out.push(format!("{pad}     TABLE: {}", relation_table(relation)));
                if let Some(mv) =
                    self.node_annotation(fragment.id(), node.id, "sql.mv_rewritten_from", work)?
                {
                    out.push(format!("{pad}     rewritten with mv: {mv}"));
                }
                if let Some(provenance) =
                    self.node_annotation(fragment.id(), node.id, "sql.mv_rewrite_provenance", work)?
                {
                    out.push(format!("{pad}     mv rewrite provenance: {provenance}"));
                }
                if self.detailed() {
                    // The relation's own columns, by the relation's own names:
                    // which of them this scan reads is the question here, and
                    // the name the statement reaches them by is not part of it.
                    let columns = node
                        .output
                        .columns
                        .iter()
                        .map(|value| {
                            let name = self.value_name(fragment.id(), *value, work)?;
                            let text = name
                                .rsplit_once('.')
                                .map_or(name.as_str(), |(_, column)| column);
                            super::copy_text(text, work)
                        })
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    if !columns.is_empty() {
                        out.push(format!(
                            "{pad}     columns: {}",
                            super::join_text(&columns, ", ", work)?
                        ));
                    }
                }
                // A column the scan derives while it reads, and the call it
                // derives it with: the reader applies that call to the bytes
                // it is already reading rather than to a column handed on.
                if self.detailed() && !derived_values.is_empty() {
                    let derived = derived_values
                        .iter()
                        .map(|value| {
                            let name = self.value_name(fragment.id(), *value, work)?;
                            let text = match fragment.values().get(value).map(|def| &def.origin) {
                                Some(novarocks_physical_plan::ValueOrigin::Expr {
                                    expr, ..
                                }) => {
                                    format!("{name} := {}", self.expr(fragment, *expr, work)?)
                                }
                                _ => name,
                            };
                            work.step()?;
                            Ok(text)
                        })
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    out.push(format!(
                        "{pad}     variant columns: {}",
                        super::join_text(&derived, ", ", work)?
                    ));
                }
                // Whether this scan's shape admits min/max pruning at all:
                // it reads data rather than metadata, and every column it
                // projects is one a reader can state bounds for. It says
                // nothing about whether bounds have been collected.
                if self.verbose() && scan_admits_min_max_stats(fragment, frozen, node, work)? {
                    out.push(format!("{pad}     min-max stats"));
                }
                if !residuals.is_empty() {
                    let predicates = residuals
                        .iter()
                        .map(|expression| self.expr(fragment, *expression, work))
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    out.push(format!(
                        "{pad}     predicates: {}",
                        super::join_text(&predicates, " AND ", work)?
                    ));
                }
            }
            NodeKind::Filter { predicates } => {
                out.push(format!("{prefix}FILTER{stats}"));
                let text = predicates
                    .iter()
                    .map(|expression| self.expr(fragment, *expression, work))
                    .collect::<Result<Vec<_>, SqlCompileError>>()?;
                out.push(format!(
                    "{pad}  predicate: {}",
                    super::join_text(&text, " AND ", work)?
                ));
            }
            NodeKind::Project { expressions } => {
                let items = expressions
                    .iter()
                    .map(|(expression, value)| {
                        let text = self.expr(fragment, *expression, work)?;
                        let name = self.value_name(fragment.id(), *value, work)?;
                        let value = if text == name {
                            name
                        } else {
                            format!("{text} AS {name}")
                        };
                        work.step()?;
                        Ok(value)
                    })
                    .collect::<Result<Vec<_>, SqlCompileError>>()?;
                out.push(format!(
                    "{prefix}PROJECT [{}]{stats}",
                    super::join_text(&items, ", ", work)?
                ));
            }
            NodeKind::Aggregate {
                group_by,
                calls,
                grouping,
            } => {
                let mut header = format!(
                    "{prefix}HASH AGGREGATE ({}",
                    aggregate_mode(calls, *grouping, work)?
                );
                if !group_by.is_empty() {
                    let keys = group_by
                        .iter()
                        .map(|(expression, _)| self.expr(fragment, *expression, work))
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    let _ = write!(
                        header,
                        ", group by: [{}]",
                        super::join_text(&keys, ", ", work)?
                    );
                }
                let _ = write!(header, "){stats}");
                out.push(header);
                if !calls.is_empty() {
                    let aggregates = calls
                        .iter()
                        .map(|call| -> Result<String, SqlCompileError> {
                            // A phase that merges reads the state the phase
                            // below it produced, and that state is already
                            // named after the call it belongs to. Printing the
                            // call around it would say the call twice.
                            if !call.binding.phase.consumes_logical_arguments() {
                                return self.value_name(fragment.id(), call.output, work);
                            }
                            let args = call
                                .arguments
                                .iter()
                                .map(|argument| self.expr(fragment, *argument, work))
                                .collect::<Result<Vec<_>, SqlCompileError>>()?;
                            let text = format!(
                                "{}({}{})",
                                function_text(&call.binding.function.function_id),
                                if call.distinct { "DISTINCT " } else { "" },
                                super::join_text(&args, ", ", work)?
                            );
                            work.step()?;
                            Ok(text)
                        })
                        .collect::<Result<Vec<_>, SqlCompileError>>()?;
                    out.push(format!(
                        "{pad}  aggregations: {}",
                        super::join_text(&aggregates, ", ", work)?
                    ));
                }
            }
            NodeKind::HashJoin {
                kind,
                keys,
                distribution,
                residual,
                ..
            } => {
                let equalities = keys
                    .iter()
                    .map(|key| {
                        let text = format!(
                            "{} {} {}",
                            self.expr(fragment, key.left, work)?,
                            if key.null_safe { "<=>" } else { "=" },
                            self.expr(fragment, key.right, work)?
                        );
                        work.step()?;
                        Ok(text)
                    })
                    .collect::<Result<Vec<_>, SqlCompileError>>()?;
                out.push(format!(
                    "{prefix}HASH JOIN ({}, {}, eq: [{}]){}{stats}",
                    join_distribution_text(*distribution),
                    join_kind_text(*kind),
                    super::join_text(&equalities, ", ", work)?,
                    self.broadcast_suffix(fragment.id(), node.id, work)?
                ));
                if let Some(residual) = residual {
                    out.push(format!(
                        "{pad}  other: {}",
                        self.expr(fragment, *residual, work)?
                    ));
                }
            }
            NodeKind::NestLoopJoin {
                kind, predicate, ..
            } => {
                out.push(format!(
                    "{prefix}NEST LOOP JOIN ({}){stats}",
                    join_kind_text(*kind)
                ));
                if let Some(predicate) = predicate {
                    out.push(format!(
                        "{pad}  on: {}",
                        self.expr(fragment, *predicate, work)?
                    ));
                }
            }
            NodeKind::Sort { order_by, mode } => {
                let mut keys = match mode {
                    novarocks_physical_plan::SortMode::Global => Vec::new(),
                    novarocks_physical_plan::SortMode::Analytic { partition_by }
                    | novarocks_physical_plan::SortMode::PartitionTopN { partition_by, .. } => {
                        let mut keys = Vec::new();
                        for key in partition_by.iter() {
                            keys.push(key.clone());
                            work.step()?;
                        }
                        keys
                    }
                };
                for key in order_by.iter() {
                    keys.push(key.clone());
                    work.step()?;
                }
                let mut suffix = String::new();
                if let novarocks_physical_plan::SortMode::PartitionTopN { limit, kind, .. } = mode {
                    let _ = write!(
                        suffix,
                        " partition_limit={limit} topn_type={}",
                        partition_topn_text(*kind)
                    );
                }
                out.push(format!(
                    "{prefix}SORT BY [{}]{suffix}{stats}",
                    sort_items_text(self, fragment, &keys, work)?
                ));
            }
            NodeKind::TopN {
                order_by,
                limit,
                offset,
                phase,
                reduction,
            } => {
                let label = match phase {
                    novarocks_physical_plan::TopNPhase::Partial { .. } => "LOCAL TOP-N",
                    _ => "TOP-N",
                };
                // Both bounds, always: a top-N that skips nothing says so
                // rather than leaving a reader to infer it.
                let mut parts = vec![format!("limit={limit}"), format!("offset={offset}")];
                if let novarocks_physical_plan::TopNReduction::GroupedStates {
                    group_by,
                    calls,
                    comparator,
                } = reduction
                {
                    parts.push(format!(
                        "unit=group-key, keys={}, merged-states={}, comparator={}",
                        group_by.len(),
                        calls.len(),
                        comparator.stable_name()
                    ));
                }
                out.push(format!(
                    "{prefix}{label} ({}) [{}]{stats}",
                    super::join_text(&parts, ", ", work)?,
                    sort_items_text(self, fragment, order_by, work)?
                ));
            }
            NodeKind::Limit { limit, offset } => {
                let mut parts = Vec::new();
                if let Some(limit) = limit {
                    parts.push(format!("limit={limit}"));
                }
                if *offset > 0 {
                    parts.push(format!("offset={offset}"));
                }
                out.push(format!(
                    "{prefix}LIMIT ({}){stats}",
                    super::join_text(&parts, ", ", work)?
                ));
            }
            NodeKind::Window(spec) => {
                let functions = spec
                    .expressions
                    .iter()
                    .map(|expression| self.expr(fragment, expression.expression, work))
                    .collect::<Result<Vec<_>, SqlCompileError>>()?;
                out.push(format!(
                    "{prefix}WINDOW [{}]{stats}",
                    super::join_text(&functions, "; ", work)?
                ));
                if self.detailed() && !spec.partition_by.is_empty() {
                    out.push(format!(
                        "{pad}  partition by: [{}]",
                        sort_items_text(self, fragment, &spec.partition_by, work)?
                    ));
                }
                if self.detailed() && !spec.order_by.is_empty() {
                    out.push(format!(
                        "{pad}  order by: [{}]",
                        sort_items_text(self, fragment, &spec.order_by, work)?
                    ));
                }
            }
            NodeKind::SetOp { kind, .. } => {
                out.push(format!(
                    "{prefix}{}{stats}",
                    match kind {
                        novarocks_physical_plan::SetOperationKind::UnionAll => "UNION ALL",
                        novarocks_physical_plan::SetOperationKind::Intersect => "INTERSECT",
                        novarocks_physical_plan::SetOperationKind::Except => "EXCEPT",
                    }
                ));
            }
            NodeKind::Values { rows } => {
                out.push(format!("{prefix}VALUES ({} rows){stats}", rows.len()));
            }
            NodeKind::Repeat { grouping_sets, .. } => {
                out.push(format!(
                    "{prefix}REPEAT ({} grouping sets){stats}",
                    grouping_sets.len()
                ));
            }
            NodeKind::Unpivot { spec } => {
                out.push(format!(
                    "{prefix}UNPIVOT (mappings={}){stats}",
                    spec.mappings.len()
                ));
            }
            NodeKind::GenerateSeries { start, stop, step } => {
                let step = match step {
                    Some(expression) => self.expr(fragment, *expression, work)?,
                    None => "1".to_string(),
                };
                out.push(format!(
                    "{prefix}GENERATE_SERIES({}, {}, {step}){stats}",
                    self.expr(fragment, *start, work)?,
                    self.expr(fragment, *stop, work)?
                ));
            }
            NodeKind::TableFunction {
                function,
                left_outer,
                ..
            } => {
                out.push(format!(
                    "{prefix}TABLE_FUNCTION [{} {}]{stats}",
                    if *left_outer { "LEFT" } else { "CROSS" },
                    super::uppercase_text(function_text(&function.function_id), work)?
                ));
            }
            NodeKind::AssertOneRow(_) => {
                out.push(format!("{prefix}ASSERT ONE ROW{stats}"));
            }
            NodeKind::ChangeEventExpand { events, .. } => {
                out.push(format!(
                    "{prefix}CHANGE_EVENT_EXPAND(events={}){stats}",
                    events.len()
                ));
            }
            NodeKind::ExchangeSource { edge, .. } => {
                // A gather that keeps an ordering is merging its senders
                // rather than concatenating them, and that is the difference
                // a reader is looking for.
                let ordered = !node.output_properties.ordering.is_empty();
                let label = self.plan.edges().get(edge).map_or("EXCHANGE", |edge| {
                    match edge.partitioning.destination {
                        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
                            "HASH EXCHANGE"
                        }
                        Distribution::Broadcast => "BROADCAST EXCHANGE",
                        Distribution::RoundRobin => "RANDOM EXCHANGE",
                        Distribution::Singleton | Distribution::Unconstrained if ordered => {
                            "MERGING-EXCHANGE"
                        }
                        Distribution::Singleton | Distribution::Unconstrained => "GATHER",
                    }
                });
                out.push(format!("{prefix}{label}{stats}"));
            }
            NodeKind::TableWriter { .. } => {
                out.push(format!("{prefix}TABLE WRITER{stats}"));
            }
            NodeKind::TableFinish(_) => {
                out.push(format!("{prefix}TABLE FINISH{stats}"));
            }
        }

        work.step()?;
        Ok(())
    }
}

/// The name the wire and the reader both know this aggregate phase by.
fn aggregate_mode(
    calls: &[novarocks_physical_plan::AggregateCall],
    grouping: novarocks_physical_plan::AggregateGrouping,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'static str, SqlCompileError> {
    use novarocks_physical_plan::AggregateGrouping;
    let mut finalizes = false;
    for call in calls {
        finalizes = call.binding.phase.produces_final_result();
        work.step()?;
        if finalizes {
            break;
        }
    }
    let mut merges = false;
    for call in calls {
        merges = call.binding.phase.sequence().is_some();
        work.step()?;
        if merges {
            break;
        }
    }
    Ok(match (grouping, finalizes) {
        (_, true) if merges => "GLOBAL",
        (_, true) => "SINGLE",
        (AggregateGrouping::Partial, false) => "LOCAL",
        (AggregateGrouping::Complete, false) => "DISTINCT_GLOBAL",
    })
}

/// When a consumer starts reading through a filter.
fn activation_text(
    activation: &novarocks_physical_plan::RuntimeFilterConsumerActivation,
) -> String {
    use novarocks_physical_plan::{LateApplyGranularity, RuntimeFilterConsumerActivation};

    let granularity = |late_apply: LateApplyGranularity| match late_apply {
        LateApplyGranularity::Row => "Row",
        LateApplyGranularity::Batch => "Batch",
        LateApplyGranularity::RowGroup => "RowGroup",
        LateApplyGranularity::Split => "Split",
        LateApplyGranularity::File => "File",
    };
    match activation {
        RuntimeFilterConsumerActivation::BlockingSnapshot => "BlockingSnapshot".to_string(),
        RuntimeFilterConsumerActivation::NonBlockingLive { late_apply }
        | RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply } => {
            format!("NonBlockingLive({})", granularity(*late_apply))
        }
    }
}

fn join_kind_text(kind: novarocks_physical_plan::JoinKind) -> &'static str {
    use novarocks_physical_plan::JoinKind;
    match kind {
        JoinKind::Inner => "INNER",
        JoinKind::LeftOuter => "LEFT OUTER",
        JoinKind::RightOuter => "RIGHT OUTER",
        JoinKind::FullOuter => "FULL OUTER",
        JoinKind::LeftSemi => "LEFT SEMI",
        JoinKind::RightSemi => "RIGHT SEMI",
        JoinKind::LeftAnti => "LEFT ANTI",
        JoinKind::RightAnti => "RIGHT ANTI",
        JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI",
        JoinKind::Cross => "CROSS",
    }
}

fn join_distribution_text(distribution: novarocks_physical_plan::JoinDistribution) -> &'static str {
    use novarocks_physical_plan::JoinDistribution;
    match distribution {
        JoinDistribution::BroadcastBuild => "BROADCAST",
        JoinDistribution::Partitioned => "PARTITIONED",
        JoinDistribution::Colocated => "COLOCATE",
        JoinDistribution::Singleton => "SINGLETON",
    }
}

fn partition_topn_text(kind: novarocks_physical_plan::PartitionTopNType) -> &'static str {
    use novarocks_physical_plan::PartitionTopNType;
    match kind {
        PartitionTopNType::RowNumber => "ROW_NUMBER",
        PartitionTopNType::Rank => "RANK",
        PartitionTopNType::DenseRank => "DENSE_RANK",
    }
}

#[cfg(test)]
mod interval_literal_tests {
    #[test]
    fn explicit_interval_literal_display_preserves_all_three_components() {
        let literal = novarocks_physical_plan::LiteralValue::IntervalMonthDayNano {
            months: -7,
            days: 23,
            nanoseconds: i64::MIN,
        };
        assert_eq!(
            super::literal_text(&literal).to_string(),
            "INTERVAL(months=-7, days=23, nanoseconds=-9223372036854775808)"
        );
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use novarocks_constant_contract::ConstantPool;
    use novarocks_physical_plan::{
        ConstantPoolId, ConstantReference, FragmentBuilder, OutputPort, PipelineDopDomain,
        PlanBuilder, PlanVersionId, ResultField, ResultPort, ValueOrigin,
    };
    use novarocks_type_contract::{CompileControlError, CompilePhase, FunctionValueType};
    use std::sync::{Arc, Mutex};

    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl Control {
        fn new(stop: Option<(usize, CompileControlError)>) -> Self {
            Self {
                trace: Mutex::new(Vec::new()),
                stop,
            }
        }
        fn trace(&self) -> Vec<(CompilePhase, u32)> {
            self.trace.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            if let Some((at, _)) = self.stop {
                assert!(
                    trace.len() < at,
                    "the originating refusal must not be followed by a callback"
                );
            }
            trace.push((phase, units));
            match self.stop {
                Some((at, cause)) if trace.len() == at => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn selected_constant_plan(annotation_count: usize) -> PhysicalPlan {
        let control = Control::new(None);
        let ty = FunctionValueType::new(DataType::Int64, false);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("literal").unwrap()),
            ty.clone(),
            Int64Array::from(vec![999, 17, -999]).to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let fragment_id = FragmentId::new(71);
        let mut fragment = FragmentBuilder::new(fragment_id);
        let source = fragment.reserve_node_id().unwrap();
        fragment
            .add_values(
                source,
                vec![Box::<[ExprId]>::default()].into_boxed_slice(),
                Box::default(),
            )
            .unwrap();
        let root = fragment.reserve_node_id().unwrap();
        let expression = fragment
            .add_expression(
                root,
                ty.clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(9107),
                    ordinal: 1,
                }),
            )
            .unwrap();
        let value = fragment
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node: root,
                    expr: expression,
                },
            )
            .unwrap();
        fragment
            .add_project(
                root,
                source,
                Box::from([(expression, value)]),
                Box::from([value]),
            )
            .unwrap();
        let fragment = fragment
            .finish_definition(
                root,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut plan = PlanBuilder::new(PlanVersionId::try_new([71; 16]).unwrap());
        plan.insert_constant_pool(ConstantPoolId::new(9107), pool)
            .unwrap();
        plan.add_fragment(fragment).unwrap();
        plan.set_result_port(ResultPort {
            fragment: fragment_id,
            output: OutputPort {
                node: root,
                columns: Box::from([value]),
            },
            fields: Box::from([ResultField {
                name: "answer".into(),
                alias: None,
                value,
                ty,
            }]),
        })
        .unwrap();
        plan.add_annotation(PlanAnnotation {
            subject: AnnotationSubject::Value(fragment_id, value),
            key: "sql.display_name".into(),
            value: "answer".into(),
        });
        for ordinal in 0..annotation_count {
            plan.add_annotation(PlanAnnotation {
                subject: AnnotationSubject::Plan,
                key: format!("diagnostic.{ordinal}").into(),
                value: "checked annotation".into(),
            });
        }
        plan.finish_observed(&control).unwrap()
    }

    #[test]
    fn completed_tree_resolves_actual_nonzero_constant_ordinal_without_address_text() {
        let plan = selected_constant_plan(0);
        assert_eq!(
            render_completed_plan_tree(&plan, ExplainLevel::Normal, &Control::new(None)).unwrap(),
            ["0:PROJECT [17 AS answer]", "  1:VALUES (1 rows)"]
        );
    }

    #[test]
    fn completed_tree_original_control_refusals_preserve_every_callback_prefix() {
        let plan = selected_constant_plan(320);
        let baseline = Control::new(None);
        render_completed_plan_tree(&plan, ExplainLevel::Verbose, &baseline).unwrap();
        let trace = baseline.trace();
        assert_eq!(trace.first(), Some(&(CompilePhase::LowerProgram, 0)));
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.iter().all(|(_, units)| *units <= 256));
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=trace.len() {
                let refused = Control::new(Some((at, cause)));
                assert_eq!(
                    render_completed_plan_tree(&plan, ExplainLevel::Verbose, &refused),
                    Err(cause.into())
                );
                assert_eq!(refused.trace(), trace[..at]);
            }
        }
    }
}
