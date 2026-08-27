//! The coordinator-side control boundary to a typed connector.
//!
//! Planning asks the connector for a relation handle and its columns, then
//! offers filter, projection, and limit pushdown. Everything crossing this
//! boundary is either a protocol-validated carrier or a generic SPI value, so
//! the frontend never links a provider crate and never interprets a variant.

use std::sync::Arc;

use novarocks_spi::connector::read_stack::{
    ConnectorExpression, ConnectorSession, SchemaTableName, SystemTableDistribution,
};
use novarocks_spi::connector::{ConnectorError, ConnectorPinnedFileSet};

use super::execution::WireConstraint;
use super::handle::CatalogTableHandle;
use super::predicate::ValidatedColumnHandle;
use super::scan::ScanAssignment;
use super::split::ValidatedConnectorSplit;

/// One column a relation exposes, in the connector's own schema order.
#[derive(Clone, Debug)]
pub struct TypedColumnBinding {
    name: Arc<str>,
    column: ValidatedColumnHandle,
    hidden: bool,
}

impl TypedColumnBinding {
    pub fn new(name: impl AsRef<str>, column: ValidatedColumnHandle, hidden: bool) -> Self {
        Self {
            name: Arc::from(name.as_ref()),
            column,
            hidden,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn column(&self) -> &ValidatedColumnHandle {
        &self.column
    }

    /// A hidden column is addressable by name but never part of `SELECT *`.
    pub const fn is_hidden(&self) -> bool {
        self.hidden
    }
}

/// What a connector accepted from a filter pushdown offer.
#[derive(Clone, Debug)]
pub struct TypedFilterApplication {
    handle: CatalogTableHandle,
    remaining_constraint: WireConstraint,
    remaining_expression: Option<ConnectorExpression>,
}

impl TypedFilterApplication {
    pub const fn new(
        handle: CatalogTableHandle,
        remaining_constraint: WireConstraint,
        remaining_expression: Option<ConnectorExpression>,
    ) -> Self {
        Self {
            handle,
            remaining_constraint,
            remaining_expression,
        }
    }

    pub const fn handle(&self) -> &CatalogTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> CatalogTableHandle {
        self.handle
    }

    /// What the engine must still evaluate itself.
    pub const fn remaining_constraint(&self) -> &WireConstraint {
        &self.remaining_constraint
    }

    pub const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }
}

/// What a connector accepted from a limit pushdown offer.
#[derive(Clone, Debug)]
pub struct TypedLimitApplication {
    handle: CatalogTableHandle,
    limit_guaranteed: bool,
}

impl TypedLimitApplication {
    pub const fn new(handle: CatalogTableHandle, limit_guaranteed: bool) -> Self {
        Self {
            handle,
            limit_guaranteed,
        }
    }

    pub const fn handle(&self) -> &CatalogTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> CatalogTableHandle {
        self.handle
    }

    /// Whether the engine may drop its own limit operator. A connector that
    /// cannot guarantee the bound must say so, or rows would go missing.
    pub const fn limit_guaranteed(&self) -> bool {
        self.limit_guaranteed
    }
}

/// How a system relation must be executed.
#[derive(Clone, Debug)]
pub struct TypedSystemTablePlan {
    handle: CatalogTableHandle,
    distribution: SystemTableDistribution,
}

impl TypedSystemTablePlan {
    pub const fn new(handle: CatalogTableHandle, distribution: SystemTableDistribution) -> Self {
        Self {
            handle,
            distribution,
        }
    }

    pub const fn handle(&self) -> &CatalogTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> CatalogTableHandle {
        self.handle
    }

    /// `AllNodes` uses a typed split source; `SingleCoordinator` is executed by
    /// exactly one selected backend reading an immutable metadata file, with no
    /// synthetic split.
    pub const fn distribution(&self) -> SystemTableDistribution {
        self.distribution
    }
}

/// The two snapshots whose visible-row sets a change window differences.
///
/// The window is a set difference between two endpoints, not a replay of the
/// manifests between them: a row that was written and deleted inside the
/// window is invisible at both endpoints and must not appear.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypedChangeWindow {
    from_snapshot_id: i64,
    to_snapshot_id: i64,
}

impl TypedChangeWindow {
    pub const fn new(from_snapshot_id: i64, to_snapshot_id: i64) -> Self {
        Self {
            from_snapshot_id,
            to_snapshot_id,
        }
    }

    /// The exclusive start endpoint: rows visible here are the window's "before".
    pub const fn from_snapshot_id(&self) -> i64 {
        self.from_snapshot_id
    }

    /// The inclusive end endpoint: rows visible here are the window's "after".
    pub const fn to_snapshot_id(&self) -> i64 {
        self.to_snapshot_id
    }
}

/// The exact frozen rewrite group one distributed `ALTER TABLE ... EXECUTE`
/// procedure instance owns.
///
/// A distributed rewrite freezes its whole file selection into one immutable
/// external artifact before any worker runs, and cuts that selection into
/// groups. The procedure instance reading this group must read exactly the
/// files the group names -- the same set its commit replaces. Naming the group
/// rather than restating a selection rule is what makes those two sets one
/// fact: a rule re-evaluated later could select a different set, and a rewrite
/// that reads an artifact its commit does not replace corrupts the relation
/// rather than returning a wrong answer.
///
/// The artifact is named by location *and* content digest. Location alone
/// would let a replacement written at the same place be read as the same plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypedFrozenRewriteGroup<'a> {
    artifact_location: &'a str,
    artifact_digest_hex: &'a str,
    group_digest_hex: &'a str,
}

impl<'a> TypedFrozenRewriteGroup<'a> {
    pub const fn new(
        artifact_location: &'a str,
        artifact_digest_hex: &'a str,
        group_digest_hex: &'a str,
    ) -> Self {
        Self {
            artifact_location,
            artifact_digest_hex,
            group_digest_hex,
        }
    }

    /// Where the immutable artifact lives.
    pub const fn artifact_location(&self) -> &'a str {
        self.artifact_location
    }

    /// The artifact's content digest, as lowercase hex.
    pub const fn artifact_digest_hex(&self) -> &'a str {
        self.artifact_digest_hex
    }

    /// The group inside that artifact, as lowercase hex.
    pub const fn group_digest_hex(&self) -> &'a str {
        self.group_digest_hex
    }
}

/// The `ALTER TABLE ... EXECUTE` procedure a scan asks a connector to freeze.
///
/// Only a procedure that reads through a distributed scan appears here. A
/// procedure the coordinator performs over metadata alone never reaches the
/// read stack, and one that rewrites table rows reads them through
/// [`TypedConnectorMetadata::get_pinned_file_set_handle`] like any other
/// cohort -- its input is a set of data files, which the pinned lane already
/// names exactly. What is left is the one procedure whose input is not table
/// rows at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedTableExecuteProcedure<'a> {
    /// Re-encode the delete artifacts of one frozen group into fresh ones.
    ///
    /// The read produces the rows those artifacts remove, not the rows of the
    /// relation, so it pins no data file set and cannot travel the pinned
    /// lane.
    RewritePositionDeleteFiles(TypedFrozenRewriteGroup<'a>),
}

impl<'a> TypedTableExecuteProcedure<'a> {
    /// The frozen group this procedure instance reads.
    pub const fn group(&self) -> TypedFrozenRewriteGroup<'a> {
        match self {
            Self::RewritePositionDeleteFiles(group) => *group,
        }
    }
}

/// How a relation should be read at a point in time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedRelationVersion {
    /// The reference the catalog currently resolves to.
    Current,
    SnapshotId(i64),
    /// A branch or tag name resolved by the connector, never by the engine.
    Reference,
}

/// The coordinator-side control entry point a typed connector implements.
pub trait TypedConnectorMetadata: Send + Sync {
    /// Freeze one relation handle. The returned handle pins its snapshot: a
    /// worker must never re-resolve it or fall back to a later snapshot.
    fn get_table_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        version: TypedRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<CatalogTableHandle>, ConnectorError>;

    /// Freeze one relation restricted to exactly the files a provider-frozen
    /// cohort reads.
    ///
    /// The set is the whole definition of the read: the cohort's commit
    /// replaces precisely those files, so a connector that cannot honor the
    /// set exactly must fail rather than widen it to the snapshot or narrow it
    /// by any rule of its own. `None` means this connector does not read
    /// relations by pinned file set at all, which is different from a set it
    /// cannot serve: that is an error.
    fn get_pinned_file_set_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<CatalogTableHandle>, ConnectorError>;

    /// The relation's columns in connector schema order.
    fn get_column_bindings(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
    ) -> Result<Vec<TypedColumnBinding>, ConnectorError>;

    /// Offer a filter. `None` means the connector accepted nothing, so the
    /// engine keeps the whole predicate.
    fn apply_filter(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        constraint: &WireConstraint,
    ) -> Result<Option<TypedFilterApplication>, ConnectorError>;

    /// Offer a projection. Ordered output remains the scan node's authority:
    /// what the connector records is a set-shaped pushdown fact.
    fn apply_projection(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        assignments: &[ScanAssignment],
    ) -> Result<Option<CatalogTableHandle>, ConnectorError>;

    /// Offer a limit.
    fn apply_limit(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        limit: u64,
    ) -> Result<Option<TypedLimitApplication>, ConnectorError>;

    /// Resolve a system relation to a pinned, immutable metadata reference.
    fn get_system_table_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<TypedSystemTablePlan>, ConnectorError>;

    /// Freeze one change window over a relation.
    ///
    /// Both endpoints are pinned by the returned handle, exactly as
    /// [`Self::get_table_handle`] pins one snapshot. `None` means this
    /// connector does not expose change windows over that relation at all,
    /// which is different from a window it cannot serve: that is an error.
    fn get_change_window_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        window: TypedChangeWindow,
    ) -> Result<Option<CatalogTableHandle>, ConnectorError>;

    /// Freeze the relation one distributed `ALTER TABLE ... EXECUTE` procedure
    /// instance reads.
    ///
    /// The returned handle pins the snapshot the procedure's frozen group was
    /// selected against, exactly as [`Self::get_table_handle`] pins one, and it
    /// names the group rather than any rule that could reselect it. `None`
    /// means this connector exposes no table-execute relation over that
    /// relation at all, which is different from a group it cannot serve: that
    /// is an error.
    fn get_table_execute_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        procedure: TypedTableExecuteProcedure<'_>,
    ) -> Result<Option<CatalogTableHandle>, ConnectorError>;
}

/// The engine-visible outcome of one split-enumeration batch.
pub type TypedSplitBatch =
    novarocks_spi::connector::read_stack::ConnectorSplitBatch<ValidatedConnectorSplit>;
