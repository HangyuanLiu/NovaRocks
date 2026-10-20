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
//! Original ordered result declaration from the same unpublished SQL emission.
//! This receipt freezes completed guarantees from the same expression owner.
//! It does not derive kernel types or NULL policy from presentation metadata.
use novarocks_physical_plan::{PlanVersionId, ResultField, ResultPort};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
};

#[derive(Debug)]
pub struct SqlResultDeclaration {
    version: PlanVersionId,
    original: ResultPort,
}

#[derive(Debug)]
pub enum ResultDeclarationError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Association(&'static str),
}
impl std::fmt::Display for ResultDeclarationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Type(cause) => cause.fmt(f),
            Self::Association(detail) => f.write_str(detail),
        }
    }
}
impl std::error::Error for ResultDeclarationError {}
impl From<CompileControlError> for ResultDeclarationError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for ResultDeclarationError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}

/// Constructed only by the same source owner that retains this declaration
/// and the computed port. No caller can supply an unverified public schema.
pub struct CheckedSqlResultDeclaration<'a> {
    original: &'a ResultPort,
}
impl<'a> CheckedSqlResultDeclaration<'a> {
    pub const fn original_port(&self) -> &'a ResultPort {
        self.original
    }

    pub fn fields(&self) -> &[ResultField] {
        &self.original.fields
    }
}

/// Only the same completed owner can construct this publication proof.
/// The original port stays owned here once; no second public schema is cached.
#[derive(Debug)]
pub(super) struct PublishedSqlResultDeclaration {
    declaration: SqlResultDeclaration,
}
impl PublishedSqlResultDeclaration {
    pub(super) fn checked_loan(&self) -> CheckedSqlResultDeclaration<'_> {
        CheckedSqlResultDeclaration {
            original: &self.declaration.original,
        }
    }
    pub(super) fn recheck_observed(
        &self,
        version: PlanVersionId,
        computed: &ResultPort,
        control: &dyn PureCompileControl,
    ) -> Result<CheckedSqlResultDeclaration<'_>, ResultDeclarationError> {
        self.declaration
            .check_computed_port(version, computed, control)
    }
}

impl SqlResultDeclaration {
    pub(in crate::planner::distributed::build) const fn original_port(&self) -> &ResultPort {
        &self.original
    }
    pub(super) fn publish(
        mut self,
        version: PlanVersionId,
        computed: &ResultPort,
        control: &dyn PureCompileControl,
    ) -> Result<PublishedSqlResultDeclaration, ResultDeclarationError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            // Before publication the same original owner may widen only a
            // root guarantee. All occurrence identity, child semantics and
            // layout facts must remain exact.
            self.check_inner(version, computed, true, &mut work)?;
            for (declared, actual) in self.original.fields.iter_mut().zip(&computed.fields) {
                work.step()?;
                declared.ty = actual.ty.clone_observed(|| work.step())?;
            }
            work.flush()?;
            self.original.scalar_schema = computed.scalar_schema.clone();
            work.step()?;
            Ok::<_, ResultDeclarationError>(())
        })();
        if matches!(&result, Err(ResultDeclarationError::Control(_))) {
            return result.map(|()| PublishedSqlResultDeclaration { declaration: self });
        }
        work.finish()?;
        result?;
        Ok(PublishedSqlResultDeclaration { declaration: self })
    }

    /// Move the actual root ResultPort just authored by the original lowering.
    /// This neither clones a plan nor reads TypedExpr nullability again.
    pub(in crate::planner::distributed::build) fn capture(
        version: PlanVersionId,
        original: ResultPort,
    ) -> Self {
        Self { version, original }
    }

    /// Called by the owning SqlAuthoredPhysicalPlan, never from an arbitrary
    /// same-version external plan. The computed port must be its actual port.
    pub(in crate::planner::distributed::build) fn check_computed_port<'a>(
        &'a self,
        version: PlanVersionId,
        computed: &ResultPort,
        control: &dyn PureCompileControl,
    ) -> Result<CheckedSqlResultDeclaration<'a>, ResultDeclarationError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.check_inner(version, computed, false, &mut work);
        if matches!(&result, Err(ResultDeclarationError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn check_inner<'a>(
        &'a self,
        version: PlanVersionId,
        computed: &ResultPort,
        allow_unpublished_root_widening: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedSqlResultDeclaration<'a>, ResultDeclarationError> {
        work.step()?;
        if version != self.version
            || computed.fragment != self.original.fragment
            || computed.output.node != self.original.output.node
            || computed.fields.len() != self.original.fields.len()
            || computed.output.columns.len() != self.original.output.columns.len()
            || computed.fields.len() != computed.output.columns.len()
            || self.original.fields.len() != self.original.output.columns.len()
        {
            return Err(ResultDeclarationError::Association(
                "source result declaration differs from its completed port identity",
            ));
        }
        for (ordinal, (declared, actual)) in self
            .original
            .fields
            .iter()
            .zip(computed.fields.iter())
            .enumerate()
        {
            work.step()?;
            if declared.value != actual.value
                || declared.domain != actual.domain
                || declared.value != self.original.output.columns[ordinal]
                || actual.value != computed.output.columns[ordinal]
                || !text_equal(&declared.name, &actual.name, work)?
                || !alias_equal(declared.alias.as_deref(), actual.alias.as_deref(), work)?
                // An immutable public guarantee is exact. Admission widening
                // exists only inside the consumed prepublication owner.
                || if allow_unpublished_root_widening {
                    declared.ty.nullable() && !actual.ty.nullable()
                } else {
                    declared.ty.nullable() != actual.ty.nullable()
                }
                || !declared.ty.same_physical_binding_ignoring_root_nullability_observed(
                    &actual.ty, || work.step().map_err(ResultDeclarationError::from),
                )?
            {
                return Err(ResultDeclarationError::Association(
                    "source result declaration differs from its completed guarantee",
                ));
            }
        }
        Ok(CheckedSqlResultDeclaration {
            original: &self.original,
        })
    }
}

fn text_equal(
    a: &str,
    b: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ResultDeclarationError> {
    work.step()?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (left, right) in a.bytes().zip(b.bytes()) {
        work.step()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
fn alias_equal(
    a: Option<&str>,
    b: Option<&str>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ResultDeclarationError> {
    work.step()?;
    match (a, b) {
        (Some(a), Some(b)) => text_equal(a, b, work),
        (None, None) => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{FragmentId, NodeId, OutputPort, ResultValueDomain, ValueId};
    use novarocks_type_contract::FunctionValueType;

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    fn port(ty: FunctionValueType) -> ResultPort {
        ResultPort {
            scalar_schema: None,
            fragment: FragmentId::new(0),
            output: OutputPort {
                node: NodeId::new(1),
                columns: Box::from([ValueId::new(2)]),
            },
            fields: Box::from([ResultField {
                domain: ResultValueDomain::Plain,
                name: "value".into(),
                alias: Some("alias".into()),
                value: ValueId::new(2),
                ty,
            }]),
        }
    }
    #[test]
    fn published_guarantee_is_exact_after_original_owner_admission() {
        let version = PlanVersionId::try_new([5; 16]).unwrap();
        let required = port(FunctionValueType::new(DataType::Int8, false));
        let nullable = port(required.fields[0].ty.with_nullable(true).unwrap());
        let admitted = SqlResultDeclaration::capture(version, required.clone())
            .publish(version, &nullable, &Control)
            .unwrap();
        assert_eq!(admitted.checked_loan().original_port(), &nullable);
        admitted
            .recheck_observed(version, &nullable, &Control)
            .unwrap();
        assert!(matches!(
            admitted.recheck_observed(version, &required, &Control),
            Err(ResultDeclarationError::Association(
                "source result declaration differs from its completed guarantee"
            ))
        ));
        let unchanged = SqlResultDeclaration::capture(version, required.clone())
            .publish(version, &required, &Control)
            .unwrap();
        // A published required guarantee cannot silently widen later either.
        assert!(matches!(
            unchanged.recheck_observed(version, &nullable, &Control),
            Err(ResultDeclarationError::Association(
                "source result declaration differs from its completed guarantee"
            ))
        ));
        let different_carrier = port(FunctionValueType::new(DataType::Int16, true));
        assert!(matches!(
            SqlResultDeclaration::capture(version, required).publish(
                version,
                &different_carrier,
                &Control
            ),
            Err(ResultDeclarationError::Association(_))
        ));
    }
}
