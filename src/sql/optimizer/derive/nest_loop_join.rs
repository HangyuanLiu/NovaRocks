//! NestLoopJoin: always Gather both inputs; output Gather.

use crate::sql::optimizer::operator::PhysicalNestLoopJoinOp;
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalNestLoopJoinOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::gather()
    }
}

impl DeriveRequired for PhysicalNestLoopJoinOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::gather(), PhysicalPropertySet::gather()]
    }
}
