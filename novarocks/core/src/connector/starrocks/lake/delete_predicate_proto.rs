// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

//! Encode FE-level `DeletePredicateTerms` into the wire `DeletePredicatePb`
//! that gets persisted into rowset metadata for DUP/UNIQUE/AGG StarRocks table
//! tables. `sub_predicates` is left empty: it is the legacy hybrid
//! key/value string format used by the StarRocks shared-nothing path, and
//! lake mode reads only the structured `binary_predicates` / `in_predicates`
//! / `is_null_predicates` fields.

use crate::engine::delete_predicate_translate::{
    BinaryTerm, CmpOp, DeletePredicateTerms, InTerm, IsNullTerm,
};
use crate::service::grpc_client::proto::starrocks::{
    BinaryPredicatePb, DeletePredicatePb, InPredicatePb, IsNullPredicatePb,
};

pub fn build_delete_predicate_pb(terms: &DeletePredicateTerms, version: i32) -> DeletePredicatePb {
    DeletePredicatePb {
        version,
        sub_predicates: Vec::new(),
        in_predicates: terms.in_list.iter().map(in_to_pb).collect(),
        binary_predicates: terms.binary.iter().map(binary_to_pb).collect(),
        is_null_predicates: terms.is_null.iter().map(isnull_to_pb).collect(),
    }
}

fn binary_to_pb(term: &BinaryTerm) -> BinaryPredicatePb {
    // StarRocks BE's delete-predicate reader (see `parse_delete_binary_op` in
    // src/formats/starrocks/plan.rs) accepts the symbolic forms only. The
    // textual `EQ`/`NE`/... names from the proto enum are intentionally NOT
    // recognized — they exist only for legacy hybrid sub_predicates strings.
    BinaryPredicatePb {
        column_name: Some(term.column.clone()),
        op: Some(
            match term.op {
                CmpOp::Eq => "=",
                CmpOp::Ne => "!=",
                CmpOp::Lt => "<",
                CmpOp::Le => "<=",
                CmpOp::Gt => ">",
                CmpOp::Ge => ">=",
            }
            .to_string(),
        ),
        value: Some(term.value.clone()),
    }
}

fn in_to_pb(term: &InTerm) -> InPredicatePb {
    InPredicatePb {
        column_name: Some(term.column.clone()),
        is_not_in: Some(term.is_not_in),
        values: term.values.clone(),
    }
}

fn isnull_to_pb(term: &IsNullTerm) -> IsNullPredicatePb {
    IsNullPredicatePb {
        column_name: Some(term.column.clone()),
        is_not_null: Some(term.is_not_null),
    }
}
