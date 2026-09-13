//! Backend-owned runtime-filter participant state.

pub(crate) mod ingress;
pub(crate) mod participant;
#[cfg(test)]
pub(crate) mod test_support;
// The Native adapter projects an already-validated carrier onto decoded SPI
// columns. Worker owns the local predicate oracle and subscription semantics.
pub(crate) mod typed_dynamic_filter;
