use crate::scenario::Scenario;

mod catalog_state;
mod connector;
mod mv_recovery;
mod query_lifecycle;

pub fn all() -> Vec<Box<dyn Scenario>> {
    let mut scenarios = Vec::new();
    scenarios.extend(query_lifecycle::scenarios());
    scenarios.extend(connector::scenarios());
    scenarios.extend(catalog_state::scenarios());
    scenarios.extend(mv_recovery::scenarios());
    scenarios
}
