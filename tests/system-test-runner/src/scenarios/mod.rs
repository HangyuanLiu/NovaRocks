use crate::scenario::Scenario;

mod backend_membership;
mod catalog_state;
mod connector;
mod mv_recovery;
mod native_trust;
mod query_lifecycle;
mod runtime_filter;
mod table_maintenance;

pub fn all() -> Vec<Box<dyn Scenario>> {
    let mut scenarios = Vec::new();
    scenarios.extend(backend_membership::scenarios());
    scenarios.extend(query_lifecycle::scenarios());
    scenarios.extend(runtime_filter::scenarios());
    scenarios.extend(runtime_filter::native_trust_directional_scenarios());
    scenarios.extend(connector::scenarios());
    scenarios.extend(catalog_state::scenarios());
    scenarios.extend(mv_recovery::scenarios());
    scenarios.extend(native_trust::scenarios());
    scenarios.extend(table_maintenance::scenarios());
    scenarios
}
