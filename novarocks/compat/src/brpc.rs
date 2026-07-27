pub(crate) use novarocks::service::compat::CompatConfig;

pub(crate) fn start(
    config: &CompatConfig<'_>,
) -> Result<(), novarocks::service::compat::CompatError> {
    novarocks::service::compat::start(config)
}

pub(crate) fn stop() {
    novarocks::service::compat::stop();
}
