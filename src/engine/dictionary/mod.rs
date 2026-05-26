pub(crate) mod maintenance;
pub(crate) mod model;
pub(crate) mod rebuild;

use std::sync::Arc;

use crate::engine::dictionary::model::{DictionaryOwner, DictionarySnapshot};
use crate::meta::repository::dictionary::DictionaryMetaRepository;

#[derive(Clone, Default)]
pub(crate) struct DictionaryManager {
    repo: Arc<DictionaryMetaRepository>,
}

impl DictionaryManager {
    pub(crate) fn repo(&self) -> &DictionaryMetaRepository {
        &self.repo
    }

    pub(crate) fn load_active_snapshot(
        &self,
        _state: &crate::engine::StandaloneState,
        _owner: &DictionaryOwner,
        _column_name: &str,
    ) -> Result<Option<DictionarySnapshot>, String> {
        Ok(None)
    }
}
