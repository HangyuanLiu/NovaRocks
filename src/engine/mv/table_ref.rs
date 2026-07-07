#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct IcebergTableRef {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl IcebergTableRef {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}
