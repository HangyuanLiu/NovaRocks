# NovaRocks patches on top of crates.io iceberg-catalog-rest 0.9.0

- Implemented the Iceberg REST view endpoints on `RestCatalog`:
  create_view / load_view / update_view (commit) / drop_view /
  view_exists / list_views, plus `CreateViewRequest`, `LoadViewResult`,
  `CommitViewRequest` wire types. Upstream 0.9.0 has no view support.
