# LNP-3C runtime-state hard cut

This explicit-only suite is the product acceptance for the maintenance and
statistics FE process-runtime boundary.  It requires the runner-owned native
`1FE+3BE` cross-process topology and the shared REST Catalog plus MinIO
fixture.

It publishes statistics through three independent Iceberg writes, restarts the
FE, proves the old process-local statistics job is absent while the
provider-owned artifact remains readable, then proves a maintenance manifest
rewrite leaves readable lake truth across another FE restart.  GC observation
accelerator safety is covered by its focused StateStore tests because the SQL
surface intentionally has no unsafe fixture that can age and delete arbitrary
objects.
